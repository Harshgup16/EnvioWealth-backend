"""
FastAPI Backend for BRSR Report Generation with Chunked Gemini API Calls.

This module provides a focused, working implementation that:
- sources prompts from `agents.py`
- extracts text from PDF/XLSX via `pdfplumber`/`openpyxl` (if installed)
- sends chunked prompts to Gemini (`google.generativeai`) when configured
- merges chunk responses into final BRSR JSON using `BRSR_DATA_SKELETON` from `data.py`.
- transforms Gemini's flat output keys to nested frontend structure using `transform.py`

Run: uvicorn fastapi_brsr_backend:app --reload --port 8000
"""

import io
import os
import re
import json
import time
import asyncio
from typing import Dict, Any, List

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from dotenv import load_dotenv

# Load environment variables once
load_dotenv()

# Import transformation utilities
from transform import transform_flat_to_nested, merge_nested_data

# Import agents (centralized prompts)
from agents import (
    create_sectionA_agent,
    create_sectionB_agent,
    create_principles_1_2_agent,
    create_principles_3_4_agent,
    create_principles_5_6_agent,
    create_principles_7_8_9_agent,
)

# Import canonical skeleton
from data import BRSR_DATA_SKELETON

# Optional libs
try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    import openpyxl
except Exception:
    openpyxl = None

try:
    import google.generativeai as genai
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
except Exception:
    genai = None
    HarmCategory = None
    HarmBlockThreshold = None

try:
    import openpyxl
except Exception:
    openpyxl = None

# Optional Gemini client
try:
    import google.generativeai as genai
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    if GOOGLE_API_KEY:
        genai.configure(api_key=GOOGLE_API_KEY)
except Exception:
    genai = None
    GOOGLE_API_KEY = None

app = FastAPI(title="BRSR Report Generator API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

GEMINI_CONFIG = {
    "model": os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
    "max_retries": 3,
    "retry_delay_base": 2,
    "delay_between_chunks": 15,  # Increased to 15s for free tier (5 req/min = 12s minimum)
    "max_input_tokens": 30000,
    "max_output_tokens": 32768,  # Doubled to 32K for Section C P3-4 (200+ fields)
    "requests_per_minute": 5,  # Free tier limit is 5/min, not 10
}

_rate_tracker: Dict[str, Any] = {"requests": [], "last_reset": time.time()}


async def wait_for_rate_limit():
    now = time.time()
    if now - _rate_tracker["last_reset"] > 60:
        _rate_tracker["requests"] = []
        _rate_tracker["last_reset"] = now
    if len(_rate_tracker["requests"]) >= GEMINI_CONFIG["requests_per_minute"]:
        await asyncio.sleep(60 - (now - _rate_tracker["last_reset"]))
        _rate_tracker["requests"] = []
        _rate_tracker["last_reset"] = time.time()
    _rate_tracker["requests"].append(now)


def repair_json(text: str) -> str:
    """Clean and extract JSON from response text.
    
    Handles:
    - Markdown code blocks (```json ... ``` or ```...```)
    - Leading/trailing whitespace
    - Text before/after JSON
    - Unterminated strings
    - Missing closing brackets
    """
    text = text.strip()
    
    # Remove markdown code blocks more aggressively
    if "```" in text:
        # Remove all ``` markers
        text = text.replace("```json", "").replace("```", "").strip()
    
    # Remove any remaining language markers at the start
    lines = text.split('\n')
    if lines and lines[0].strip().lower() in ['json', 'js', 'javascript']:
        text = '\n'.join(lines[1:]).strip()
    
    # Try to find JSON array first (for key-value format)
    array_start = text.find("[")
    obj_start = text.find("{")
    
    # Determine if we have an array or object based on which comes first
    if array_start != -1 and (obj_start == -1 or array_start < obj_start):
        # This is an array - find its end
        array_end = text.rfind("]")
        if array_end != -1 and array_end > array_start:
            json_text = text[array_start:array_end+1]
            # Fix common JSON errors
            json_text = fix_json_errors(json_text)
            return json_text
    
    # Fall back to JSON object
    if obj_start != -1:
        obj_end = text.rfind("}")
        if obj_end != -1 and obj_end > obj_start:
            json_text = text[obj_start:obj_end+1]
            json_text = fix_json_errors(json_text)
            return json_text
    
    return text


def fix_json_errors(json_text: str) -> str:
    """Fix common JSON syntax errors like unterminated strings, trailing commas, unexpected characters, etc."""
    # Remove unexpected unicode characters and control characters
    import re
    # Remove non-ASCII characters that aren't part of valid JSON
    json_text = re.sub(r'[^\x00-\x7F\u0080-\uFFFF]+', '', json_text)
    # Remove specific problematic patterns like "자를"
    json_text = json_text.replace('자를', '').replace('\\uc790\\ub97c', '')
    
    # Fix unterminated strings at the end of lines
    # Pattern: "value at end of line without closing quote
    lines = json_text.split('\n')
    fixed_lines = []
    
    for i, line in enumerate(lines):
        # Check if line has unterminated string (odd number of quotes after the colon)
        if '": "' in line or '":\\"' in line:
            # Count quotes after the last ": " or ":\\"
            if '": "' in line:
                parts = line.split('": "', 1)
            elif '":\\"' in line:
                parts = line.split('":\\"', 1)
            else:
                fixed_lines.append(line)
                continue
                
            if len(parts) == 2:
                value_part = parts[1]
                # Count unescaped quotes
                quote_count = 0
                escaped = False
                for char in value_part:
                    if char == '\\' and not escaped:
                        escaped = True
                        continue
                    if char == '"' and not escaped:
                        quote_count += 1
                    escaped = False
                
                # If odd number of quotes, add closing quote before comma or end
                if quote_count % 2 == 0:  # Even means unterminated (we expect odd: opening + closing)
                    # Add closing quote before comma or at end
                    if ',' in value_part:
                        value_part = value_part.replace(',', '",', 1)
                    elif not value_part.rstrip().endswith('"'):
                        value_part = value_part.rstrip() + '"'
                    line = parts[0] + '": "' + value_part
        
        fixed_lines.append(line)
    
    json_text = '\n'.join(fixed_lines)
    
    # Remove trailing commas before closing brackets
    json_text = re.sub(r',(\s*[\]}])', r'\1', json_text)
    
    # Fix incomplete last object in array - if ends with { without closing }
    if json_text.count('{') > json_text.count('}'):
        json_text = json_text.rstrip()
        if not json_text.endswith('}'):
            json_text += '}'
    
    return json_text


def get_extraction_chunks() -> List[Dict[str, Any]]:
    """Return chunk definitions whose prompts come from `agents.py`.
    
    Toggle chunks by commenting/uncommenting lines below.
    Start with Section A only to test, then enable others gradually.

    Chunks:
      - sectionA_complete -> create_sectionA_agent()
      - sectionB_complete -> create_sectionB_agent()
      - sectionC_p1_p2 -> create_principles_1_2_agent()
      - sectionC_p3_p4 -> create_principles_3_4_agent()
      - sectionC_p5_p6 -> create_principles_5_6_agent()
      - sectionC_p7_p8_p9 -> create_principles_7_8_9_agent()
    """
    secA = create_sectionA_agent()["prompt"]
    secB = create_sectionB_agent()["prompt"]
    p1p2 = create_principles_1_2_agent()["prompt"]
    p3p4 = create_principles_3_4_agent()["prompt"]
    p5p6 = create_principles_5_6_agent()["prompt"]
    p7p8p9 = create_principles_7_8_9_agent()["prompt"]

    # TESTING MODE: Uncomment chunks one by one to test and debug
    # FREE TIER: 5 requests/minute = minimum 12s between chunks (using 15s to be safe)
    return [
        # Step 1: Test Section A first
        {"id": "sectionA_complete", "name": "Section A: Complete Company Information", "delay_seconds": 15, "prompt": secA},
        
        # Step 2: After Section A works, uncomment Section B
        # {"id": "sectionB_complete", "name": "Section B: Policies and Governance", "delay_seconds": 15, "prompt": secB},
        
        # Step 3: Uncomment Section C chunks one by one
        # {"id": "sectionC_p1_p2", "name": "Section C: Principles 1-2", "delay_seconds": 15, "prompt": p1p2},
        # {"id": "sectionC_p3_p4", "name": "Section C: Principles 3-4", "delay_seconds": 15, "prompt": p3p4},
        # {"id": "sectionC_p5_p6", "name": "Section C: Principles 5-6", "delay_seconds": 15, "prompt": p5p6},
        # {"id": "sectionC_p7_p8_p9", "name": "Section C: Principles 7-9", "delay_seconds": 15, "prompt": p7p8p9},
    ]
    
    # PRODUCTION MODE: Uncomment this when all chunks are working
    # return [
    #     {"id": "sectionA_complete", "name": "Section A: Complete Company Information", "delay_seconds": 8, "prompt": secA},
    #     {"id": "sectionB_complete", "name": "Section B: Policies and Governance", "delay_seconds": 6, "prompt": secB},
    #     {"id": "sectionC_p1_p2", "name": "Section C: Principles 1-2", "delay_seconds": 6, "prompt": p1p2},
    #     {"id": "sectionC_p3_p4", "name": "Section C: Principles 3-4", "delay_seconds": 6, "prompt": p3p4},
    #     {"id": "sectionC_p5_p6", "name": "Section C: Principles 5-6", "delay_seconds": 6, "prompt": p5p6},
    #     {"id": "sectionC_p7_p8_p9", "name": "Section C: Principles 7-9", "delay_seconds": 6, "prompt": p7p8p9},
    # ]


def extract_text_from_pdf(file_content: bytes) -> str:
    if not pdfplumber:
        raise HTTPException(status_code=500, detail="pdfplumber not installed")
    text = ""
    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            text += page_text + "\n"
    return text


def extract_text_from_excel(file_content: bytes) -> str:
    if not openpyxl:
        raise HTTPException(status_code=500, detail="openpyxl not installed")
    workbook = openpyxl.load_workbook(io.BytesIO(file_content))
    text = ""
    print(f"[Excel] Found {len(workbook.worksheets)} sheets: {[sheet.title for sheet in workbook.worksheets]}")
    
    for sheet in workbook.worksheets:
        # Add sheet name as header for context
        text += f"\n\n=== SHEET: {sheet.title} ===\n\n"
        row_count = 0
        for row in sheet.iter_rows(values_only=True):
            row_text = " | ".join([str(cell) if cell is not None else "" for cell in row])
            if row_text.strip():
                text += row_text + "\n"
                row_count += 1
        print(f"[Excel] Sheet '{sheet.title}': extracted {row_count} rows")
    
    print(f"[Excel] Total text length: {len(text)} characters")
    return text


async def extract_chunk_with_gemini(text: str, chunk: Dict[str, Any]) -> Dict[str, Any]:
    # (implementation replaced by the later, more robust version lower in this file)
    raise RuntimeError("Deprecated: use the improved extract_chunk_with_gemini implementation")


# The robust `validate_extracted_data` and `merge_extracted_data` implementations
# appear later in the file and will be used at runtime.


@app.post("/api/extract")
async def extract_brsr_data(file: UploadFile = File(...)):
    filename = file.filename.lower()
    if not any(filename.endswith(ext) for ext in [".pdf", ".xlsx", ".xls"]):
        raise HTTPException(status_code=400, detail="Only PDF and Excel files are supported")

    content = await file.read()
    text = extract_text_from_pdf(content) if filename.endswith('.pdf') else extract_text_from_excel(content)

    chunks = get_extraction_chunks()
    results = []
    failed = []
    for ch in chunks:
        try:
            res = await extract_chunk_with_gemini(text, ch)
            results.append((ch['id'], res))
            await asyncio.sleep(ch.get('delay_seconds', GEMINI_CONFIG['delay_between_chunks']))
        except Exception:
            results.append((ch['id'], {}))
            failed.append(ch['id'])

    merged = merge_extracted_data(results)
    return {"success": True, "data": merged, "failed_chunks": failed}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
    pass


async def extract_chunk_with_gemini(text: str, chunk: Dict[str, Any]) -> Dict[str, Any]:
    """Extract data using Gemini API with rate limiting, retry logic, and JSON repair"""
    if not GOOGLE_API_KEY or not genai:
        raise HTTPException(status_code=500, detail="Gemini API not configured. Set GOOGLE_API_KEY.")
    
    model = genai.GenerativeModel(GEMINI_CONFIG["model"])
    
    # Wait for rate limit
    await wait_for_rate_limit()
    
    # Truncate text to max tokens (approximate)
    max_chars = GEMINI_CONFIG["max_input_tokens"] * 4  # ~4 chars per token
    truncated_text = text[:max_chars]
    
    # Original prompt structure:
    # prompt = f"""You are a BRSR (Business Responsibility and Sustainability Reporting) expert.
    # Extract data from this document following SEBI BRSR Annexure 1 format.

    # {chunk['prompt']}

    # Document text:
    # {truncated_text}

    # CRITICAL INSTRUCTIONS:
    # 1. Return ONLY valid JSON - no markdown, no code blocks, no explanations
    # 2. Use the exact field names specified in the structure
    # 3. If data is not found, use empty string "" not null
    # 4. For Yes/No fields, use exact "Yes" or "No" or "Y" or "N"
    # 5. For percentages, include the % symbol
    # 6. For amounts, include currency symbol and units"""
    
    # Updated prompt as per updates section
    prompt = f"""You are a BRSR (Business Responsibility and Sustainability Reporting) expert with advanced calculation capabilities.
Extract data from this Indian company's annual report following SEBI BRSR Annexure 1 format.

{chunk['prompt']}

Document text:
{truncated_text}

CRITICAL INSTRUCTIONS:
1. Return ONLY valid JSON - no markdown code blocks, no explanations before or after
2. Use the exact field names specified
3. If data is not found, use empty string ""
4. For numbers, use just the numeric value
5. For percentages, include % symbol

CALCULATION & PERCENTAGE INSTRUCTIONS (IMPORTANT):
6. AUTOMATICALLY CALCULATE percentage fields when you have:
   - Base values like Total Turnover, Total Revenue, Total Employees, Total Energy, Total Water, etc.
   - Category/segment values (e.g., renewable energy, male employees, water consumed)
   - Formula: (Category Value / Total Value) × 100
   - Example: If Turnover = 185.6 INR Cr and Renewable Energy Spend = 50 INR Cr, calculate: (50/185.6)×100 = 26.94%

7. DERIVE missing fields from available data:
   - If you have FY and PY (Previous Year) values, calculate growth rates
   - If you have absolute numbers and totals, calculate percentages
   - If you have percentages and totals, calculate absolute values
   - If you have intensity metrics components, calculate the ratio

8. INTELLIGENT DATA EXTRACTION:
   - Look for related data across different sections of the document
   - Use footnotes, tables, and text to find base values (turnover, employee count, etc.)
   - Cross-reference data to ensure calculations are accurate
   - Round percentages to 2 decimal places

9. Keep responses concise - no long paragraphs in values
10. Return the COMPLETE array - do not truncate or stop mid-response
11. Ensure the JSON array is properly closed with ]
12. Your response MUST start with [ and end with ]

CALCULATION EXAMPLES:
- Water intensity per turnover: (Total Water Withdrawal / Turnover) 
- % Female Employees: (Female Count / Total Employees) × 100
- % Board Independence: (Independent Directors / Total Directors) × 100
- Energy intensity: (Total Energy / Production Output)"""
    
    for attempt in range(GEMINI_CONFIG["max_retries"]):
        try:
            print(f"[Chunk: {chunk['id']}] Attempt {attempt + 1}...")
            
            response = await asyncio.to_thread(
                model.generate_content,
                prompt,
                generation_config={
                    "temperature": 0.1,
                    "max_output_tokens": GEMINI_CONFIG["max_output_tokens"]
                },
                safety_settings={
                    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                }
            )
            
            # Debug: Check if response exists and for safety blocks
            if not response:
                print(f"[Chunk: {chunk['id']}] No response from Gemini")
                raise ValueError("Empty response from Gemini")
            
            # Check for blocked responses
            if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                print(f"[Chunk: {chunk['id']}] Prompt feedback: {response.prompt_feedback}")
            
            if not hasattr(response, 'text') or not response.text:
                print(f"[Chunk: {chunk['id']}] No text in response. Candidates: {response.candidates if hasattr(response, 'candidates') else 'N/A'}")
                if hasattr(response, 'candidates') and response.candidates:
                    print(f"[Chunk: {chunk['id']}] First candidate: {response.candidates[0]}")
                raise ValueError("No text in Gemini response - possibly blocked by safety filters")
            
            response_text = response.text.strip()
            print(f"[Chunk: {chunk['id']}] Response length: {len(response_text)} chars")
            
            # Save raw response to file for debugging
            debug_dir = "extraction_output/debug"
            os.makedirs(debug_dir, exist_ok=True)
            debug_file = os.path.join(debug_dir, f"{chunk['id']}_raw_response.txt")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(response_text)
            print(f"[Chunk: {chunk['id']}] Saved raw response to: {debug_file}")
            
            # Debug: Print first 200 chars of response
            if response_text:
                print(f"[Chunk: {chunk['id']}] Response preview: {response_text[:200]}...")
            else:
                print(f"[Chunk: {chunk['id']}] WARNING: Empty response text")
                raise ValueError("Empty response text from Gemini")
            
            # Check if response looks incomplete
            if len(response_text) < 1000:
                print(f"[Chunk: {chunk['id']}] WARNING: Response seems short/incomplete")
            
            repaired_json = repair_json(response_text)
            print(f"[Chunk: {chunk['id']}] Repaired JSON length: {len(repaired_json)} chars")
            print(f"[Chunk: {chunk['id']}] Repaired JSON starts with: {repaired_json[:50]}...")
            print(f"[Chunk: {chunk['id']}] Repaired JSON ends with: ...{repaired_json[-50:]}")
            
            # Save repaired JSON to file for debugging
            repaired_file = os.path.join(debug_dir, f"{chunk['id']}_repaired_json.txt")
            with open(repaired_file, 'w', encoding='utf-8') as f:
                f.write(repaired_json)
            print(f"[Chunk: {chunk['id']}] Saved repaired JSON to: {repaired_file}")
            
            try:
                result = json.loads(repaired_json)
                
                # Convert key-value array format to flat dictionary
                if isinstance(result, list):
                    print(f"[Chunk: {chunk['id']}] Converting key-value array to flat dict...")
                    flat_dict = {}
                    
                    # Fields that should be parsed as JSON even without _array suffix
                    json_fields = ["sectiona_materialIssues_array"]
                    
                    for item in result:
                        if isinstance(item, dict) and "key" in item and "value" in item:
                            key = item["key"]
                            value = item["value"]
                            
                            # If value is a JSON string and key ends with _array OR is in json_fields, parse it
                            if isinstance(value, str) and (key.endswith("_array") or key in json_fields):
                                try:
                                    # Try to parse JSON string to actual array/object
                                    parsed_value = json.loads(value)
                                    
                                    # Fix field names in materialIssues array to match frontend structure
                                    if key == "sectiona_materialIssues_array" and isinstance(parsed_value, list):
                                        for issue in parsed_value:
                                            if isinstance(issue, dict):
                                                # Rename: materialIssue → issue, riskOpportunity → type
                                                if "materialIssue" in issue:
                                                    issue["issue"] = issue.pop("materialIssue")
                                                if "riskOpportunity" in issue:
                                                    issue["type"] = issue.pop("riskOpportunity")
                                                if "approachToMitigate" in issue:
                                                    issue["approach"] = issue.pop("approachToMitigate")
                                                if "financialImplication" in issue:
                                                    issue["financialImplications"] = issue.pop("financialImplication")
                                    
                                    flat_dict[key] = parsed_value
                                except json.JSONDecodeError:
                                    # If parsing fails, keep as string
                                    flat_dict[key] = value
                            else:
                                flat_dict[key] = value
                    
                    result = flat_dict
                    print(f"[Chunk: {chunk['id']}] Converted {len(flat_dict)} key-value pairs")
                    
                    # Save flat dict to file for debugging
                    flat_dict_file = os.path.join(debug_dir, f"{chunk['id']}_flat_dict.json")
                    with open(flat_dict_file, 'w', encoding='utf-8') as f:
                        json.dump(flat_dict, f, indent=2, ensure_ascii=False)
                    print(f"[Chunk: {chunk['id']}] Saved flat dict to: {flat_dict_file}")
                
            except json.JSONDecodeError as inner_e:
                print(f"[Chunk: {chunk['id']}] JSON parse error, trying fallback extraction...")
                print(f"[Chunk: {chunk['id']}] Repaired JSON preview: {repaired_json[:300]}...")
                
                # Try to extract array or object - use a more robust pattern
                # First try array (for key-value format)
                array_match = re.search(r'\[[\s\S]*\]', repaired_json)
                if array_match:
                    try:
                        result = json.loads(array_match.group())
                        print(f"[Chunk: {chunk['id']}] Fallback: Extracted array with {len(result) if isinstance(result, list) else 'unknown'} items")
                        
                        # Convert key-value array to flat dict
                        if isinstance(result, list):
                            flat_dict = {}
                            for item in result:
                                if isinstance(item, dict) and "key" in item and "value" in item:
                                    flat_dict[item["key"]] = item["value"]
                            result = flat_dict
                            print(f"[Chunk: {chunk['id']}] Fallback: Converted {len(flat_dict)} key-value pairs")
                        else:
                            print(f"[Chunk: {chunk['id']}] Fallback: Result is not a list, type: {type(result)}")
                    except Exception as fallback_err:
                        print(f"[Chunk: {chunk['id']}] Fallback array parsing failed: {fallback_err}")
                        # If array parsing fails, manually extract all key-value pairs
                        print(f"[Chunk: {chunk['id']}] Attempting manual key-value extraction...")
                        flat_dict = {}
                        # Find all {"key": "...", "value": "..."} patterns
                        kv_pattern = r'\{\s*"key"\s*:\s*"([^"]+)"\s*,\s*"value"\s*:\s*("(?:[^"\\]|\\.)*"|[^,}]+)\s*\}'
                        matches = re.finditer(kv_pattern, repaired_json)
                        count = 0
                        for match in matches:
                            key = match.group(1)
                            value_str = match.group(2)
                            # Parse the value (remove quotes if it's a string, or parse JSON if it's an object/array)
                            try:
                                # First json.loads removes outer quotes
                                value = json.loads(value_str)
                                # If result is still a string starting with [ or {, parse again (double-escaped JSON)
                                if isinstance(value, str) and (value.startswith('[') or value.startswith('{')):
                                    try:
                                        value = json.loads(value)
                                    except:
                                        pass  # Keep as string if second parse fails
                            except:
                                value = value_str.strip('"')
                            flat_dict[key] = value
                            count += 1
                        
                        if flat_dict:
                            result = flat_dict
                            print(f"[Chunk: {chunk['id']}] Manual extraction: Found {count} key-value pairs")
                        else:
                            print(f"[Chunk: {chunk['id']}] Fallback: No key-value pairs found")
                            raise inner_e
                else:
                    # Try object fallback
                    print(f"[Chunk: {chunk['id']}] Fallback: No array found, trying object extraction...")
                    obj_match = re.search(r'\{[^{}]*\}', repaired_json)
                    if obj_match:
                        result = json.loads(obj_match.group())
                        print(f"[Chunk: {chunk['id']}] Fallback: Extracted single object")
                    else:
                        print(f"[Chunk: {chunk['id']}] Fallback: No valid JSON found in repaired text")
                        raise inner_e
            
            # Original code for validation:
            # # Validate critical fields using guidance
            # validate_extracted_data(result, chunk['id'])
            
            # Updated validation call
            validate_extracted_data(result, chunk['id'])
            
            print(f"[Chunk: {chunk['id']}] Success!")
            return result
            
        except json.JSONDecodeError as e:
            print(f"[Chunk: {chunk['id']}] JSON parse error (attempt {attempt + 1}): {e}")
            if attempt < GEMINI_CONFIG["max_retries"] - 1:
                # delay = GEMINI_CONFIG["retry_delay_base"] ** (attempt + 1) # Original
                # Updated delay calculation
                delay = GEMINI_CONFIG["retry_delay_base"] * (attempt + 1)
                print(f"[Chunk: {chunk['id']}] Retrying in {delay}s...")
                await asyncio.sleep(delay)
        except Exception as e:
            error_str = str(e).lower() # Added for easier error checking
            print(f"[Chunk: {chunk['id']}] Error (attempt {attempt + 1}): {e}")
            if "429" in str(e) or "quota" in error_str or "rate" in error_str or "exceeded" in error_str: # Updated error checking
                # Rate limit hit - wait full minute for free tier quota to reset
                delay = 60  # Always wait 60s for rate limit (free tier resets every minute)
                print(f"[Chunk: {chunk['id']}] Rate limit hit! Free tier is 5 requests/minute. Waiting {delay}s for quota reset...")
                await asyncio.sleep(delay)
            elif attempt < GEMINI_CONFIG["max_retries"] - 1:
                # delay = GEMINI_CONFIG["retry_delay_base"] ** (attempt + 1) # Original
                # Updated delay calculation
                delay = GEMINI_CONFIG["retry_delay_base"] * (attempt + 1)
                await asyncio.sleep(delay)
    
    print(f"[Chunk: {chunk['id']}] Failed after {GEMINI_CONFIG['max_retries']} attempts")
    return {}


def validate_extracted_data(data: Dict[str, Any], chunk_id: str):
    """Placeholder validator (keeps code path stable)."""
    return True

def merge_extracted_data(all_chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge all extracted chunks into final BRSR data structure matching frontend demo format.
    
    This function transforms Gemini's flat output keys (e.g., "sectiona_cin") into the 
    nested structure expected by the frontend (e.g., {"sectionA": {"cin": "..."}}).
    """
    # Start with empty result - will build nested structure from flat keys
    result = {}
    
    for chunk_id, flat_data in all_chunks:
        if not flat_data:
            print(f"[Merge] Skipping empty chunk: {chunk_id}")
            continue
        
        print(f"[Merge] Processing {chunk_id} with {len(flat_data)} flat keys")
        
        # Transform flat keys to nested structure
        try:
            nested_chunk = transform_flat_to_nested(flat_data)
            print(f"[Merge] Transformed to nested structure: {list(nested_chunk.keys())}")
            
            # Deep merge this chunk into result
            result = merge_nested_data(result, nested_chunk)
            
        except Exception as e:
            print(f"[Merge ERROR] Failed to transform chunk {chunk_id}: {str(e)}")
            continue
    
    print(f"[Merge] Final structure sections: {list(result.keys())}")
    
    # Save final merged result for debugging
    debug_dir = "extraction_output/debug"
    os.makedirs(debug_dir, exist_ok=True)
    final_output_file = os.path.join(debug_dir, "final_merged_output.json")
    with open(final_output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"[Merge] Saved final output to: {final_output_file}")
    
    # Ensure all required sections exist (even if empty)
    if "sectionA" not in result:
        result["sectionA"] = {}
    if "sectionB" not in result:
        result["sectionB"] = {}
    if "sectionC" not in result:
        result["sectionC"] = {}
    
    return result


@app.post("/api/extract")
async def extract_brsr_data(file: UploadFile = File(...)):
    """Extract BRSR data from uploaded PDF/Excel using Gemini with chunked processing"""
    
    # Validate file type
    filename = file.filename.lower()
    if not any(filename.endswith(ext) for ext in ['.pdf', '.xlsx', '.xls']):
        raise HTTPException(status_code=400, detail="Only PDF and Excel files are supported")
    
    # Read file content
    file_content = await file.read()
    
    # Extract text
    if filename.endswith('.pdf'):
        text = extract_text_from_pdf(file_content)
    else:
        text = extract_text_from_excel(file_content)
    
    print(f"[Extract] Extracted {len(text)} characters from {file.filename}")
    
    if len(text) < 100:
        raise HTTPException(status_code=400, detail="Could not extract sufficient text from file")
    
    # Create output directory for JSON files
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"extraction_output/{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    print(f"[Output] Saving extraction results to: {output_dir}")
    
    chunks = get_extraction_chunks()
    all_chunk_results = []
    failed_chunks = []
    
    # Process each chunk sequentially with proper rate limiting
    for i, chunk in enumerate(chunks):
        print(f"\n[Progress] Processing chunk {i+1}/{len(chunks)}: {chunk['name']}")
        
        try:
            chunk_data = await extract_chunk_with_gemini(text, chunk)
            all_chunk_results.append((chunk["id"], chunk_data))
            
            # Save individual chunk result to JSON file
            chunk_file = f"{output_dir}/chunk_{i+1}_{chunk['id']}.json"
            with open(chunk_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "chunk_id": chunk["id"],
                    "chunk_name": chunk["name"],
                    "timestamp": timestamp,
                    "data": chunk_data
                }, f, indent=2, ensure_ascii=False)
            print(f"[Saved] {chunk_file}")
            
            if not chunk_data:
                failed_chunks.append(chunk['name'])
            
            # Delay between chunks
            if i < len(chunks) - 1:
                delay = chunk.get("delay_seconds", GEMINI_CONFIG["delay_between_chunks"])
                print(f"[Delay] Waiting {delay}s before next chunk...")
                await asyncio.sleep(delay)
                
        except Exception as e:
            print(f"[Error] Chunk {chunk['id']} failed: {e}")
            failed_chunks.append(chunk['name'])
            all_chunk_results.append((chunk["id"], {}))
    
    # Merge all chunks
    extracted_data = merge_extracted_data(all_chunk_results)
    
    # Save merged final result
    final_file = f"{output_dir}/final_merged_data.json"
    with open(final_file, 'w', encoding='utf-8') as f:
        json.dump({
            "timestamp": timestamp,
            "source_file": file.filename,
            "total_chunks": len(chunks),
            "successful_chunks": len(chunks) - len(failed_chunks),
            "failed_chunks": failed_chunks,
            "merged_data": extracted_data
        }, f, indent=2, ensure_ascii=False)
    print(f"[Saved] Final merged data: {final_file}")
    
    success_count = len(chunks) - len(failed_chunks)
    
    return {
        "success": success_count > 0,
        "data": extracted_data,
        "message": f"Extracted BRSR Annexure 1 (Full Report) from {file.filename}",
        "reportType": "BRSR Annexure 1 (Full Report)",
        "stats": {
            "totalChunks": len(chunks),
            "successfulChunks": success_count,
            "failedChunks": failed_chunks
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

