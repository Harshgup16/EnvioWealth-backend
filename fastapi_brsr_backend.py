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

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

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
    # Model options (in order of capability vs cost):
    # - "gemini-2.5-flash" (recommended) - Best accuracy for complex BRSR extraction
    # - "gemini-2.5-flash-lite" (experimental) - Faster/cheaper but may reduce accuracy on complex fields
    # - "gemini-1.5-flash" - Older model, may have higher free tier limits
    "model": os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite"),  # Change to "gemini-2.5-flash-lite" to test
    "max_retries": 3,
    "retry_delay_base": 2,
    "delay_between_chunks": 1,  # Minimal delay for paid tier (360 req/min)
    "max_input_tokens": 800000,  # Increased to 800K tokens (~3.2M chars) - Gemini 2.5 Flash supports 1M tokens
    "max_output_tokens": 32768,
    "requests_per_minute": 360,  # Paid tier: 360 requests/min
    "enable_parallel_processing": True,  # Set to False to revert to sequential
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


def fill_nil_defaults(obj: Any) -> Any:
    """Recursively replace empty strings or None with "NIL" in the given object.

    - Empty strings ("" or whitespace-only) -> "NIL"
    - None -> "NIL"
    - Lists and dicts are traversed recursively; empty lists are left as-is but their elements are processed.
    """
    if obj is None:
        return "NIL"
    if isinstance(obj, str):
        return obj if obj.strip() != "" else "NIL"
    if isinstance(obj, list):
        return [fill_nil_defaults(x) for x in obj]
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            out[k] = fill_nil_defaults(v)
        return out
    return obj


def get_extraction_chunks() -> List[Dict[str, Any]]:
    """Return chunk definitions whose prompts come from `agents.py`.
    
    Toggle chunks by commenting/uncommenting lines below.
    Currently: TESTING SECTIONS A, B, C P1-P2
    
    Chunks available:
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
    # p3p4 = create_principles_3_4_agent()["prompt"]
    # p5p6 = create_principles_5_6_agent()["prompt"]
    # p7p8p9 = create_principles_7_8_9_agent()["prompt"]

    # TESTING MODE: Sections A, B, C P1-P2 enabled
    return [
        {"id": "sectionA_complete", "name": "Section A: Complete Company Information", "delay_seconds": 1, "prompt": secA},
        {"id": "sectionB", "name": "Section B: Policies and Governance", "delay_seconds": 1, "prompt": secB},
        {"id": "sectionC_p1_p2", "name": "Section C: Principles 1-2", "delay_seconds": 1, "prompt": p1p2},
        # {"id": "sectionC_p3_p4", "name": "Section C: Principles 3-4", "delay_seconds": 1, "prompt": p3p4},
        # {"id": "sectionC_p5_p6", "name": "Section C: Principles 5-6", "delay_seconds": 1, "prompt": p5p6},
        # {"id": "sectionC_p7_p8_p9", "name": "Section C: Principles 7-9", "delay_seconds": 1, "prompt": p7p8p9},
    ]


def extract_text_from_pdf(file_content: bytes) -> str:
    if not pdfplumber:
        raise HTTPException(status_code=500, detail="pdfplumber not installed")
    
    # Suppress ALL PDF parsing warnings (malformed PDFs generate noise but don't affect text extraction)
    import warnings
    import logging
    warnings.filterwarnings("ignore")
    logging.getLogger("pdfminer").setLevel(logging.ERROR)
    
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

# REMOVED OLD SEQUENTIAL-ONLY ENDPOINT - Using new parallel-capable endpoint at line ~678


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
    pass


async def extract_chunk_with_gemini(text: str, chunk: Dict[str, Any], manual_data: Dict[str, Any] = None, manual_data_b: Dict[str, Any] = None, manual_data_cp1: Dict[str, Any] = None, manual_data_cp2: Dict[str, Any] = None) -> Dict[str, Any]:
    """Extract data using Gemini API with rate limiting, retry logic, and JSON repair.
    
    Args:
        text: PDF text to extract from
        chunk: Extraction chunk configuration
        manual_data: Optional manual Section A data from frontend that AI can validate/correct
        manual_data_b: Optional manual Section B data (Policy Matrix + policyWebLink) that AI can use as context
        manual_data_cp1: Optional manual Section C P1 data that AI should expand/elaborate
        manual_data_cp2: Optional manual Section C P2 data that AI should expand/elaborate
    """
    
    # SPECIAL HANDLING: For Section C chunks (any chunk id containing 'sectionC_p1_p2'),
    # if manual data exists, skip AI extraction and return manual data directly
    # (AI is only for elaboration in this case)
    if 'sectionC_p1_p2' in chunk.get('id', ''):
        result = {}
        
        # If manual P1 data provided, use it directly
        if manual_data_cp1:
            print(f"[Section C P1] Using manual data directly (skipping PDF extraction)")
            result["principle1"] = manual_data_cp1
        
        # If manual P2 data provided, use it directly  
        if manual_data_cp2:
            print(f"[Section C P2] Using manual data directly (skipping PDF extraction)")
            result["principle2"] = manual_data_cp2
        
        # If we have manual data for this chunk, return it without calling AI
        if manual_data_cp1 or manual_data_cp2:
            print(f"[Section C] Returning user-provided data without AI extraction")
            return result
    
    if not GOOGLE_API_KEY or not genai:
        raise HTTPException(status_code=500, detail="Gemini API not configured. Set GOOGLE_API_KEY.")
    
    model = genai.GenerativeModel(GEMINI_CONFIG["model"])
    
    # Wait for rate limit
    await wait_for_rate_limit()
    
    # Truncate text to max tokens (approximate)
    max_chars = GEMINI_CONFIG["max_input_tokens"] * 4  # ~4 chars per token
    truncated_text = text[:max_chars]
    
    # Build context about manual Section A data if provided
    manual_data_context = ""
    if manual_data and chunk['id'] in ['sectionA_complete', 'sectionA_part1']:
        manual_data_context = f"""

USER PROVIDED MANUAL DATA (Section A - ALWAYS USE THIS):
The user has manually entered the following Section A data via frontend form.
This data is AUTHORITATIVE and you MUST NOT override it.
{json.dumps(manual_data, indent=2)}

CONTEXT USAGE INSTRUCTIONS:
- DO NOT extract/override these fields from PDF - user data is final
- USE this data as CONTEXT to better understand the company
- Use employee/worker counts to calculate percentages in OTHER sections
- Use contact info to identify the company in the document
- Use turnover rates as reference for validating OTHER sections
- If you see contradictions in PDF, TRUST THE USER DATA
- Focus on extracting OTHER Section A fields not provided by user

Example usage:
- User provided 150 male employees → Use this for calculating % in other fields
- User provided turnover 3.2% → Don't extract turnover, but use it to validate other metrics
- User contact: john@example.com → Focus on extracting other fields like CIN, entity name, etc.
"""
    
    # Build context about manual Section B data if provided
    if manual_data_b and chunk.get('id') == 'sectionB':
        policy_matrix_summary = ""
        if manual_data_b.get("policyMatrix"):
            policy_matrix_summary = "\nPolicy Matrix provided by user:\n"
            for principle, data in manual_data_b["policyMatrix"].items():
                policy_matrix_summary += f"  {principle.upper()}: hasPolicy={data.get('hasPolicy', False)}, approvedByBoard={data.get('approvedByBoard', False)}, "
                policy_matrix_summary += f"translatedToProcedures={data.get('translatedToProcedures', False)}"
                if data.get('webLink'):
                    policy_matrix_summary += f", webLink={data['webLink']}"
                policy_matrix_summary += "\n"
        
        policyWebLink = manual_data_b.get("policyWebLink", "")
        
        manual_data_context = f"""

USER PROVIDED MANUAL DATA (Section B - Policy Matrix - AUTHORITATIVE):
The user has manually entered Section B Policy Matrix and general policy web link.
This data is AUTHORITATIVE - DO NOT override these specific fields, but USE AS CONTEXT for extracting OTHER Section B fields.
{policy_matrix_summary}
General Policy Web Link: {policyWebLink}

CONTEXT USAGE INSTRUCTIONS FOR SECTION B:
1. DO NOT extract/override Policy Matrix fields (hasPolicy, approvedByBoard, translatedToProcedures, webLink for P1-P9)
2. DO NOT extract/override policyWebLink - user has provided authoritative version
3. ABSOLUTELY EXTRACT these fields from PDF (user did NOT provide):
   - Q3: valueChainExtension (Do policies extend to value chain partners?)
   - Q4: certifications (National/International certifications)
   - Q5: commitments (Specific commitments, goals, targets with timeline)
   - Q6: performance (Performance against commitments)
   - Q7: directorStatement (Director's statement on ESG challenges/targets)
   - Q8: highestAuthority (Name, designation, DIN, email, phone)
   - Q9: sustainabilityCommittee (Committee details)
   - Q10: review (Review of NGRBC performance, frequency, compliance)
   - Q11: independentAssessment (Independent assessment for P1-P9)
   - Q12: noPolicyReasons (Reasons if no policy - for any principle)

4. USE user's Policy Matrix as CONTEXT to:
   - Understand which principles the company focuses on
   - Validate consistency when extracting Q3-Q12 data
   - If user says P1 hasPolicy=Yes, expect to find related data in PDF about ethics/governance
   - Use web links as reference to understand policy scope
   - Elaborate/expand on policies mentioned if user input says "Elaborate" or similar

5. ELABORATION CAPABILITY:
   - If user input contains "Elaborate", "Expand", "Detail this", or incomplete sentences → Search PDF for detailed information
   - Example: User writes "Sustainability policy" in webLink → Extract full policy details from PDF
   - Example: User writes "Elaborate P1" → Find and extract comprehensive P1 (ethics) policy information from document
   - Use user's brief inputs as SEEDS to find comprehensive details in the PDF

6. INTELLIGENCE:
   - Cross-reference user's policy data with PDF content
   - If user marked hasPolicy=Yes for P6 (Environment), prioritize extracting environmental commitments (Q5), certifications (Q4)
   - Use policy web links as hints to locate related sections in PDF
   - If contradictions exist between user input and PDF, TRUST USER for Policy Matrix, EXTRACT from PDF for Q3-Q12
"""
    
    # Build context for Section C P1 elaboration if provided
    if manual_data_cp1 and 'sectionC_p1_p2' in chunk.get('id', ''):
        cp1_essential = manual_data_cp1.get("essential", {})
        cp1_q1 = cp1_essential.get("q1_percentageCoveredByTraining", {})
        
        # Check if any topicsCovered fields have brief inputs that need elaboration
        topics_to_elaborate = []
        for category in ["boardOfDirectors", "kmp", "employees", "workers"]:
            cat_data = cp1_q1.get(category, {})
            topics = cat_data.get("topicsCovered", "")
            if topics and len(topics.strip()) > 0:
                topics_to_elaborate.append(f"{category}: {topics}")
        
        if topics_to_elaborate:
            manual_data_context += f"""

USER PROVIDED SECTION C P1 DATA (PRINCIPLE 1 - AUTHORITATIVE MANUAL INPUT):
The user has manually entered Section C Principle 1 data via frontend form.
This data is AUTHORITATIVE - DO NOT extract Section C P1 from PDF.

User's Complete Section C P1 Data:
{json.dumps(manual_data_cp1, indent=2)}

CRITICAL INSTRUCTIONS FOR SECTION C P1:
1. DO NOT EXTRACT Section C P1 from PDF - User has provided all data manually
2. ONLY TASK: Elaborate/expand the "topicsCovered" field where user entered brief text
3. ALL OTHER FIELDS: Use exactly as user provided (totalProgrammes, percentageCovered, etc.)

ELABORATION TASK - ONLY FOR topicsCovered FIELD:
User's Brief Topics to Elaborate:
{chr(10).join(topics_to_elaborate)}

ELABORATION INSTRUCTIONS:
1. USER INPUT IS A SEED: Treat user's brief text as a keyword to expand
2. SEARCH PDF FOR DETAILS: Find comprehensive training information related to:
   - Ethics and anti-corruption training programs
   - Code of conduct and business ethics
   - Conflict of interest policies
   - Anti-bribery and anti-corruption measures
   - Transparent business practices
   - Compliance with NGRBC Principle 1

3. EXPAND BRIEF INPUTS: Transform short keywords into detailed descriptions (50-150 words)
   Examples:
   - "Ethics" → "Ethics and anti-corruption training covering NGRBC Principle 1, including conflict of interest policies, code of conduct, transparent business practices, prevention of corruption, and adherence to ethical business standards"
   - "Compliance" → "Compliance training programs on regulatory requirements, SEBI guidelines, Companies Act provisions, anti-bribery laws, FCPA compliance, and ethical governance practices"
   - "Safety" → "Safety and workplace ethics training covering occupational health protocols, accident prevention, emergency response procedures, personal protective equipment usage, hazard identification, and creating a safe working environment in line with ethical workplace practices"
   - "Governance" → "Corporate governance and ethics training covering board responsibilities, fiduciary duties, shareholder rights, transparent disclosure practices, risk management, internal controls, audit committee functions, and adherence to listing regulations and governance codes"

4. WHAT NOT TO CHANGE:
   - totalProgrammes: Keep user's exact number
   - percentageCovered: Keep user's exact percentage
   - All other Section C P1 fields: Use user data as-is
   - ONLY modify: topicsCovered field (expand brief text to comprehensive description)

5. IF PDF LACKS DETAILS:
   - Use industry best practices for the topic
   - Include relevant NGRBC Principle 1 aspects (ethics, transparency, anti-corruption)
   - Make reasonable inferences based on company sector
   - Ensure elaboration is professional and comprehensive

6. OUTPUT REQUIREMENT:
   - Return the complete Section C P1 structure exactly as user provided
   - ONLY difference: topicsCovered fields are expanded from brief to detailed
   - All numbers, percentages, and other values remain unchanged
"""
    
    # Build context for Section C P2 elaboration if provided
    if manual_data_cp2 and 'sectionC_p1_p2' in chunk.get('id', ''):
        cp2_essential = manual_data_cp2.get("essential", {})
        cp2_leadership = manual_data_cp2.get("leadership", {})
        
        # Check fields that might need elaboration in P2
        fields_to_elaborate = []
        
        # Essential Q1: improvement details
        q1_rd = cp2_essential.get("q1_rdCapexInvestments", {}).get("rd", {})
        if q1_rd.get("improvementDetails"):
            fields_to_elaborate.append(f"R&D Improvement Details: {q1_rd.get('improvementDetails')}")
        
        q1_capex = cp2_essential.get("q1_rdCapexInvestments", {}).get("capex", {})
        if q1_capex.get("improvementDetails"):
            fields_to_elaborate.append(f"CAPEX Improvement Details: {q1_capex.get('improvementDetails')}")
        
        # Essential Q2: sustainable sourcing procedures
        q2 = cp2_essential.get("q2_sustainableSourcing", {})
        if q2.get("proceduresInPlace"):
            fields_to_elaborate.append(f"Sustainable Sourcing Procedures: {q2.get('proceduresInPlace')}")
        
        # Essential Q3: reclaim processes
        q3 = cp2_essential.get("q3_reclaimProcesses", {})
        for waste_type in ["plastics", "eWaste", "hazardousWaste", "otherWaste"]:
            waste_data = q3.get(waste_type, {})
            if waste_data.get("process"):
                fields_to_elaborate.append(f"{waste_type} Reclaim Process: {waste_data.get('process')}")
        
        # Leadership Q1: LCA details
        if cp2_leadership.get("q1_lcaDetails"):
            fields_to_elaborate.append(f"LCA Details: {cp2_leadership.get('q1_lcaDetails')}")
        
        # Leadership Q2: significant concerns
        if cp2_leadership.get("q2_significantConcerns"):
            fields_to_elaborate.append(f"Significant Concerns: {cp2_leadership.get('q2_significantConcerns')}")
        
        if fields_to_elaborate:
            manual_data_context += f"""

USER PROVIDED SECTION C P2 DATA (PRINCIPLE 2 - AUTHORITATIVE MANUAL INPUT):
The user has manually entered Section C Principle 2 data via frontend form.
This data is AUTHORITATIVE - DO NOT extract Section C P2 from PDF.

User's Complete Section C P2 Data:
{json.dumps(manual_data_cp2, indent=2)}

CRITICAL INSTRUCTIONS FOR SECTION C P2:
1. DO NOT EXTRACT Section C P2 from PDF - User has provided all data manually
2. ONLY TASK: Elaborate/expand brief text fields where user entered keywords
3. ALL OTHER FIELDS: Use exactly as user provided (numbers, percentages, etc.)

ELABORATION TASK - FOR THESE FIELDS:
User's Brief Inputs to Elaborate:
{chr(10).join(fields_to_elaborate)}

ELABORATION INSTRUCTIONS FOR SECTION C P2:
1. USER INPUT IS A SEED: Treat brief text as keywords to expand
2. SEARCH PDF FOR DETAILS: Find comprehensive information about:
   - Product lifecycle and sustainable design
   - R&D investments in sustainability
   - Sustainable sourcing and procurement practices
   - Waste reclamation and recycling processes
   - Extended Producer Responsibility (EPR)
   - Life Cycle Assessment (LCA) methodologies
   - Environmental concerns and mitigation

3. EXPAND BRIEF INPUTS: Transform short text into detailed descriptions (50-150 words)
   Examples:
   - "Recycling" → "Comprehensive recycling processes including collection, segregation, material recovery, and safe disposal systems. Implementation of circular economy principles with partnerships with authorized recyclers, waste-to-resource initiatives, and compliance with EPR regulations for plastic, e-waste, and hazardous materials"
   - "Sustainable sourcing" → "Sustainable sourcing procedures encompassing supplier ESG assessments, green procurement policies, preference for certified sustainable materials, local sourcing to reduce carbon footprint, supplier code of conduct adherence, and integration of environmental and social criteria in vendor selection and evaluation processes"
   - "LCA conducted" → "Life Cycle Assessment (LCA) conducted following ISO 14040/14044 standards, covering cradle-to-grave analysis of products including raw material extraction, manufacturing, distribution, use phase, and end-of-life disposal. Assessment includes carbon footprint, water footprint, resource depletion, and environmental impact evaluation across all lifecycle stages"

4. WHAT NOT TO CHANGE:
   - All numerical values (currentFY, previousFY amounts)
   - Percentages
   - Yes/No/Applicable fields
   - ONLY elaborate: Text description fields that are brief

5. IF PDF LACKS DETAILS:
   - Use industry best practices for sustainable product lifecycle
   - Include NGRBC Principle 2 aspects (product sustainability, circularity)
   - Reference relevant standards (ISO 14001, EPR guidelines)
   - Ensure elaboration is professional and comprehensive

6. OUTPUT REQUIREMENT:
   - Return complete Section C P2 structure exactly as user provided
   - ONLY difference: Brief text fields are expanded to detailed descriptions
   - All numbers and percentages remain unchanged
"""
    
    # Override prompt for Section C when manual data is provided
    chunk_prompt = chunk['prompt']
    if 'sectionC_p1_p2' in chunk.get('id', '') and (manual_data_cp1 or manual_data_cp2):
        # Replace extraction prompt with elaboration-only prompt
        chunk_prompt = f"""SPECIAL MODE: ELABORATION ONLY - DO NOT EXTRACT FROM PDF

You are receiving user-provided manual data for Section C Principles.
Your ONLY task is to return the user's data with elaborated text fields.

DO NOT extract Section C data from the PDF below.
The PDF is provided ONLY as reference for elaborating brief user inputs.

Return the exact JSON structure provided by the user, with ONLY the specified text fields expanded.
"""
    
    # Updated prompt as per updates section
    prompt = f"""You are a BRSR (Business Responsibility and Sustainability Reporting) expert with advanced calculation capabilities.
Extract data from this Indian company's annual report following SEBI BRSR Annexure 1 format.
{manual_data_context}
{chunk_prompt}

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


# Model for manual Section A data from frontend
class SectionAManualData(BaseModel):
    contactName: Optional[str] = None
    contactDesignation: Optional[str] = None
    contactPhone: Optional[str] = None
    contactEmail: Optional[str] = None
    reportingBoundary: Optional[str] = None
    employees: Optional[dict] = None  # {permanent: {male, female, total}, otherThanPermanent: {male, female, total}}
    workers: Optional[dict] = None    # {permanent: {male, female, total}, otherThanPermanent: {male, female, total}}
    turnover: Optional[dict] = None   # {employees: {male, female, total}, workers: {male, female, total}}


# Model for manual Section B data from frontend
# NOTE: Manual form only allows Policy Matrix + policyWebLink input
# All other Section B fields are extracted by AI from PDF only
class SectionBManualData(BaseModel):
    policyMatrix: Optional[dict] = None  # {p1-p9: {hasPolicy, approvedByBoard, webLink, translatedToProcedures}}
    policyWebLink: Optional[str] = None


@app.post("/api/extract")
async def extract_brsr_data(
    files: List[UploadFile] = File(...),
    sectionAManualData: Optional[str] = Form(None),
    sectionBManualData: Optional[str] = Form(None),
    sectionCP1ManualData: Optional[str] = Form(None),
    sectionCP2ManualData: Optional[str] = Form(None)
):
    """Extract BRSR data from uploaded PDF/Excel files using Gemini with parallel processing.
    Accepts multiple files and processes them in parallel for faster extraction.
    Accepts optional manual Section A/B data from frontend which takes precedence over extracted data.
    """
    
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")
    
    print(f"[Files] Received {len(files)} file(s) for extraction")
    
    # Validate file types
    for file in files:
        filename = file.filename.lower()
        if not any(filename.endswith(ext) for ext in ['.pdf', '.xlsx', '.xls']):
            raise HTTPException(status_code=400, detail=f"File {file.filename}: Only PDF and Excel files are supported")
    
    # Parse manual Section A data if provided
    manual_data = None
    if sectionAManualData:
        try:
            manual_data = json.loads(sectionAManualData)
            print(f"[Manual Data] Received Section A manual inputs: {list(manual_data.keys())}")
        except json.JSONDecodeError as e:
            print(f"[Warning] Could not parse manual Section A data: {e}")
    
    # Parse manual Section B data if provided
    manual_data_b = None
    if sectionBManualData:
        try:
            manual_data_b = json.loads(sectionBManualData)
            print(f"[Manual Data] Received Section B manual inputs: {list(manual_data_b.keys())}")
        except json.JSONDecodeError as e:
            print(f"[Warning] Could not parse manual Section B data: {e}")
    
    # Parse manual Section C P1 data if provided
    manual_data_cp1 = None
    if sectionCP1ManualData:
        try:
            manual_data_cp1 = json.loads(sectionCP1ManualData)
            print(f"[Manual Data] Received Section C P1 manual inputs")
        except json.JSONDecodeError as e:
            print(f"[Warning] Could not parse manual Section C P1 data: {e}")
    
    # Parse manual Section C P2 data if provided
    manual_data_cp2 = None
    if sectionCP2ManualData:
        try:
            manual_data_cp2 = json.loads(sectionCP2ManualData)
            print(f"[Manual Data] Received Section C P2 manual inputs")
        except json.JSONDecodeError as e:
            print(f"[Warning] Could not parse manual Section C P2 data: {e}")
    
    # Extract text from all files and combine
    combined_text = ""
    for idx, file in enumerate(files):
        filename = file.filename.lower()
        file_content = await file.read()
        
        # Extract text based on file type
        if filename.endswith('.pdf'):
            text = extract_text_from_pdf(file_content)
        else:
            text = extract_text_from_excel(file_content)
        
        print(f"[Extract] File {idx + 1}/{len(files)} ({file.filename}): {len(text)} characters")
        
        # Add separator between files for AI context
        if combined_text:
            combined_text += f"\n\n{'='*80}\n[FILE: {file.filename}]\n{'='*80}\n\n"
        combined_text += text
    
    print(f"[Extract] Total combined text: {len(combined_text)} characters from {len(files)} file(s)")
    
    # Use combined text for extraction
    text = combined_text
    
    if len(text) < 100:
        raise HTTPException(status_code=400, detail="Could not extract sufficient text from file")
    
    # Create output directory for JSON files
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"extraction_output/{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    print(f"[Output] Saving extraction results to: {output_dir}")
    
    # Apply NIL defaults to manual Section C data so empty fields are preserved as 'NIL'
    if manual_data_cp1:
        try:
            manual_data_cp1 = fill_nil_defaults(manual_data_cp1)
            print("[Manual Data] Section C P1 after fill_nil_defaults applied")
        except Exception as e:
            print(f"[Warning] fill_nil_defaults failed for CP1: {e}")

    if manual_data_cp2:
        try:
            manual_data_cp2 = fill_nil_defaults(manual_data_cp2)
            print("[Manual Data] Section C P2 after fill_nil_defaults applied")
        except Exception as e:
            print(f"[Warning] fill_nil_defaults failed for CP2: {e}")

    chunks = get_extraction_chunks()

    # If user provided manual data for Principle 1 or Principle 2, skip the AI extraction chunk for those
    # (the chunk id used by the extractor is 'sectionC_p1_p2')
    if manual_data_cp1 or manual_data_cp2:
        original_count = len(chunks)
        # Remove any chunk whose id refers to Section C P1/P2 (including variants like sectionC_p1_p2_p3)
        chunks = [c for c in chunks if 'sectionC_p1_p2' not in c.get('id', '')]
        print(f"[Manual Override] Removed sectionC_p1_p2 chunk(s) (skipped AI). Chunks {original_count} -> {len(chunks)}")
    all_chunk_results = []
    failed_chunks = []
    
    # Check if parallel processing is enabled
    if GEMINI_CONFIG.get("enable_parallel_processing", True):
        print(f"\n[Parallel Mode] Processing {len(chunks)} chunks simultaneously...")
        
        # Process all chunks in parallel using asyncio.gather
        async def process_chunk_wrapper(i, chunk):
            try:
                print(f"[Started] Chunk {i+1}/{len(chunks)}: {chunk['name']}")
                chunk_data = await extract_chunk_with_gemini(text, chunk, manual_data, manual_data_b, manual_data_cp1, manual_data_cp2)
                
                # Save individual chunk result
                chunk_file = f"{output_dir}/chunk_{i+1}_{chunk['id']}.json"
                with open(chunk_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        "chunk_id": chunk["id"],
                        "chunk_name": chunk["name"],
                        "timestamp": timestamp,
                        "data": chunk_data
                    }, f, indent=2, ensure_ascii=False)
                print(f"[Completed] Chunk {i+1}: {chunk['name']} - Saved to {chunk_file}")
                
                return (chunk["id"], chunk_data, chunk['name'] if not chunk_data else None)
            except Exception as e:
                print(f"[Error] Chunk {i+1} ({chunk['id']}) failed: {e}")
                return (chunk["id"], {}, chunk['name'])
        
        # Run all chunks in parallel
        results = await asyncio.gather(*[process_chunk_wrapper(i, chunk) for i, chunk in enumerate(chunks)])
        
        # Process results
        for chunk_id, chunk_data, failed_name in results:
            all_chunk_results.append((chunk_id, chunk_data))
            if failed_name:
                failed_chunks.append(failed_name)
    
    else:
        # Sequential processing (original behavior)
        print(f"\n[Sequential Mode] Processing {len(chunks)} chunks one by one...")
        for i, chunk in enumerate(chunks):
            print(f"\n[Progress] Processing chunk {i+1}/{len(chunks)}: {chunk['name']}")
            
            try:
                chunk_data = await extract_chunk_with_gemini(text, chunk, manual_data, manual_data_b, manual_data_cp1, manual_data_cp2)
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
    
    # Merge manual Section A data - USER DATA ALWAYS WINS (takes precedence over AI)
    if manual_data:
        print(f"[Merge] Merging manual Section A data (user data takes precedence)...")
        if "sectionA" not in extracted_data:
            extracted_data["sectionA"] = {}
        
        # User-provided data always overrides AI extraction - completely replace fields
        for field in ["contactName", "contactDesignation", "contactPhone", "contactEmail", "reportingBoundary"]:
            if manual_data.get(field):
                extracted_data["sectionA"][field] = manual_data[field]
                print(f"[Merge] Using user {field}")
        
        # Employee counts - COMPLETELY REPLACE with user data
        if manual_data.get("employees"):
            emp_data = manual_data["employees"]
            extracted_data["sectionA"]["employees"] = {
                "permanent": {
                    "male": emp_data.get("permanent", {}).get("male", 0),
                    "female": emp_data.get("permanent", {}).get("female", 0),
                    "total": emp_data.get("permanent", {}).get("total", 0)
                },
                "other": {
                    "male": emp_data.get("otherThanPermanent", {}).get("male", 0),
                    "female": emp_data.get("otherThanPermanent", {}).get("female", 0),
                    "total": emp_data.get("otherThanPermanent", {}).get("total", 0)
                }
            }
            print(f"[Merge] REPLACED employees with user data: Perm={emp_data.get('permanent', {})}, Other={emp_data.get('otherThanPermanent', {})}")
        
        # Worker counts - COMPLETELY REPLACE with user data
        if manual_data.get("workers"):
            worker_data = manual_data["workers"]
            extracted_data["sectionA"]["workers"] = {
                "permanent": {
                    "male": worker_data.get("permanent", {}).get("male", 0),
                    "female": worker_data.get("permanent", {}).get("female", 0),
                    "total": worker_data.get("permanent", {}).get("total", 0)
                },
                "other": {
                    "male": worker_data.get("otherThanPermanent", {}).get("male", 0),
                    "female": worker_data.get("otherThanPermanent", {}).get("female", 0),
                    "total": worker_data.get("otherThanPermanent", {}).get("total", 0)
                }
            }
            print(f"[Merge] REPLACED workers with user data")
        
        # Turnover rates - COMPLETELY REPLACE with user data
        if manual_data.get("turnover"):
            extracted_data["sectionA"]["turnover"] = manual_data["turnover"]
            print(f"[Merge] REPLACED turnover with user data")
        
        print(f"[Merge] User data successfully merged - user inputs take precedence over AI")
    
    # Merge manual Section B data - USER DATA ALWAYS WINS (takes precedence over AI)
    # NOTE: Manual form only allows Policy Matrix + policyWebLink. All other fields extracted by AI.
    if manual_data_b:
        print(f"[Merge] Merging manual Section B data (user data takes precedence)...")
        if "sectionB" not in extracted_data:
            extracted_data["sectionB"] = {}
        
        # Policy Matrix (P1-P9) - COMPLETELY REPLACE with user data
        if manual_data_b.get("policyMatrix"):
            extracted_data["sectionB"]["policyMatrix"] = {}
            
            for principle in ["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"]:
                if principle in manual_data_b["policyMatrix"]:
                    policy_data = manual_data_b["policyMatrix"][principle]
                    extracted_data["sectionB"]["policyMatrix"][principle] = {
                        "hasPolicy": "Y" if policy_data.get("hasPolicy") else "N",
                        "approvedByBoard": "Y" if policy_data.get("approvedByBoard") else "N",
                        "translatedToProcedures": "Y" if policy_data.get("translatedToProcedures") else "N",
                        "webLink": policy_data.get("webLink", "")
                    }
                else:
                    # If user didn't provide this principle, use defaults
                    extracted_data["sectionB"]["policyMatrix"][principle] = {
                        "hasPolicy": "N",
                        "approvedByBoard": "N",
                        "translatedToProcedures": "N",
                        "webLink": ""
                    }
            
            print(f"[Merge] REPLACED Policy Matrix with user data for all 9 principles")
        
        # General Policy Web Link - User data wins
        if manual_data_b.get("policyWebLink"):
            extracted_data["sectionB"]["policyWebLink"] = manual_data_b["policyWebLink"]
            print(f"[Merge] Using user policyWebLink")

        # Simple text fields
        for k in [
            "valueChainExtension",
            "certifications",
            "commitments",
            "performance",
            "directorStatement",
            "sustainabilityCommittee",
        ]:
            v = manual_data_b.get(k)
            if isinstance(v, str) and v.strip():
                extracted_data["sectionB"][k] = v
                print(f"[Merge] Using user {k}")

        # Highest Authority object
        if isinstance(manual_data_b.get("highestAuthority"), dict):
            ha = manual_data_b["highestAuthority"]
            if "highestAuthority" not in extracted_data["sectionB"]:
                extracted_data["sectionB"]["highestAuthority"] = {}
            for f in ["name", "designation", "din", "email", "phone"]:
                if f in ha and isinstance(ha[f], str) and ha[f].strip():
                    extracted_data["sectionB"]["highestAuthority"][f] = ha[f]
            print("[Merge] Using user highestAuthority details")

        # Review: performance p1..p9, frequency, compliance
        if isinstance(manual_data_b.get("review"), dict):
            rev = manual_data_b["review"]
            if "review" not in extracted_data["sectionB"]:
                extracted_data["sectionB"]["review"] = {}
            # performance
            perf = rev.get("performance")
            if isinstance(perf, dict):
                if "performance" not in extracted_data["sectionB"]["review"]:
                    extracted_data["sectionB"]["review"]["performance"] = {}
                for p in ["p1","p2","p3","p4","p5","p6","p7","p8","p9"]:
                    if isinstance(perf.get(p), str) and perf.get(p).strip():
                        extracted_data["sectionB"]["review"]["performance"][p] = perf[p]
            # frequency and compliance
            if isinstance(rev.get("performanceFrequency"), str) and rev["performanceFrequency"].strip():
                extracted_data["sectionB"]["review"]["performanceFrequency"] = rev["performanceFrequency"]
            if isinstance(rev.get("compliance"), str) and rev["compliance"].strip():
                extracted_data["sectionB"]["review"]["compliance"] = rev["compliance"]
            print("[Merge] Using user review fields where provided")

        # Independent Assessment p1..p9
        if isinstance(manual_data_b.get("independentAssessment"), dict):
            ia = manual_data_b["independentAssessment"]
            if "independentAssessment" not in extracted_data["sectionB"]:
                extracted_data["sectionB"]["independentAssessment"] = {}
            for p in ["p1","p2","p3","p4","p5","p6","p7","p8","p9"]:
                if isinstance(ia.get(p), str) and ia.get(p).strip():
                    extracted_data["sectionB"]["independentAssessment"][p] = ia[p]
            print("[Merge] Using user independentAssessment where provided")

        # No Policy Reasons - all sub-objects p1..p9
        npr = manual_data_b.get("noPolicyReasons")
        if isinstance(npr, dict):
            if "noPolicyReasons" not in extracted_data["sectionB"]:
                extracted_data["sectionB"]["noPolicyReasons"] = {}
            for sub in ["notMaterial", "notReady", "noResources", "plannedNextYear", "otherReason"]:
                sub_data = npr.get(sub)
                if isinstance(sub_data, dict):
                    if sub not in extracted_data["sectionB"]["noPolicyReasons"]:
                        extracted_data["sectionB"]["noPolicyReasons"][sub] = {}
                    for p in ["p1","p2","p3","p4","p5","p6","p7","p8","p9"]:
                        if isinstance(sub_data.get(p), str) and sub_data.get(p).strip():
                            extracted_data["sectionB"]["noPolicyReasons"][sub][p] = sub_data[p]
                    print(f"[Merge] Using user noPolicyReasons.{sub} where provided")
        
        print(f"[Merge] Section B user data successfully merged - user inputs take precedence over AI")
    
    # Merge manual Section C P1 data - USER DATA ALWAYS WINS (takes precedence over AI)
    if manual_data_cp1:
        print(f"[Merge] Merging manual Section C P1 data (user data takes precedence)...")
        if "sectionC" not in extracted_data:
            extracted_data["sectionC"] = {}
        if "principle1" not in extracted_data["sectionC"]:
            extracted_data["sectionC"]["principle1"] = {}
        
        # Essential indicators
        if "essential" in manual_data_cp1:
            if "essential" not in extracted_data["sectionC"]["principle1"]:
                extracted_data["sectionC"]["principle1"]["essential"] = {}
            
            ess = manual_data_cp1["essential"]
            
            # Recursively merge all essential fields
            for key, value in ess.items():
                if value:  # Only merge if value is provided
                    extracted_data["sectionC"]["principle1"]["essential"][key] = value
            
            print(f"[Merge] Using user Section C P1 essential data")
        
        # Leadership indicators
        if "leadership" in manual_data_cp1:
            if "leadership" not in extracted_data["sectionC"]["principle1"]:
                extracted_data["sectionC"]["principle1"]["leadership"] = {}
            
            lead = manual_data_cp1["leadership"]
            
            # Recursively merge all leadership fields
            for key, value in lead.items():
                if value:  # Only merge if value is provided
                    extracted_data["sectionC"]["principle1"]["leadership"][key] = value
            
            print(f"[Merge] Using user Section C P1 leadership data")
        
        print(f"[Merge] Section C P1 user data successfully merged")
    
    # Merge manual Section C P2 data - USER DATA ALWAYS WINS (takes precedence over AI)
    if manual_data_cp2:
        print(f"[Merge] Merging manual Section C P2 data (user data takes precedence)...")
        if "sectionC" not in extracted_data:
            extracted_data["sectionC"] = {}
        if "principle2" not in extracted_data["sectionC"]:
            extracted_data["sectionC"]["principle2"] = {}
        
        # Essential indicators
        if "essential" in manual_data_cp2:
            if "essential" not in extracted_data["sectionC"]["principle2"]:
                extracted_data["sectionC"]["principle2"]["essential"] = {}
            
            ess = manual_data_cp2["essential"]
            
            # Recursively merge all essential fields
            for key, value in ess.items():
                if value:  # Only merge if value is provided
                    extracted_data["sectionC"]["principle2"]["essential"][key] = value
            
            print(f"[Merge] Using user Section C P2 essential data")
        
        # Leadership indicators
        if "leadership" in manual_data_cp2:
            if "leadership" not in extracted_data["sectionC"]["principle2"]:
                extracted_data["sectionC"]["principle2"]["leadership"] = {}
            
            lead = manual_data_cp2["leadership"]
            
            # Recursively merge all leadership fields
            for key, value in lead.items():
                if value:  # Only merge if value is provided
                    extracted_data["sectionC"]["principle2"]["leadership"][key] = value
            
            print(f"[Merge] Using user Section C P2 leadership data")
        
        print(f"[Merge] Section C P2 user data successfully merged")
    
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
    
    file_names = ", ".join([f.filename for f in files])
    
    return {
        "success": success_count > 0,
        "data": extracted_data,
        "message": f"Extracted BRSR data from {len(files)} file(s): {file_names}",
        "reportType": "BRSR Annexure 1 (Full Report)",
        "stats": {
            "totalFiles": len(files),
            "totalChunks": len(chunks),
            "successfulChunks": success_count,
            "failedChunks": failed_chunks
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

