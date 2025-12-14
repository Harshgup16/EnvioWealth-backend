"""
FastAPI Backend for BRSR Report Generation with Chunked Gemini API Calls
Uses BRSR Field Guidance for validation and calculations

Run: uvicorn scripts.fastapi_brsr_backend:app --reload --port 8000
Set GOOGLE_API_KEY environment variable for Gemini API access.
"""

import os
import json
import asyncio
import time
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import io
import re # Added for repair_json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import BRSR field guidance
# from scripts.brsr_field_guidance import ( # Original import, updated below
#     BRSR_FIELD_GUIDANCE,
#     BRSR_SECTION_C_QUESTIONS,
#     SECTION_C_TABLE_HEADERS,
#     BRSRCalculations,
#     validate_brsr_field,
#     get_field_description
# )
# Updated import as per updates section
from brsr_field_guidance import BRSR_FIELD_GUIDANCE, BRSRCalculations, BRSR_SECTION_C_QUESTIONS


# PDF/Excel extraction
try:
    import pdfplumber
except ImportError:
    pdfplumber = None
    print("Install pdfplumber: pip install pdfplumber")

try:
    import openpyxl
except ImportError:
    openpyxl = None
    print("Install openpyxl: pip install openpyxl")

# Gemini AI
try:
    import google.generativeai as genai
    # GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "") # Original
    # Updated as per updates section
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    if GOOGLE_API_KEY:
        genai.configure(api_key=GOOGLE_API_KEY)
except ImportError:
    genai = None
    # GOOGLE_API_KEY = "" # Original
    GOOGLE_API_KEY = None # Updated as per updates section


app = FastAPI(title="BRSR Report Generator API", version="3.0.0") # Original version kept, but updates suggest 2.0.0. Using 3.0.0 for now.

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GEMINI_CONFIG = {
    "model": "gemini-2.5-flash", # Updated as per updates section
    # "rpm_limit": 15,  # Original
    "max_retries": 5,  # Increased retries (from 3)
    "retry_delay_base": 3,  # Increased base delay (from 2)
    # "tpm_limit": 32000,  # Original
    "requests_per_minute": 10,  # Conservative for free tier (from 15)
    "delay_between_chunks": 8,  # Increased delay between chunks (from 5)
    "max_input_tokens": 30000,
    # "max_output_tokens": 8192 # Original
    "max_output_tokens": 4096  # Reduced to prevent truncation (from 8192)
}

# Rate limiting tracker
# api_usage = { # Original
#     "last_request_time": 0,
#     "requests_this_minute": 0,
#     "minute_start": 0,
# }
# Updated as per updates section
rate_limit_tracker = {
    "requests": [],
    "last_reset": time.time()
}


# async def wait_for_rate_limit(): # Original
#     """Enforce rate limiting for Gemini Free Tier"""
#     global api_usage
#     current_time = time.time()
    
#     # Reset counter if a minute has passed
#     if current_time - api_usage["minute_start"] >= 60:
#         api_usage["requests_this_minute"] = 0
#         api_usage["minute_start"] = current_time
    
#     # Check if we've hit the rate limit
#     if api_usage["requests_this_minute"] >= GEMINI_CONFIG["rpm_limit"]:
#         wait_time = 60 - (current_time - api_usage["minute_start"])
#         if wait_time > 0:
#             print(f"[Rate Limit] Waiting {wait_time:.1f}s before next request...")
#             await asyncio.sleep(wait_time)
#             api_usage["requests_this_minute"] = 0
#             api_usage["minute_start"] = time.time()
    
#     # Add delay between requests
#     time_since_last = current_time - api_usage["last_request_time"]
#     if time_since_last < GEMINI_CONFIG["delay_between_chunks"]:
#         await asyncio.sleep(GEMINI_CONFIG["delay_between_chunks"] - time_since_last)
    
#     api_usage["last_request_time"] = time.time()
#     api_usage["requests_this_minute"] += 1
# Updated as per updates section
async def wait_for_rate_limit():
    """Ensure we don't exceed rate limits"""
    current_time = time.time()
    
    # Reset counter every minute
    if current_time - rate_limit_tracker["last_reset"] > 60:
        rate_limit_tracker["requests"] = []
        rate_limit_tracker["last_reset"] = current_time
    
    # If at limit, wait
    if len(rate_limit_tracker["requests"]) >= GEMINI_CONFIG["requests_per_minute"]:
        wait_time = 60 - (current_time - rate_limit_tracker["last_reset"])
        if wait_time > 0:
            print(f"[Rate Limit] Waiting {wait_time:.1f}s before next request...")
            await asyncio.sleep(wait_time)
            rate_limit_tracker["requests"] = []
            rate_limit_tracker["last_reset"] = time.time()
    
    rate_limit_tracker["requests"].append(current_time)


def repair_json(text: str) -> str:
    """Attempt to repair malformed JSON from Gemini"""
    # Remove markdown code blocks
    if "\`\`\`" in text:
        # Find JSON content between code blocks
        match = re.search(r'\`\`\`(?:json)?\s*([\s\S]*?)\s*\`\`\`', text)
        if match:
            text = match.group(1)
        else:
            # Try to extract anything that looks like JSON
            text = re.sub(r'\`\`\`\w*\n?', '', text)
    
    # Remove any leading/trailing non-JSON content
    text = text.strip()
    
    # Find the first { and last }
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end != -1 and end > start:
        text = text[start:end+1]
    
    # Fix common issues
    # 1. Remove trailing commas before } or ]
    text = re.sub(r',(\s*[}\]])', r'\1', text)
    
    # 2. Fix unescaped quotes in strings
    # This is tricky - we'll try a simple approach
    
    # 3. Fix missing quotes around keys
    text = re.sub(r'(\{|\,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', text)
    
    # 4. Replace single quotes with double quotes (but not in strings)
    # Simple approach: if it's clearly a JSON delimiter
    text = re.sub(r"'(\s*[,:\}\]\{])", r'"\1', text)
    text = re.sub(r"([,:\{\[\s])\s*'", r'\1"', text)
    
    # 5. Fix truncated JSON by closing open brackets
    open_braces = text.count('{') - text.count('}')
    open_brackets = text.count('[') - text.count(']')
    
    # Remove any incomplete key-value pair at the end
    if open_braces > 0 or open_brackets > 0:
        # Try to find the last complete entry
        # Look for the last complete string value
        last_complete = max(
            text.rfind('",'),
            text.rfind('"}'),
            text.rfind('"]'),
            text.rfind('" }'),
            text.rfind('" ]')
        )
        if last_complete > len(text) // 2:  # Only truncate if we're past halfway
            text = text[:last_complete+1]
            # Recount
            open_braces = text.count('{') - text.count('}')
            open_brackets = text.count('[') - text.count(']')
    
    # Add missing closing brackets
    text += ']' * open_brackets
    text += '}' * open_braces
    
    return text


def get_extraction_chunks() -> List[Dict[str, Any]]:
    """
    Define extraction chunks with prompts based on BRSR Field Guidance.
    Reorganized: 1 chunk for Section A, 1 for Section B, 3 for Section C (Principles 1-9)
    """
    return [
        {
            "id": "sectionA_complete",
            "name": "Section A: Complete Company Information",
            "delay_seconds": 10,
            "prompt": """Extract ALL Section A information from the BRSR document.

CRITICAL: Return ONLY a valid JSON object. No markdown code blocks (```json), no explanations, just pure JSON starting with { and ending with }.

{
    "cin": "",
    "entityName": "",
    "yearOfIncorporation": "",
    "registeredAddress": "",
    "corporateAddress": "",
    "email": "",
    "telephone": "",
    "website": "",
    "financialYear": "",
    "stockExchanges": "",
    "paidUpCapital": "",
    "contactName": "",
    "contactDesignation": "",
    "contactPhone": "",
    "contactEmail": "",
    "reportingBoundary": "",
    "assuranceProvider": "",
    "assuranceType": "",
    "businessActivities": [{"mainActivity": "", "businessDescription": "", "turnoverPercent": ""}],
    "products": [{"name": "", "nicCode": "", "turnoverPercent": ""}],
    "nationalPlants": "",
    "nationalOffices": "",
    "internationalPlants": "",
    "internationalOffices": "",
    "nationalStates": "",
    "internationalCountries": "",
    "exportContribution": "",
    "employees": {"permanent": {"male": 0, "female": 0, "total": 0}, "otherThanPermanent": {"male": 0, "female": 0, "total": 0}},
    "workers": {"permanent": {"male": 0, "female": 0, "total": 0}, "otherThanPermanent": {"male": 0, "female": 0, "total": 0}},
    "board": {"total": 0, "female": 0, "femalePercent": ""},
    "kmp": {"total": 0, "female": 0, "femalePercent": ""},
    "turnover": {"employees": {"male": "", "female": "", "total": ""}, "workers": {"male": "", "female": "", "total": ""}},
    "subsidiaries": "",
    "csr": {"prescribedAmount": "", "amountSpent": "", "surplus": ""},
    "complaints": {"communities": {"filed": 0, "pending": 0, "remarks": ""}, "investors": {"filed": 0, "pending": 0, "remarks": ""}, "shareholders": {"filed": 0, "pending": 0, "remarks": ""}, "employees": {"filed": 0, "pending": 0, "remarks": ""}, "customers": {"filed": 0, "pending": 0, "remarks": ""}, "valueChain": {"filed": 0, "pending": 0, "remarks": ""}},
    "materialIssues": [{"issue": "", "type": "", "rationale": "", "approach": "", "financialImplications": ""}]
}"""
        },
        {
            "id": "sectionB_complete",
            "name": "Section B: Policies and Governance",
            "delay_seconds": 10,
            "prompt": """Extract ALL Section B information.

CRITICAL: Return ONLY valid JSON. No markdown, no explanations.

{
    "policyMatrix": {
        "p1": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""},
        "p2": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""},
        "p3": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""},
        "p4": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""},
        "p5": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""},
        "p6": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""},
        "p7": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""},
        "p8": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""},
        "p9": {"hasPolicy": "", "approvedByBoard": "", "webLink": ""}
    },
    "governance": {
        "directorStatement": "",
        "frequencyReview": "",
        "chiefResponsibility": "",
        "weblink": ""
    }
}"""
        },
        {
            "id": "sectionC_p1_p2",
            "name": "Section C: Principles 1-2",
            "delay_seconds": 10,
            "prompt": """Extract Section C Principles 1 and 2 with detailed structure.

CRITICAL: Return ONLY valid JSON. No markdown, no explanations.

{
    "principle1": {
        "essential": {
            "q1_percentageCoveredByTraining": {
                "boardOfDirectors": {"totalProgrammes": "", "topicsCovered": "", "percentageCovered": ""},
                "kmp": {"totalProgrammes": "", "topicsCovered": "", "percentageCovered": ""},
                "employees": {"totalProgrammes": "", "topicsCovered": "", "percentageCovered": ""},
                "workers": {"totalProgrammes": "", "topicsCovered": "", "percentageCovered": ""}
            },
            "q2_finesPenalties": {"monetary": [], "nonMonetary": []},
            "q3_appealsOutstanding": "",
            "q4_antiCorruptionPolicy": {"exists": "", "details": "", "webLink": ""},
            "q5_disciplinaryActions": {"directors": {"currentFY": "", "previousFY": ""}, "kmps": {"currentFY": "", "previousFY": ""}, "employees": {"currentFY": "", "previousFY": ""}, "workers": {"currentFY": "", "previousFY": ""}},
            "q6_conflictOfInterestComplaints": {},
            "q7_correctiveActions": "",
            "q8_accountsPayableDays": {"currentFY": "", "previousFY": ""},
            "q9_opennessBusiness": {}
        },
        "leadership": {
            "q1_valueChainAwareness": [],
            "q2_conflictOfInterestProcess": {"exists": "", "details": ""}
        }
    },
    "principle2": {
        "essential": {
            "q1_rdCapexInvestments": {"rd": {"currentFY": "", "previousFY": "", "improvementDetails": ""}, "capex": {"currentFY": "", "previousFY": "", "improvementDetails": ""}},
            "q2_sustainableSourcing": {"proceduresInPlace": "", "percentageSustainablySourced": ""},
            "q3_reclaimProcesses": {"plastics": {"applicable": "", "process": ""}, "eWaste": {"applicable": "", "process": ""}, "hazardousWaste": {"applicable": "", "process": ""}, "otherWaste": {"applicable": "", "process": ""}},
            "q4_epr": {"applicable": "", "wasteCollectionPlanInLine": ""}
        },
        "leadership": {
            "q1_lcaDetails": "",
            "q2_significantConcerns": "",
            "q3_recycledInputMaterial": [],
            "q4_productsReclaimed": {},
            "q5_reclaimedPercentage": ""
        }
    }
}"""
        },
        {
            "id": "sectionC_p3",
            "name": "Section C: Principle 3 (Employee & Worker Wellbeing)",
            "delay_seconds": 10,
            "prompt": """Extract Section C Principle 3 with complete wellbeing and safety details.

CRITICAL: Return ONLY valid JSON matching the exact structure below. Fill in actual data from the document.

{
    "principle3": {
        "essential": {
            "q1a_employeeWellbeing": {
                "permanentMale": { 
                    "total": "number as string",
                    "healthInsurance": { "no": "count", "percent": "XX%" },
                    "accidentInsurance": { "no": "count", "percent": "XX%" },
                    "maternityBenefits": { "no": "NA or count", "percent": "NA or XX%" },
                    "paternityBenefits": { "no": "NA or count", "percent": "NA or XX%" },
                    "dayCare": { "no": "NA or count", "percent": "NA or XX%" }
                },
                "permanentFemale": { 
                    "total": "",
                    "healthInsurance": { "no": "", "percent": "" },
                    "accidentInsurance": { "no": "", "percent": "" },
                    "maternityBenefits": { "no": "", "percent": "" },
                    "paternityBenefits": { "no": "", "percent": "" },
                    "dayCare": { "no": "", "percent": "" }
                },
                "permanentTotal": { 
                    "total": "",
                    "healthInsurance": { "no": "", "percent": "" },
                    "accidentInsurance": { "no": "", "percent": "" },
                    "maternityBenefits": { "no": "", "percent": "" },
                    "paternityBenefits": { "no": "", "percent": "" },
                    "dayCare": { "no": "", "percent": "" }
                },
                "otherMale": "fill same structure or 'Not Applicable'",
                "otherFemale": "fill same structure or 'Not Applicable'",
                "otherTotal": "fill same structure or 'Not Applicable'"
            },
            "q1b_workerWellbeing": {
                "permanentMale": { "total": "", "healthInsurance": { "no": "", "percent": "" }, "accidentInsurance": { "no": "", "percent": "" }, "maternityBenefits": { "no": "", "percent": "" }, "paternityBenefits": { "no": "", "percent": "" }, "dayCare": { "no": "", "percent": "" } },
                "permanentFemale": { "total": "", "healthInsurance": { "no": "", "percent": "" }, "accidentInsurance": { "no": "", "percent": "" }, "maternityBenefits": { "no": "", "percent": "" }, "paternityBenefits": { "no": "", "percent": "" }, "dayCare": { "no": "", "percent": "" } },
                "permanentTotal": { "total": "", "healthInsurance": { "no": "", "percent": "" }, "accidentInsurance": { "no": "", "percent": "" }, "maternityBenefits": { "no": "", "percent": "" }, "paternityBenefits": { "no": "", "percent": "" }, "dayCare": { "no": "", "percent": "" } },
                "otherMale": { "total": "", "healthInsurance": { "no": "", "percent": "" }, "accidentInsurance": { "no": "", "percent": "" }, "maternityBenefits": { "no": "", "percent": "" }, "paternityBenefits": { "no": "", "percent": "" }, "dayCare": { "no": "", "percent": "" } },
                "otherFemale": { "total": "", "healthInsurance": { "no": "", "percent": "" }, "accidentInsurance": { "no": "", "percent": "" }, "maternityBenefits": { "no": "", "percent": "" }, "paternityBenefits": { "no": "", "percent": "" }, "dayCare": { "no": "", "percent": "" } },
                "otherTotal": { "total": "", "healthInsurance": { "no": "", "percent": "" }, "accidentInsurance": { "no": "", "percent": "" }, "maternityBenefits": { "no": "", "percent": "" }, "paternityBenefits": { "no": "", "percent": "" }, "dayCare": { "no": "", "percent": "" } }
            },
            "q1c_spendingOnWellbeing": {"currentFY": "percentage", "previousFY": "percentage"},
            "q2_retirementBenefits": {
                "pf": {
                    "currentFY": { "employeesPercent": "XX%", "workersPercent": "XX%", "deductedDeposited": "Y or N" },
                    "previousFY": { "employeesPercent": "XX%", "workersPercent": "XX%", "deductedDeposited": "Y or N" }
                },
                "gratuity": {
                    "currentFY": { "employeesPercent": "", "workersPercent": "", "deductedDeposited": "" },
                    "previousFY": { "employeesPercent": "", "workersPercent": "", "deductedDeposited": "" }
                },
                "esi": {
                    "currentFY": { "employeesPercent": "", "workersPercent": "", "deductedDeposited": "" },
                    "previousFY": { "employeesPercent": "", "workersPercent": "", "deductedDeposited": "" }
                },
                "nps": {
                    "currentFY": { "employeesPercent": "", "workersPercent": "- if not applicable", "deductedDeposited": "" },
                    "previousFY": { "employeesPercent": "", "workersPercent": "", "deductedDeposited": "" }
                }
            },
            "q3_accessibilityOfWorkplaces": "descriptive text about accessibility measures",
            "q4_equalOpportunityPolicy": {"exists": "Yes or No", "details": "policy description"},
            "q5_parentalLeaveRates": {
                "permanentEmployees": {
                    "male": { "returnToWorkRate": "XX% or Not Applicable", "retentionRate": "XX% or Not Applicable" },
                    "female": { "returnToWorkRate": "XX%", "retentionRate": "XX%" },
                    "total": { "returnToWorkRate": "XX%", "retentionRate": "XX%" }
                },
                "permanentWorkers": {
                    "male": { "returnToWorkRate": "", "retentionRate": "" },
                    "female": { "returnToWorkRate": "", "retentionRate": "" },
                    "total": { "returnToWorkRate": "", "retentionRate": "" }
                }
            },
            "q6_grievanceMechanism": {
                "permanentWorkers": "Yes or No",
                "otherThanPermanentWorkers": "Yes or No",
                "permanentEmployees": "Yes or No",
                "otherThanPermanentEmployees": "Yes or No",
                "details": "description of grievance mechanism"
            },
            "q7_membershipUnions": {
                "permanentEmployees": {
                    "currentFY": { "totalEmployees": "number or NIL", "membersInUnions": "number or NIL", "percentage": "XX% or NIL" },
                    "previousFY": { "totalEmployees": "", "membersInUnions": "", "percentage": "" }
                },
                "permanentWorkers": {
                    "currentFY": { "totalWorkers": "", "membersInUnions": "", "percentage": "" },
                    "previousFY": { "totalWorkers": "", "membersInUnions": "", "percentage": "" }
                }
            },
            "q8_trainingDetails": {
                "employees": {
                    "currentFY": {
                        "male": { "total": "number", "healthSafety": { "no": "number", "percent": "XX%" }, "skillUpgradation": { "no": "number", "percent": "XX%" } },
                        "female": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } },
                        "total": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } }
                    },
                    "previousFY": {
                        "male": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } },
                        "female": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } },
                        "total": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } }
                    }
                },
                "workers": {
                    "currentFY": {
                        "male": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } },
                        "female": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } },
                        "total": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } }
                    },
                    "previousFY": {
                        "male": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } },
                        "female": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } },
                        "total": { "total": "", "healthSafety": { "no": "", "percent": "" }, "skillUpgradation": { "no": "", "percent": "" } }
                    }
                }
            },
            "q9_performanceReviews": {
                "employees": {
                    "currentFY": {
                        "male": { "total": "number", "reviewed": "number", "percentage": "XX%" },
                        "female": { "total": "", "reviewed": "", "percentage": "" },
                        "total": { "total": "", "reviewed": "", "percentage": "" }
                    },
                    "previousFY": {
                        "male": { "total": "", "reviewed": "", "percentage": "" },
                        "female": { "total": "", "reviewed": "", "percentage": "" },
                        "total": { "total": "", "reviewed": "", "percentage": "" }
                    }
                },
                "workers": {
                    "currentFY": {
                        "male": { "total": "", "reviewed": "", "percentage": "" },
                        "female": { "total": "", "reviewed": "", "percentage": "" },
                        "total": { "total": "", "reviewed": "", "percentage": "" }
                    },
                    "previousFY": {
                        "male": { "total": "", "reviewed": "", "percentage": "" },
                        "female": { "total": "", "reviewed": "", "percentage": "" },
                        "total": { "total": "", "reviewed": "", "percentage": "" }
                    }
                }
            },
            "q10_healthSafetyManagement": {
                "a": "text about occupational health and safety management system",
                "b": "text about hazard identification and risk assessment process",
                "c": "text about worker reporting of hazards",
                "d": "Yes or No"
            },
            "q11_safetyIncidents": {
                "ltifr": {
                    "employees": { "currentYear": "number or 0", "previousYear": "number or 0" },
                    "workers": { "currentYear": "number", "previousYear": "number" }
                },
                "totalRecordableInjuries": {
                    "employees": { "currentYear": "number", "previousYear": "number" },
                    "workers": { "currentYear": "number", "previousYear": "number" }
                },
                "fatalities": {
                    "employees": { "currentYear": "number", "previousYear": "number" },
                    "workers": { "currentYear": "number", "previousYear": "number" }
                },
                "highConsequenceInjuries": {
                    "employees": { "currentYear": "number", "previousYear": "number" },
                    "workers": { "currentYear": "number", "previousYear": "number" }
                }
            },
            "q12_safetyMeasures": "detailed text about safety measures and protocols",
            "q13_complaintsWorkingConditions": {
                "workingConditions": {
                    "currentFY": { "filed": "number or -", "pendingResolution": "number or -", "remarks": "text or -" },
                    "previousFY": { "filed": "", "pendingResolution": "", "remarks": "" }
                },
                "healthSafety": {
                    "currentFY": { "filed": "", "pendingResolution": "", "remarks": "" },
                    "previousFY": { "filed": "", "pendingResolution": "", "remarks": "" }
                }
            },
            "q14_assessments": {
                "healthSafetyPractices": "percentage coverage or description",
                "workingConditions": "percentage coverage or description"
            },
            "q15_correctiveActions": "text about corrective actions taken"
        },
        "leadership": {
            "q1_lifeInsurance": "text about life insurance coverage",
            "q2_statutoryDuesValueChain": "text about statutory compliance",
            "q3_rehabilitation": {
                "employees": {
                    "currentFY": { "totalAffected": "number", "rehabilitated": "number" },
                    "previousFY": { "totalAffected": "number", "rehabilitated": "number" }
                },
                "workers": {
                    "currentFY": { "totalAffected": "number", "rehabilitated": "number" },
                    "previousFY": { "totalAffected": "number", "rehabilitated": "number" }
                }
            },
            "q4_transitionAssistance": "Yes or No",
            "q5_valueChainAssessment": {
                "healthSafetyPractices": "text about assessments",
                "workingConditions": "text about assessments"
            },
            "q6_correctiveActionsValueChain": "text or Not Applicable"
        }
    }
}

IMPORTANT NOTES:
- For numbers in "total", "no", "reviewed" fields: use actual numbers as strings (e.g., "3424" not 3424)
- For percentages: include % symbol (e.g., "24.01%")
- For not applicable: use "NA" or "Not Applicable" or "NIL" as appropriate
- For Yes/No fields: use exact "Yes" or "No" or "Y" or "N"
- Fill ALL nested structures completely - don't leave any as empty objects"""
        },
        {
            "id": "sectionC_p4_p5",
            "name": "Section C: Principles 4-5 (Stakeholders & Human Rights)",
            "delay_seconds": 10,
            "prompt": """Extract Section C Principles 4 and 5.

CRITICAL: Return ONLY valid JSON. No markdown, no explanations.

{
    "principle4": {
        "essential": {
            "q1_stakeholderIdentification": "",
            "q2_stakeholderEngagement": []
        },
        "leadership": {
            "q1_boardConsultation": "",
            "q2_stakeholderConsultationUsed": "",
            "q2_details": {"a": "", "b": "", "c": ""},
            "q3_vulnerableEngagement": []
        }
    },
    "principle5": {
        "essential": {
            "q1_humanRightsTraining": {},
            "q2_minimumWages": {"employees": {}, "workers": {}},
            "q3_medianRemuneration": {},
            "q3a_grossWagesFemales": {"currentFY": "", "previousFY": ""},
            "q4_focalPointHumanRights": "",
            "q5_grievanceMechanisms": "",
            "q6_complaints": {},
            "q7_poshComplaints": {},
            "q8_mechanismsPreventAdverseConsequences": "",
            "q9_humanRightsInContracts": "",
            "q10_assessments": {},
            "q11_correctiveActions": ""
        },
        "leadership": {
            "q1_businessProcessModified": "",
            "q2_humanRightsDueDiligence": "",
            "q3_accessibilityDifferentlyAbled": "",
            "q4_valueChainAssessment": {},
            "q5_correctiveActionsValueChain": ""
        }
    }
}"""
        },
        {
            "id": "sectionC_p6",
            "name": "Section C: Principle 6 (Environment)",
            "delay_seconds": 10,
            "prompt": """Extract Section C Principle 6 with complete environmental data.

CRITICAL: Return ONLY valid JSON. No markdown, no explanations.

{
    "principle6": {
        "essential": {
            "q1_energyConsumption": {"renewable": {}, "nonRenewable": {}, "totalEnergyConsumed": {}, "energyIntensityPerTurnover": {}, "energyIntensityPPP": {}, "energyIntensityPhysicalOutput": "", "externalAssessment": ""},
            "q2_patScheme": "",
            "q2_patFacilities": [],
            "q3_waterDetails": {"withdrawal": {}, "consumption": {}, "waterIntensityPerTurnover": {}, "waterIntensityPPP": {}, "waterIntensityPhysicalOutput": "", "externalAssessment": ""},
            "q4_waterDischarge": {},
            "q5_zeroLiquidDischarge": "",
            "q6_airEmissions": {},
            "q7_ghgEmissions": {},
            "q8_ghgReductionProjects": "",
            "q9_wasteManagement": {},
            "q10_wastePractices": "",
            "q11_ecologicallySensitiveAreas": "",
            "q11_ecologicallySensitiveDetails": "",
            "q12_environmentalImpactAssessments": "",
            "q13_environmentalCompliance": "",
            "q13_nonCompliances": ""
        },
        "leadership": {
            "q1_waterStressAreas": {},
            "q2_scope3Emissions": "",
            "q2_scope3EmissionsPerTurnover": "",
            "q2_scope3IntensityPhysicalOutput": "",
            "q2_externalAssessment": "",
            "q3_biodiversityImpact": "",
            "q4_resourceEfficiencyInitiatives": [],
            "q5_businessContinuityPlan": "",
            "q6_valueChainEnvironmentalImpact": "",
            "q7_valueChainPartnersAssessed": ""
        }
    }
}"""
        },
        {
            "id": "sectionC_p7_p8_p9",
            "name": "Section C: Principles 7-9",
            "delay_seconds": 10,
            "prompt": """Extract Section C Principles 7, 8, and 9.

CRITICAL: Return ONLY valid JSON. No markdown, no explanations.

{
    "principle7": {
        "essential": {
            "q1a_numberOfAffiliations": "",
            "q1b_affiliationsList": [],
            "q2_antiCompetitiveConduct": {},
            "q3_correctiveActions": ""
        },
        "leadership": {
            "q1_publicPolicyPositions": "",
            "q2_policyAdvocacyDetails": ""
        }
    },
    "principle8": {
        "essential": {
            "q1_socialImpactAssessments": {},
            "q2_rehabilitationResettlement": {},
            "q3_csrProjects": []
        },
        "leadership": {
            "q1_impactAssessmentMethodology": "",
            "q2_csrGrievances": {}
        }
    },
    "principle9": {
        "essential": {
            "q1_consumerComplaints": {},
            "q2_productRecalls": {},
            "q3_cyberSecurityBreaches": {},
            "q4_dataPrivacy": "",
            "q5_advertisingCompliance": {}
        },
        "leadership": {
            "q1_consumerEducation": "",
            "q2_customerSatisfaction": {}
        }
    }
}"""
        },
    ]


def extract_text_from_pdf(file_content: bytes) -> str:
    """Extract text from PDF using pdfplumber for better table extraction"""
    # Original code:
    # if not pdfplumber:
    #     raise HTTPException(status_code=500, detail="pdfplumber not installed")
    
    # text = ""
    # with pdfplumber.open(io.BytesIO(file_content)) as pdf:
    #     for page in pdf.pages:
    #         page_text = page.extract_text() or ""
    #         text += page_text + "\n"
            
    #         # Also extract tables
    #         tables = page.extract_tables()
    #         for table in tables:
    #             for row in table:
    #                 if row:
    #                     row_text = " | ".join([str(cell) if cell else "" for cell in row])
    #                     text += row_text + "\n"
    # return text
    
    # Updated as per updates section
    text = ""
    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text


def extract_text_from_excel(file_content: bytes) -> str:
    """Extract text from Excel file"""
    # Original code:
    # if not openpyxl:
    #     raise HTTPException(status_code=500, detail="openpyxl not installed")
    
    # workbook = openpyxl.load_workbook(io.BytesIO(file_content))
    # text = ""
    # for sheet in workbook.worksheets:
    #     text += f"\n--- Sheet: {sheet.title} ---\n"
    #     for row in sheet.iter_rows(values_only=True):
    #         row_text = " | ".join([str(cell) if cell else "" for cell in row])
    #         if row_text.strip():
    #             text += row_text + "\n"
    # return text
    
    # Updated as per updates section
    workbook = openpyxl.load_workbook(io.BytesIO(file_content))
    text = ""
    for sheet in workbook.worksheets:
        text += f"\n--- Sheet: {sheet.title} ---\n"
        for row in sheet.iter_rows(values_only=True):
            row_text = " | ".join([str(cell) if cell else "" for cell in row])
            if row_text.strip():
                text += row_text + "\n"
    return text


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
    prompt = f"""You are a BRSR (Business Responsibility and Sustainability Reporting) expert.
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
6. Keep responses concise - no long paragraphs in values
7. Start your response with {{ and end with }}"""
    
    for attempt in range(GEMINI_CONFIG["max_retries"]):
        try:
            print(f"[Chunk: {chunk['id']}] Attempt {attempt + 1}...")
            
            response = await asyncio.to_thread(
                model.generate_content,
                prompt,
                generation_config={
                    "temperature": 0.1,
                    # "max_output_tokens": 8192 # Original
                    "max_output_tokens": GEMINI_CONFIG["max_output_tokens"] # Updated
                }
            )
            
            response_text = response.text.strip()
            
            repaired_json = repair_json(response_text)
            
            try:
                result = json.loads(repaired_json)
            except json.JSONDecodeError as inner_e:
                # Try one more repair - extract just the first complete object
                match = re.search(r'\{[^{}]*\}', repaired_json)
                if match:
                    result = json.loads(match.group())
                else:
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
            if "429" in str(e) or "quota" in error_str or "rate" in error_str: # Updated error checking
                # Rate limit hit - wait longer
                delay = 30 * (attempt + 1)
                print(f"[Chunk: {chunk['id']}] Rate limit hit, waiting {delay}s...")
                await asyncio.sleep(delay)
            elif attempt < GEMINI_CONFIG["max_retries"] - 1:
                # delay = GEMINI_CONFIG["retry_delay_base"] ** (attempt + 1) # Original
                # Updated delay calculation
                delay = GEMINI_CONFIG["retry_delay_base"] * (attempt + 1)
                await asyncio.sleep(delay)
    
    print(f"[Chunk: {chunk['id']}] Failed after {GEMINI_CONFIG['max_retries']} attempts")
    return {}


def validate_extracted_data(data: Dict[str, Any], chunk_id: str):
    """Validate extracted data against BRSR field guidance and apply calculations"""
    calc = BRSRCalculations()
    
    # Validate Section A fields
    if chunk_id == "sectionA_basic":
        if "cin" in data:
            # is_valid, msg = validate_brsr_field("q1_cin", data["cin"]) # Original, uses a non-existent function
            # Original code intended to use BRSR_FIELD_GUIDANCE, but validate_brsr_field is not defined in the provided scripts.
            # Assuming this section is for basic validation and will be handled by the LLM prompt and JSON schema.
            # if not is_valid:
            #     print(f"[Validation] CIN validation: {msg}")
            pass # Placeholder as validation function is not provided.

    # Apply calculations for Section C Principle 3 (Safety)
    # if "principle3" in data: # Original structure for P3
    #     p3 = data.get("principle3", {}).get("essential", {})
    #     safety = p3.get("q11_safetyIncidents", {})
        
    #     # Validate LTIFR values are reasonable (typically 0-10)
    #     ltifr = safety.get("ltifr", {})
    #     for key, value in ltifr.items():
    #         try:
    #             if value and float(value.replace(",", "")) > 100:
    #                 print(f"[Validation] Warning: LTIFR {key}={value} seems high")
    #         except:
    #             pass
    
    # Updated validation for P3 safety
    if "principle3_safety" in data or (isinstance(data.get("principle3"), dict)): # Checking for new structure and original fallback
        p3 = data.get("principle3_safety") or data.get("principle3", {})
        # Original structure had q11_safetyIncidents with nested ltifr. New structure has ltifr_employees and ltifr_workers directly.
        # ltifr = safety.get("ltifr", {}) # Original
        ltifr_emp = p3.get("ltifr_employees")
        ltifr_wrk = p3.get("ltifr_workers")
        
        if ltifr_emp and ltifr_emp != "":
            try:
                ltifr_val = float(str(ltifr_emp).replace(",", ""))
                # Assuming LTIFR should be a non-negative number, typically small. 100 is a loose upper bound.
                if not (0 <= ltifr_val <= 100): # Adjusted upper bound for wider compatibility
                    print(f"[Validation] Warning: LTIFR (Employees) {ltifr_val} outside expected range 0-100")
            except:
                pass # Ignore if conversion fails
        
        if ltifr_wrk and ltifr_wrk != "":
            try:
                ltifr_val = float(str(ltifr_wrk).replace(",", ""))
                if not (0 <= ltifr_val <= 100): # Adjusted upper bound for wider compatibility
                    print(f"[Validation] Warning: LTIFR (Workers) {ltifr_val} outside expected range 0-100")
            except:
                pass # Ignore if conversion fails


    # Apply calculations for Section C Principle 6 (Environment)
    if "principle6" in data: # Original structure
        p6 = data.get("principle6", {}).get("essential", {})
        
        # Validate energy intensity
        energy = p6.get("q1_energyConsumption", {})
        if energy.get("totalEnergyConsumed", {}).get("currentFY"):
            total = energy["totalEnergyConsumed"]["currentFY"]
            intensity = energy.get("energyIntensity", {}).get("currentFY", "")
            if total and not intensity:
                print(f"[Validation] Energy intensity should be calculated from total: {total}")
    
    # Updated validation for P6 energy
    if "principle6_energy" in data: # Checking for new structure
        p6_energy = data.get("principle6_energy", {})
        total_consumed = p6_energy.get("totalEnergyConsumption", "")
        intensity = p6_energy.get("energyIntensityPerTurnover", "")
        if total_consumed and not intensity:
            print(f"[Validation] Energy Intensity per turnover should be calculated for total energy consumption: {total_consumed}")

def merge_extracted_data(all_chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge all extracted chunks into final BRSR data structure matching frontend demo format"""
    
    result = {
        "sectionA": {},
        "sectionB": {},
        "sectionC": {}
    }
    
    for chunk_id, data in all_chunks:
        if not data:
            print(f"[Merge] Skipping empty chunk: {chunk_id}")
            continue
            
        if chunk_id == "sectionA_complete":
            # Section A is now complete in one chunk
            result["sectionA"] = data
            print(f"[Merge] Merged Section A: {len(data)} fields")
        elif chunk_id == "sectionB_complete":
            # Section B is complete in one chunk
            result["sectionB"] = data
            print(f"[Merge] Merged Section B")
        elif chunk_id.startswith("sectionC_"):
            # Section C split into 6 chunks: p1_p2, p3, p4_p5, p6, p7_p8_p9
            result["sectionC"].update(data)
            print(f"[Merge] Merged {chunk_id}: {list(data.keys())}")
            
    print(f"[Merge] Final structure: A={bool(result['sectionA'])}, B={bool(result['sectionB'])}, C principles={len(result['sectionC'])}")
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

