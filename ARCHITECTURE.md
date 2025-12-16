# BRSR Backend Architecture - Flat to Nested Transformation

## Overview
The BRSR backend uses a **3-layer transformation architecture** to convert PDF/Excel documents into structured JSON that the frontend can display.

## Architecture Layers

```
[PDF/Excel Document]
        ↓
[agents.py: Prompt Generation]
    - Tells Gemini to extract FLAT keys
    - Example: "sectiona_cin", "sectiona_employees_permanent_male"
        ↓
[Gemini API: Extraction]
    - Extracts flat data: {"sectiona_cin": "L23201...", "sectiona_employees_permanent_male": "3424"}
        ↓
[transform.py: Flat → Nested]
    - Converts flat keys to nested structure
    - {"sectiona_cin": "L23201..."} → {"sectionA": {"cin": "L23201..."}}
        ↓
[fastapi_brsr_backend.py: Merge & Serve]
    - Merges all chunks
    - Returns complete nested JSON to frontend
        ↓
[Frontend: Display]
    - Receives: {"sectionA": {...}, "sectionB": {...}, "sectionC": {...}}
    - Displays BRSR report
```

## Key Files

### 1. agents.py
- Contains prompt-generating functions for each extraction chunk
- Prompts tell Gemini to extract data with FLAT keys
- Example output format: `{"sectiona_cin": "L23201DL1959GOI003948"}`

### 2. data.py (Reference/Documentation)
- Maps flat extraction keys to nested frontend structure
- Used for reference/documentation
- Shows mapping pattern: `"cin": "sectiona_cin"`
- Note: Not actively used in transformation (transform.py handles this programmatically)

### 3. transform.py (Core Transformation Logic)
- **Primary transformation module**
- Functions:
  - `flat_to_nested_path(flat_key)`: Converts flat key to nested path
    - Example: `"sectiona_employees_permanent_male"` → `["sectionA", "employees", "permanent", "male"]`
  - `transform_flat_to_nested(flat_data)`: Transforms entire flat dict to nested dict
  - `merge_nested_data(base, updates)`: Deep merges nested dictionaries
- Handles camelCase conversion (policymatrix → policyMatrix)
- Handles array markers (_array suffix)

### 4. fastapi_brsr_backend.py (API Server)
- FastAPI server that orchestrates the entire process
- Key functions:
  - `extract_chunk_with_gemini()`: Calls Gemini with retry logic
  - `merge_extracted_data()`: Uses transform.py to convert flat → nested
  - `extract_brsr_data()`: Main endpoint that handles file upload and extraction

## Extraction Flow

### Step 1: Chunked Extraction
BRSR report is split into 8 chunks for Gemini:
1. **Section A** (sectionA_complete) - Company info
2. **Section B** (sectionB_complete) - Policies
3-8. **Section C** - 6 principle chunks:
   - sectionC_p1_p2 (Principles 1-2)
   - sectionC_p3 (Principle 3 - most complex)
   - sectionC_p4_p5 (Principles 4-5)
   - sectionC_p6 (Principle 6 - environmental)
   - sectionC_p7_p8_p9 (Principles 7-9)

### Step 2: Flat Key Extraction
Gemini returns flat keys per chunk:
```json
{
  "sectiona_cin": "L23201DL1959GOI003948",
  "sectiona_entityName": "Indian Oil Corporation Limited",
  "sectiona_employees_permanent_male": "3424",
  "sectiona_businessActivities_array": [...]
}
```

### Step 3: Transformation
`transform_flat_to_nested()` converts to nested:
```json
{
  "sectionA": {
    "cin": "L23201DL1959GOI003948",
    "entityName": "Indian Oil Corporation Limited",
    "employees": {
      "permanent": {
        "male": "3424"
      }
    },
    "businessActivities": [...]
  }
}
```

### Step 4: Merging
All 8 chunks are transformed and deep-merged into final structure:
```json
{
  "sectionA": {...},
  "sectionB": {...},
  "sectionC": {
    "principle1": {...},
    "principle2": {...},
    ...
    "principle9": {...}
  }
}
```

## Flat Key Naming Convention

### Pattern:
`{section}_{path}_{parts}_{separated}_{by}_{underscore}`

### Rules:
1. **Section prefix**: `sectiona_`, `sectionb_`, `sectionc_`
2. **Path parts**: All lowercase, underscore-separated
3. **Arrays**: Suffix with `_array`
4. **Nested objects**: Each level separated by `_`

### Examples:
| Flat Key | Nested Path | Notes |
|----------|-------------|-------|
| `sectiona_cin` | `sectionA.cin` | Simple field |
| `sectiona_employees_permanent_male` | `sectionA.employees.permanent.male` | Deep nesting |
| `sectiona_businessActivities_array` | `sectionA.businessActivities` | Array field |
| `sectionb_policymatrix_p1_hasPolicy` | `sectionB.policyMatrix.p1.hasPolicy` | camelCase applied |
| `sectionc_principle1_essential_q3` | `sectionC.principle1.essential.q3` | Section C principle |

## CamelCase Handling

The `transform.py` module maintains a `camel_case_map` dictionary that converts lowercase field names to proper camelCase:

```python
camel_case_map = {
    "policymatrix": "policyMatrix",
    "entityname": "entityName",
    "businessactivities": "businessActivities",
    "currentfy": "currentFY",
    "previousfy": "previousFY",
    # ... more mappings
}
```

This ensures frontend compatibility while keeping Gemini extraction simple (all lowercase).

## Why This Architecture?

### Advantages:
1. **Simple for Gemini**: Flat keys are easier for LLM to extract reliably
2. **Flexible**: Python handles complex nesting (easier than teaching LLM nested JSON)
3. **Maintainable**: Clear separation of concerns
4. **Debuggable**: Each layer can be tested independently
5. **Scalable**: Adding new fields only requires updating prompts (transform handles it automatically)

### Trade-offs:
- Extra transformation step (minimal performance impact)
- Need to maintain camelCase mapping dictionary
- Two sources of truth (prompts define flat keys, transform defines structure)

## Testing

### Test transform.py directly:
```bash
cd backend
python transform.py
```

This will output sample transformations and verify the mapping logic.

### Test full extraction:
1. Start backend: `uvicorn fastapi_brsr_backend:app --reload --port 8000`
2. Upload a BRSR PDF/Excel via frontend
3. Check backend logs for transformation details
4. Verify frontend receives correct nested structure

## Current Status

✅ **Completed**:
- Section A mapping (18 basic + complex nested fields)
- Section B mapping (policyMatrix p1-p9 + governance)
- Transform.py with programmatic flat-to-nested conversion
- FastAPI integration with transform.py
- CamelCase handling

⏳ **Pending**:
- Update agents.py prompts to explicitly output flat keys
- Test end-to-end extraction with real BRSR documents
- Verify all 400+ Section C fields transform correctly
- Add error handling for unknown flat keys

## Next Steps

1. **Update agents.py prompts**:
   - Modify prompts to explicitly instruct Gemini to return flat keys
   - Example: "Extract data as FLAT JSON with keys like: sectiona_cin, sectiona_entityName, ..."

2. **Test transformation**:
   - Run extraction on sample BRSR documents
   - Verify output matches frontend demoData structure
   - Debug any missing camelCase mappings

3. **Extend camelCase map**:
   - Add more field name mappings as discovered during testing
   - Consider auto-generating from frontend demoData structure

4. **Error handling**:
   - Add validation for unknown flat keys
   - Log warnings for unmapped fields
   - Return partial data if some chunks fail

## Contact & Maintenance

This architecture was designed to separate concerns between LLM extraction (simple flat) and structural transformation (complex nested). When adding new BRSR fields:

1. Update prompts in `agents.py` with new flat keys
2. Add camelCase mapping to `transform.py` if needed
3. Test transformation
4. No changes needed to `data.py` (reference only)

The system will automatically handle nesting based on flat key patterns.
