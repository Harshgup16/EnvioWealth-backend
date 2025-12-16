"""
Test script to verify flat key transformation after agents.py update
"""

import json
from transform import transform_flat_to_nested, flat_to_nested_path

# Sample flat data matching the NEW agent prompts
sample_flat_data = {
    # Section A (already correct)
    "sectiona_cin": "L23201DL1959GOI003948",
    "sectiona_entityName": "Indian Oil Corporation Limited",
    "sectiona_employees_permanent_male": "3424",
    
    # Section B (already correct)
    "sectionb_policymatrix_p1_hasPolicy": "Yes",
    "sectionb_governance_directorStatement": "We are committed to sustainability...",
    
    # Section C - Principle 1 (NEW flat format)
    "sectionc_principle1_essential_q1_percentagecoveredbytraining_boardofdirectors_totalprogrammes": "12",
    "sectionc_principle1_essential_q1_percentagecoveredbytraining_boardofdirectors_topicscovered": "Ethics, Compliance",
    "sectionc_principle1_essential_q3_appealsoutstanding": "None",
    "sectionc_principle1_essential_q4_anticorruptionpolicy_exists": "Yes",
    "sectionc_principle1_essential_q4_anticorruptionpolicy_weblink": "https://example.com/policy",
    
    # Principle 2
    "sectionc_principle2_essential_q1_rdcapexinvestments_rd_currentfy": "150000000",
    "sectionc_principle2_essential_q2_sustainablesourcing_proceduresinplace": "Yes, comprehensive procedures",
    
    # Principle 3
    "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_total": "3424",
    "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_healthinsurance_no": "3424",
    "sectionc_principle3_essential_q1c_spendingonwellbeing_currentfy": "0.016",
    
    # Principle 5
    "sectionc_principle5_essential_q1_humanrightstraining_employees_permanent_total": "3845",
    "sectionc_principle5_essential_q3a_grosswagesfemales_currentfy": "42.5",
    
    # Principle 6
    "sectionc_principle6_essential_q1_energyconsumption_renewable_electricity_currentfy": "1250.5",
    "sectionc_principle6_essential_q7_ghgemissions_scope1_currentfy": "850000",
    
    # Principle 7
    "sectionc_principle7_essential_q1a_numberofaffiliations": "15",
    "sectionc_principle7_essential_q2_anticompetitiveconduct": "None",
    
    # Principle 9
    "sectionc_principle9_essential_q1_consumercomplaintmechanism": "Email and phone helpline",
    "sectionc_principle9_essential_q4_productrecalls_voluntary_number": "0",
}

print("=" * 80)
print("TESTING FLAT KEY TRANSFORMATION")
print("=" * 80)

print("\n1. Sample Flat Keys → Nested Path Conversion:")
print("-" * 80)
test_keys = [
    "sectiona_cin",
    "sectionb_policymatrix_p1_hasPolicy",
    "sectionc_principle1_essential_q3_appealsoutstanding",
    "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_healthinsurance_no",
    "sectionc_principle6_essential_q1_energyconsumption_renewable_electricity_currentfy",
]

for key in test_keys:
    path = flat_to_nested_path(key)
    print(f"{key}")
    print(f"  → {'.'.join(path)}")
    print()

print("\n2. Full Transformation Test:")
print("-" * 80)
print("Input (flat keys):")
print(json.dumps(sample_flat_data, indent=2))

print("\n" + "=" * 80)
print("Output (nested structure):")
print("=" * 80)
nested_result = transform_flat_to_nested(sample_flat_data)
print(json.dumps(nested_result, indent=2))

print("\n" + "=" * 80)
print("VALIDATION:")
print("=" * 80)

# Validate key sections exist
checks = [
    ("sectionA exists", "sectionA" in nested_result),
    ("sectionB exists", "sectionB" in nested_result),
    ("sectionC exists", "sectionC" in nested_result),
    ("sectionC.principle1 exists", "principle1" in nested_result.get("sectionC", {})),
    ("sectionC.principle2 exists", "principle2" in nested_result.get("sectionC", {})),
    ("sectionC.principle3 exists", "principle3" in nested_result.get("sectionC", {})),
    ("Principle 1 has essential", "essential" in nested_result.get("sectionC", {}).get("principle1", {})),
    ("Principle 1 q3 exists", "q3" in nested_result.get("sectionC", {}).get("principle1", {}).get("essential", {})),
]

for check_name, result in checks:
    status = "✅ PASS" if result else "❌ FAIL"
    print(f"{status}: {check_name}")

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
