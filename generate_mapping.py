"""
Helper script to generate mapping structure for data.py
This script reads the demoData structure and generates flat extraction keys
"""

import json

# This script helps generate the mapping between frontend nested structure 
# and backend flat extraction keys

def generate_flat_key(path_parts, prefix=""):
    """Generate flat key from path parts"""
    if not prefix:
        prefix = path_parts[0].lower()
        path_parts = path_parts[1:]
    
    key = prefix
    for part in path_parts:
        key += "_" + part
    
    return key

def process_dict(obj, path_parts, prefix, result, is_array_item=False):
    """Recursively process dictionary to generate mappings"""
    for key, value in obj.items():
        current_path = path_parts + [key]
        
        if isinstance(value, dict):
            # It's a nested dict, recurse
            process_dict(value, current_path, prefix, result, is_array_item)
        elif isinstance(value, list):
            # It's an array, mark it
            flat_key = generate_flat_key(current_path, prefix) + "_array"
            dotted_path = ".".join(current_path)
            result.append(f'            "{dotted_path}": "{flat_key}"')
        else:
            # It's a leaf node
            flat_key = generate_flat_key(current_path, prefix)
            dotted_path = ".".join(current_path)
            result.append(f'            "{dotted_path}": "{flat_key}"')

# Example usage for Principle 1 Essential section
# You can modify this to generate for other sections

def generate_section_c_principle1():
    """Generate mapping for Principle 1"""
    # This is a simplified example - adapt as needed
    essential_structure = {
        "q1_percentageCoveredByTraining": {
            "boardOfDirectors": {"totalProgrammes": "", "topicsCovered": "", "percentageCovered": ""},
            "kmp": {"totalProgrammes": "", "topicsCovered": "", "percentageCovered": ""},
            "employees": {"totalProgrammes": "", "topicsCovered": "", "percentageCovered": ""},
            "workers": {"totalProgrammes": "", "topicsCovered": "", "percentageCovered": ""}
        }
    }
    
    result = []
    process_dict(essential_structure, ["principle1", "essential"], "sectionc", result)
    
    print("Mappings:")
    for mapping in result:
        print(mapping)

if __name__ == "__main__":
    print("This script helps generate flat key mappings.")
    print("Modify the structure dictionary and run to generate mappings.\n")
    
    # Example for generating a few keys
    print("\n=== Example mappings ===")
    
    # Principle 1 example keys
    p1_keys = [
        ("principle1.essential.q1_percentageCoveredByTraining.boardOfDirectors.totalProgrammes", 
         "sectionc_principle1_essential_q1_percentagecoveredbytraining_boardofdirectors_totalprogrammes"),
        ("principle1.essential.q2_finesPenalties.monetary", 
         "sectionc_principle1_essential_q2_finespenalties_monetary_array"),
        ("principle1.essential.q3_appealsOutstanding", 
         "sectionc_principle1_essential_q3_appealsoutstanding"),
    ]
    
    for nested, flat in p1_keys:
        print(f'"{nested}": "{flat}"')
    
    print("\n=== Pattern ===")
    print("For simple fields: sectionc_principle{N}_{level}_{question}_{subfield}")
    print("For arrays: sectionc_principle{N}_{level}_{question}_{subfield}_array")
    print("For nested objects: sectionc_principle{N}_{level}_{question}_{parent}_{child}")
