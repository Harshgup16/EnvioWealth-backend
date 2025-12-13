"""
BRSR Field Guidance - Python version of lib/brsr-field-guidance.ts
Contains all field definitions, calculations, and validation logic
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import re

# BRSR Green color
BRSR_GREEN = "#007A3D"

# Field guidance matching lib/brsr-field-guidance.ts
BRSR_FIELD_GUIDANCE = {
    "sectionA": {
        "q1_cin": {
            "description": "Corporate Identity Number (CIN) of the Listed Entity",
            "source": "MCA Portal / Certificate of Incorporation",
            "format": "21-character alphanumeric code",
            "example": "L17111PB1973PLC003345",
            "validation": r"^[A-Z]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$"
        },
        "q2_entityName": {
            "description": "Name of the Listed Entity",
            "source": "Certificate of Incorporation"
        },
        "q3_incorporationYear": {
            "description": "Year of incorporation",
            "format": "YYYY",
            "validation": r"^\d{4}$"
        },
        "q9_financialYear": {
            "description": "Financial year for which reporting is being done",
            "format": "YYYY-YY",
            "validation": r"^\d{4}-\d{2}$"
        },
        "q20_employees": {
            "description": "Employee details as at end of Financial Year",
            "definition": "Employee = as defined in section 2(k) of Factories Act, 1948",
            "categories": ["Permanent", "Other than Permanent"],
            "breakup": ["Total (A)", "Male No. (B)", "% (B/A)", "Female No. (C)", "% (C/A)"],
            "calculation": "Male % = (Male No. / Total) × 100"
        },
        "q22_turnoverRate": {
            "description": "Turnover rate for permanent employees and workers",
            "formula": "Turnover Rate = (No. of persons who left during the year × 100) / Average no. of persons employed",
            "averageFormula": "Average = (Persons at beginning of year + Persons at end of year) / 2"
        }
    },
    "sectionC": {
        "principle3": {
            "q11_safetyIncidents": {
                "ltifr_formula": "LTIFR = (Lost time injuries × 1,000,000) / Total hours worked",
                "unit": "per million hours worked"
            }
        },
        "principle6": {
            "energyIntensity": {
                "formula": "Energy Intensity = Total Energy Consumed (GJ) / Revenue from operations",
                "unit": "GJ per crore rupee"
            },
            "waterIntensity": {
                "formula": "Water Intensity = Total Water Consumption (KL) / Revenue from operations",
                "unit": "KL per crore rupee"
            },
            "ghgIntensity": {
                "formula": "GHG Intensity = Total Scope 1+2 emissions (tCO2e) / Revenue from operations",
                "unit": "tCO2e per crore rupee"
            }
        }
    }
}


class BRSRCalculations:
    """Calculation helpers matching lib/brsr-field-guidance.ts"""
    
    @staticmethod
    def calculate_gender_percent(count: float, total: float) -> str:
        """Calculate gender percentage"""
        if total == 0:
            return "0.00%"
        return f"{(count / total * 100):.2f}%"
    
    @staticmethod
    def calculate_turnover_rate(persons_left: float, avg_persons: float) -> str:
        """Calculate employee/worker turnover rate"""
        if avg_persons == 0:
            return "0.00%"
        return f"{(persons_left / avg_persons * 100):.2f}%"
    
    @staticmethod
    def calculate_average(start_count: float, end_count: float) -> float:
        """Calculate average persons for turnover rate"""
        return (start_count + end_count) / 2
    
    @staticmethod
    def calculate_ltifr(lost_time_injuries: float, total_hours_worked: float) -> str:
        """Calculate Lost Time Injury Frequency Rate"""
        if total_hours_worked == 0:
            return "0.000"
        ltifr = (lost_time_injuries * 1_000_000) / total_hours_worked
        return f"{ltifr:.3f}"
    
    @staticmethod
    def calculate_energy_intensity(total_energy_gj: float, revenue_crore: float) -> str:
        """Calculate energy intensity (GJ per crore rupee)"""
        if revenue_crore == 0:
            return "0.00"
        return f"{(total_energy_gj / revenue_crore):.2f}"
    
    @staticmethod
    def calculate_water_intensity(total_water_kl: float, revenue_crore: float) -> str:
        """Calculate water intensity (KL per crore rupee)"""
        if revenue_crore == 0:
            return "0.00"
        return f"{(total_water_kl / revenue_crore):.2f}"
    
    @staticmethod
    def calculate_ghg_intensity(total_emissions_tco2e: float, revenue_crore: float) -> str:
        """Calculate GHG emission intensity (tCO2e per crore rupee)"""
        if revenue_crore == 0:
            return "0.00"
        return f"{(total_emissions_tco2e / revenue_crore):.2f}"
    
    @staticmethod
    def calculate_waste_intensity(total_waste_mt: float, revenue_crore: float) -> str:
        """Calculate waste intensity (MT per crore rupee)"""
        if revenue_crore == 0:
            return "0.00"
        return f"{(total_waste_mt / revenue_crore):.2f}"
    
    @staticmethod
    def calculate_return_to_work_rate(returned: float, due_to_return: float) -> str:
        """Calculate return to work rate after parental leave"""
        if due_to_return == 0:
            return "0.00%"
        return f"{(returned / due_to_return * 100):.2f}%"
    
    @staticmethod
    def calculate_retention_rate(retained_12_months: float, returned_prior: float) -> str:
        """Calculate retention rate 12 months after return from parental leave"""
        if returned_prior == 0:
            return "0.00%"
        return f"{(retained_12_months / returned_prior * 100):.2f}%"


def validate_brsr_field(field_key: str, value: Any) -> tuple[bool, str]:
    """Validate a BRSR field value against the guidance"""
    guidance = None
    
    # Find the field guidance
    for section in BRSR_FIELD_GUIDANCE.values():
        if field_key in section:
            guidance = section[field_key]
            break
        # Check nested sections
        if isinstance(section, dict):
            for subsection in section.values():
                if isinstance(subsection, dict) and field_key in subsection:
                    guidance = subsection[field_key]
                    break
    
    if not guidance:
        return True, "No validation defined"
    
    # Check regex validation if defined
    if "validation" in guidance and isinstance(value, str):
        if not re.match(guidance["validation"], value):
            return False, f"Invalid format. Expected: {guidance.get('format', 'unknown')}"
    
    return True, "Valid"


def get_field_description(field_key: str) -> str:
    """Get the description for a BRSR field"""
    for section in BRSR_FIELD_GUIDANCE.values():
        if field_key in section:
            return section[field_key].get("description", "")
        if isinstance(section, dict):
            for subsection in section.values():
                if isinstance(subsection, dict) and field_key in subsection:
                    return subsection[field_key].get("description", "")
    return ""


# BRSR Section C Full Questions - matching lib/brsr-questions.ts
BRSR_SECTION_C_QUESTIONS = {
    "principle1": {
        "title": "PRINCIPLE 1: Businesses should conduct and govern themselves with integrity, and in a manner that is Ethical, Transparent and Accountable",
        "essential": {
            "q1": "Percentage of employees and workers who have been provided training on the Principles during the financial year",
            "q2": "Details of fines / penalties /punishment/ award/ compounding fees/ settlement amount paid in proceedings (by the entity or by directors / KMPs) with regulators/ law enforcement agencies/ judicial institutions, in the financial year",
            "q3": "Of the instances disclosed in Question 2 above, details of the Appeal/ Revision preferred in cases where monetary or non-monetary action has been appealed",
            "q4": "Does the entity have an anti-corruption or anti-bribery policy? If yes, provide details in brief and if available, provide a web-link to the policy",
            "q5": "Number of Directors/KMPs/employees/workers against whom disciplinary action was taken by any law enforcement agency for the charges of bribery/ corruption",
            "q6": "Details of complaints with regard to conflict of interest",
            "q7": "Provide details of any corrective action taken or underway on issues related to fines / penalties / action taken by regulators/ law enforcement agencies/ judicial institutions, on cases of corruption and conflicts of interest",
            "q8": "Number of days of accounts payables ((Accounts payable *365) / Cost of goods/services procured) in the following format",
            "q9": "Open-ness of business - Provide details of concentration of purchases and sales with trading houses, dealers, and related parties along-with loans and advances & investments, with related parties"
        },
        "leadership": {
            "q1": "Awareness programmes conducted for value chain partners on any of the Principles during the financial year",
            "q2": "Does the entity have processes in place to avoid/ manage conflict of interest involving members of the Board? (Yes/No) If yes, provide details of the same"
        }
    },
    "principle2": {
        "title": "PRINCIPLE 2: Businesses should provide goods and services in a manner that is sustainable and safe",
        "essential": {
            "q1": "Percentage of R&D and capital expenditure (capex) investments in specific technologies to improve the environmental and social impacts of product and processes to total R&D and capex investments made by the entity, respectively",
            "q2": "a. Does the entity have procedures in place for sustainable sourcing? (Yes/No)\nb. If yes, what percentage of inputs were sourced sustainably?",
            "q3": "Describe the processes in place to safely reclaim your products for reusing, recycling and disposing at the end of life, for (a) Plastics (including packaging) (b) E-waste (c) Hazardous waste and (d) other waste",
            "q4": "Whether Extended Producer Responsibility (EPR) is applicable to the entity's activities (Yes / No). If yes, whether the waste collection plan is in line with the Extended Producer Responsibility (EPR) plan submitted to Pollution Control Boards? If not, provide steps taken to address the same"
        },
        "leadership": {
            "q1": "Has the entity conducted Life Cycle Perspective / Assessments (LCA) for any of its products (for manufacturing industry) or for its services (for service industry)? If yes, provide details",
            "q2": "If there are any significant social or environmental concerns and/or risks arising from production or disposal of your products / services, as identified in the Life Cycle Perspective / Assessments (LCA) or through any other means, briefly describe the same along-with action taken to mitigate the same",
            "q3": "Percentage of recycled or reused input material to total material (by value) used in production (for manufacturing industry) or providing services (for service industry)",
            "q4": "Of the products and packaging reclaimed at end of life of products, amount (in metric tonnes) reused, recycled, and safely disposed",
            "q5": "Reclaimed products and their packaging materials (as percentage of products sold) for each product category"
        }
    },
    "principle3": {
        "title": "PRINCIPLE 3: Businesses should respect and promote the well-being of all employees, including those in their value chains",
        "essential": {
            "q1a": "Details of measures for the well-being of employees",
            "q1b": "Details of measures for the well-being of workers",
            "q2": "Details of retirement benefits, for Current FY and Previous Financial Year",
            "q3": "Accessibility of workplaces - Are the premises / offices of the entity accessible to differently abled employees and workers, as per the requirements of the Rights of Persons with Disabilities Act, 2016? If not, whether any steps are being taken by the entity in this regard",
            "q4": "Does the entity have an equal opportunity policy as per the Rights of Persons with Disabilities Act, 2016? If so, provide a web-link to the policy",
            "q5": "Return to work and Retention rates of permanent employees and workers that took parental leave",
            "q6": "Is there a mechanism available to receive and redress grievances for the following categories of employees and worker? If yes, give details of the mechanism in brief",
            "q7": "Membership of employees and worker in association(s) or Unions recognised by the listed entity",
            "q8": "Details of training given to employees and workers",
            "q9": "Details of performance and career development reviews of employees and worker",
            "q10": "Health and safety management system: a) Whether an occupational health and safety management system has been implemented by the entity? (Yes/ No). If yes, the coverage such system? b) What are the processes used to identify work-related hazards and assess risks on a routine and non-routine basis by the entity? c) Whether you have processes for workers to report the work related hazards and to remove themselves from such risks. (Yes/No) d) Do the employees/ worker have access to non-occupational medical and healthcare services? (Yes/ No)",
            "q11": "Details of safety related incidents",
            "q12": "Describe the measures taken by the entity to ensure a safe and healthy work place",
            "q13": "Number of Complaints on the following made by employees and workers",
            "q14": "Assessments for the year - % of your plants and offices that were assessed (by entity or statutory authorities or third parties)",
            "q15": "Provide details of any corrective action taken or underway to address safety-related incidents (if any) and on significant risks / concerns arising from assessments of health & safety practices and working conditions"
        },
        "leadership": {
            "q1": "Does the entity extend any life insurance or any compensatory package in the event of death of (A) Employees (Y/N) (B) Workers (Y/N)",
            "q2": "Provide the measures undertaken by the entity to ensure that statutory dues have been deducted and deposited by the value chain partners",
            "q3": "Provide the number of employees / workers having suffered high consequence work-related injury / ill-health / fatalities (as reported in Q11 of Essential Indicators above), who have been rehabilitated and placed in suitable employment or whose family members have been placed in suitable employment",
            "q4": "Does the entity provide transition assistance programs to facilitate continued employability and the management of career endings resulting from retirement or termination of employment? (Yes/ No)",
            "q5": "Details on assessment of value chain partners",
            "q6": "Provide details of any corrective actions taken or underway to address significant risks / concerns arising from assessments of health and safety practices and working conditions of value chain partners"
        }
    },
    "principle4": {
        "title": "PRINCIPLE 4: Businesses should respect the interests of and be responsive to all its stakeholders",
        "essential": {
            "q1": "Describe the processes for identifying key stakeholder groups of the entity",
            "q2": "List stakeholder groups identified as key for your entity and the frequency of engagement with each stakeholder group"
        },
        "leadership": {
            "q1": "Provide the processes for consultation between stakeholders and the Board on economic, environmental, and social topics or if consultation is delegated, how is feedback from such consultations provided to the Board",
            "q2": "Whether stakeholder consultation is used to support the identification and management of environmental, and social topics (Yes / No). If so, provide details of instances as to how the inputs received from stakeholders on these topics were incorporated into policies and activities of the entity",
            "q3": "Provide details of instances of engagement with, and actions taken to, address the concerns of vulnerable/ marginalized stakeholder groups"
        }
    },
    "principle5": {
        "title": "PRINCIPLE 5: Businesses should respect and promote human rights",
        "essential": {
            "q1": "Employees and workers who have been provided training on human rights issues and policy(ies) of the entity",
            "q2": "Details of minimum wages paid to employees and workers",
            "q3": "Details of remuneration/salary/wages",
            "q4": "Do you have a focal point (Individual/ Committee) responsible for addressing human rights impacts or issues caused or contributed to by the business? (Yes/No)",
            "q5": "Describe the internal mechanisms in place to redress grievances related to human rights issues",
            "q6": "Number of Complaints on the following made by employees and workers (Sexual Harassment, Discrimination at workplace, Child Labour, Forced Labour/Involuntary Labour, Wages, Other human rights related issues)",
            "q7": "Complaints filed under the Sexual Harassment of Women at Workplace (Prevention, Prohibition and Redressal) Act, 2013",
            "q8": "Mechanisms to prevent adverse consequences to the complainant in discrimination and harassment cases",
            "q9": "Do human rights requirements form part of your business agreements and contracts? (Yes/No)",
            "q10": "Assessments for the year - % of your plants and offices that were assessed (by entity or statutory authorities or third parties)",
            "q11": "Provide details of any corrective actions taken or underway to address significant risks / concerns arising from the assessments at Question 10 above"
        },
        "leadership": {
            "q1": "Details of a business process being modified / introduced as a result of addressing human rights grievances/complaints",
            "q2": "Details of the scope and coverage of any Human rights due diligence conducted",
            "q3": "Is the premise/office of the entity accessible to differently abled visitors, as per the requirements of the Rights of Persons with Disabilities Act, 2016?",
            "q4": "Details on assessment of value chain partners",
            "q5": "Provide details of any corrective actions taken or underway to address significant risks / concerns arising from the assessments at Question 4 above"
        }
    },
    "principle6": {
        "title": "PRINCIPLE 6: Businesses should respect and make efforts to protect and restore the environment",
        "essential": {
            "q1": "Details of total energy consumption (in Joules or multiples) and energy intensity",
            "q2": "Does the entity have any sites / facilities identified as designated consumers (DCs) under the Performance, Achieve and Trade (PAT) Scheme of the Government of India? (Yes/No) If yes, disclose whether targets set under the PAT scheme have been achieved. In case targets have not been achieved, provide the remedial action taken, if any",
            "q3": "Provide details of the following disclosures related to water",
            "q4": "Provide the following details related to water discharged",
            "q5": "Has the entity implemented a mechanism for Zero Liquid Discharge? If yes, provide details of its coverage and implementation",
            "q6": "Please provide details of air emissions (other than GHG emissions) by the entity",
            "q7": "Provide details of greenhouse gas emissions (Scope 1 and Scope 2 emissions) & its intensity",
            "q8": "Does the entity have any project related to reducing Green House Gas emission? If Yes, then provide details",
            "q9": "Provide details related to waste management by the entity",
            "q10": "Briefly describe the waste management practices adopted in your establishments. Describe the strategy adopted by your company to reduce usage of hazardous and toxic chemicals in your products and processes and the practices adopted to manage such wastes",
            "q11": "If the entity has operations/offices in/around ecologically sensitive areas (such as national parks, wildlife sanctuaries, biosphere reserves, wetlands, biodiversity hotspots, forests, coastal regulation zones etc.) where environmental approvals / clearances are required, please specify details",
            "q12": "Details of environmental impact assessments of projects undertaken by the entity based on applicable laws, in the current financial year",
            "q13": "Is the entity compliant with the applicable environmental law/ regulations/ guidelines in India; such as the Water (Prevention and Control of Pollution) Act, Air (Prevention and Control of Pollution) Act, Environment protection act and rules thereunder (Y/N). If not, provide details of all such non-compliances"
        },
        "leadership": {
            "q1": "Water withdrawal, consumption and discharge in areas of water stress (in kilolitres)",
            "q2": "Please provide details of total Scope 3 emissions & its intensity",
            "q3": "With respect to the ecologically sensitive areas reported at Question 11 of Essential Indicators above, provide details of significant direct & indirect impact of the entity on biodiversity in such areas along-with prevention and remediation activities",
            "q4": "If the entity has undertaken any specific initiatives or used innovative technology or solutions to improve resource efficiency, or reduce impact due to emissions / effluent discharge / waste generated, please provide details of the same as well as outcome of such initiatives",
            "q5": "Does the entity have a business continuity and disaster management plan? Give details in 100 words/ web link",
            "q6": "Disclose any significant adverse impact to the environment, arising from the value chain of the entity. What mitigation or adaptation measures have been taken by the entity in this regard",
            "q7": "Percentage of value chain partners (by value of business done with such partners) that were assessed for environmental impacts"
        }
    },
    "principle7": {
        "title": "PRINCIPLE 7: Businesses, when engaging in influencing public and regulatory policy, should do so in a manner that is responsible and transparent",
        "essential": {
            "q1": "a. Number of affiliations with trade and industry chambers/ associations.\nb. List the top 10 trade and industry chambers/ associations (determined based on the total members of such body) the entity is a member of/ affiliated to",
            "q2": "Provide details of corrective action taken or underway on any issues related to anti-competitive conduct by the entity, based on adverse orders from regulatory authorities"
        },
        "leadership": {
            "q1": "Details of public policy positions advocated by the entity"
        }
    },
    "principle8": {
        "title": "PRINCIPLE 8: Businesses should promote inclusive growth and equitable development",
        "essential": {
            "q1": "Details of Social Impact Assessments (SIA) of projects undertaken by the entity based on applicable laws, in the current financial year",
            "q2": "Provide information on project(s) for which ongoing Rehabilitation and Resettlement (R&R) is being undertaken by your entity",
            "q3": "Describe the mechanisms to receive and redress grievances of the community",
            "q4": "Percentage of input material (inputs to total inputs by value) sourced from suppliers",
            "q5": "Job creation in smaller towns – Disclose wages paid to persons employed (including employees or workers employed on a permanent or non-permanent / on contract basis) in the following locations, as % of total wage cost"
        },
        "leadership": {
            "q1": "Provide details of actions taken to mitigate any negative social impacts identified in the Social Impact Assessments (Reference: Question 1 of Essential Indicators above)",
            "q2": "Provide the following information on CSR projects undertaken by your entity in designated aspirational districts as identified by government bodies",
            "q3": "a) Do you have a preferential procurement policy where you give preference to purchase from suppliers comprising marginalized /vulnerable groups? (Yes/No)\nb) From which marginalized /vulnerable groups do you procure?\nc) What percentage of total procurement (by value) does it constitute?",
            "q4": "Details of the benefits derived and shared from the intellectual properties owned or acquired by your entity (in the current financial year), based on traditional knowledge",
            "q5": "Details of corrective actions taken or underway, based on any adverse order in intellectual property related disputes wherein usage of traditional knowledge is involved",
            "q6": "Details of beneficiaries of CSR Projects"
        }
    },
    "principle9": {
        "title": "PRINCIPLE 9: Businesses should engage with and provide value to their consumers in a responsible manner",
        "essential": {
            "q1": "Describe the mechanisms in place to receive and respond to consumer complaints and feedback",
            "q2": "Turnover of products and/ services as a percentage of turnover from all products/service that carry information about",
            "q3": "Number of consumer complaints in respect of the following (Data privacy, Advertising, Cyber-security, Delivery of essential services, Restrictive Trade Practices, Unfair Trade Practices, Other)",
            "q4": "Details of instances of product recalls on account of safety issues",
            "q5": "Does the entity have a framework/ policy on cyber security and risks related to data privacy? (Yes/No) If available, provide a web-link of the policy",
            "q6": "Provide details of any corrective actions taken or underway on issues relating to advertising, and delivery of essential services; cyber security and data privacy of customers; re-occurrence of instances of product recalls; penalty / action taken by regulatory authorities on safety of products / services"
        },
        "leadership": {
            "q1": "Channels / platforms where information on products and services of the entity can be accessed (provide web link, if available)",
            "q2": "Steps taken to inform and educate consumers about safe and responsible usage of products and/or services",
            "q3": "Mechanisms in place to inform consumers of any risk of disruption/discontinuation of essential services",
            "q4": "Does the entity display product information on the product over and above what is mandated as per local laws? (Yes/No/Not Applicable) If yes, provide details in brief. Did your entity carry out any survey with regard to consumer satisfaction relating to the major products / services of the entity, significant locations of operation of the entity or the entity as a whole? (Yes/No)",
            "q5": "Provide the following information relating to data breaches"
        }
    }
}


# Table column headers for Section C tables
SECTION_C_TABLE_HEADERS = {
    "training_coverage": ["Category", "Total (A)", "Trained on Principles (B)", "% (B/A)"],
    "fines_penalties_monetary": ["Type", "NGRBC Principle", "Name of regulatory/enforcement agency", "Amount (INR)", "Brief of case", "Has appeal preferred (Y/N)"],
    "fines_penalties_non_monetary": ["Type", "NGRBC Principle", "Name of regulatory/enforcement agency", "Brief of case", "Has appeal preferred (Y/N)"],
    "disciplinary_action": ["Category", "FY Current", "FY Previous"],
    "conflict_of_interest": ["Category", "Filed (Current FY)", "Pending (Current FY)", "Filed (Previous FY)", "Pending (Previous FY)"],
    "value_chain_awareness": ["Total Partners", "Partners Trained", "Topics Covered", "% Trained"],
    "rd_capex": ["Category", "FY Current", "FY Previous", "Details of improvements"],
    "employee_wellbeing": ["Category", "% Employees Covered", "% Workers Covered"],
    "retirement_benefits": ["Benefit", "Employees (%)", "Workers (%)"],
    "parental_leave": ["Gender", "Return to work rate", "Retention rate"],
    "safety_incidents": ["Category", "Employees (Current)", "Employees (Previous)", "Workers (Current)", "Workers (Previous)"],
    "safety_incidents_rows": ["Lost Time Injury Frequency Rate (LTIFR)", "Total recordable work-related injuries", "No. of fatalities", "High consequence work-related injury or ill-health"],
    "training_health_safety": ["Category", "Total (Employees)", "Trained (Employees)", "% (Employees)", "Total (Workers)", "Trained (Workers)", "% (Workers)"],
    "human_rights_training": ["Category", "Total (A)", "Trained (B)", "% (B/A)"],
    "minimum_wages": ["Category", "Equal to Min Wage", "More than Min Wage", "Total"],
    "energy_consumption": ["Parameter", "FY Current", "FY Previous"],
    "water_details": ["Parameter", "FY Current", "FY Previous"],
    "air_emissions": ["Pollutant", "Unit", "FY Current", "FY Previous"],
    "ghg_emissions": ["Parameter", "Unit", "FY Current", "FY Previous"],
    "waste_management": ["Category", "FY Current (MT)", "FY Previous (MT)"],
    "stakeholder_engagement": ["Stakeholder Group", "Engagement Methods", "Frequency", "Purpose"],
    "consumer_complaints": ["Category", "Received (Current)", "Pending (Current)", "Received (Previous)", "Pending (Previous)", "Remarks"],
}
