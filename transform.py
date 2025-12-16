"""
transform.py - Transform Gemini's flat extraction output to frontend's nested structure

This module handles the transformation of Gemini's flat JSON output (with keys like 
"sectiona_cin", "sectionc_principle1_essential_q1") into the nested structure that 
the frontend expects (like {"sectionA": {"cin": "..."}}).
"""

from typing import Dict, Any, List
import json


def set_nested_value(obj: Dict[str, Any], path: List[str], value: Any) -> None:
    """
    Set a value in a nested dictionary using a path list.
    
    Example:
        set_nested_value(obj, ["sectionA", "cin"], "L23201...")
        Results in: obj["sectionA"]["cin"] = "L23201..."
    """
    current = obj
    for key in path[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    current[path[-1]] = value


def flat_to_nested_path(flat_key: str) -> List[str]:
    """
    Convert flat extraction key to nested path with proper camelCase.
    
    Examples:
        "sectiona_cin" → ["sectionA", "cin"]
        "sectiona_employees_permanent_male" → ["sectionA", "employees", "permanent", "male"]
        "sectionb_policymatrix_p1_hasPolicy" → ["sectionB", "policyMatrix", "p1", "hasPolicy"]
        "sectionc_principle1_essential_q1" → ["sectionC", "principle1", "essential", "q1"]
    """
    # Mapping for known camelCase fields
    camel_case_map = {
        # Section A
        "policymatrix": "policyMatrix",
        "entityname": "entityName",
        "yearofincorporation": "yearOfIncorporation",
        "registeredaddress": "registeredAddress",
        "corporateaddress": "corporateAddress",
        "financialyear": "financialYear",
        "stockexchanges": "stockExchanges",
        "paidupcapital": "paidUpCapital",
        "contactperson": "contactPerson",
        "reportingboundary": "reportingBoundary",
        "businessactivities": "businessActivities",
        "niccode": "nicCode",
        "turnoverpercent": "turnoverPercent",
        "boardofdirectors": "boardOfDirectors",
        "totalprogrammes": "totalProgrammes",
        "topicscovered": "topicsCovered",
        "percentagecovered": "percentageCovered",
        "approvedbyboard": "approvedByBoard",
        "turnoverrate": "turnoverRate",
        "healthinsurance": "healthInsurance",
        "accidentinsurance": "accidentInsurance",
        "maternitybenefit": "maternityBenefits",
        "paternitybenefit": "paternityBenefits",
        "daycare": "dayCare",
        "currentfy": "currentFY",
        "previousfy": "previousFY",
        
        # Section C - Principle fields
        "percentagecoveredbytraining": "percentageCoveredByTraining",
        "finespenalties": "finesPenalties",
        "appealsoutstanding": "appealsOutstanding",
        "anticorruptionpolicy": "antiCorruptionPolicy",
        "weblink": "webLink",
        "disciplinaryactions": "disciplinaryActions",
        "conflictofinterestprocess": "conflictOfInterestProcess",
        "conflictofinterestcomplaints": "conflictOfInterestComplaints",
        "correctiveactions": "correctiveActions",
        "accountspayabledays": "accountsPayableDays",
        "opennessbusiness": "opennessBusiness",
        "concentrationpurchases": "concentrationPurchases",
        "tradinghousespercent": "tradingHousesPercent",
        "dealerscount": "dealersCount",
        "top10tradinghouses": "top10TradingHouses",
        "concentrationsales": "concentrationSales",
        "dealersdistributorspercent": "dealersDistributorsPercent",
        "top10dealers": "top10Dealers",
        "sharerpts": "shareRPTs",
        "loansadvances": "loansAdvances",
        "valuechainawareness": "valueChainAwareness",
        "rdcapexinvestments": "rdCapexInvestments",
        "improvementdetails": "improvementDetails",
        "sustainablesourcing": "sustainableSourcing",
        "proceduresinplace": "proceduresInPlace",
        "percentagesustainablysourced": "percentageSustainablySourced",
        "reclaimprocesses": "reclaimProcesses",
        "wastecollectionplaninline": "wasteCollectionPlanInLine",
        "lcadetails": "lcaDetails",
        "recycledinputmaterial": "recycledInputMaterial",
        "inputmaterial": "inputMaterial",
        "employeewellbeing": "employeeWellbeing",
        "workerwellbeing": "workerWellbeing",
        "permanentmale": "permanentMale",
        "permanentfemale": "permanentFemale",
        "permanenttotal": "permanentTotal",
        "othermale": "otherMale",
        "otherfemale": "otherFemale",
        "othertotal": "otherTotal",
        "spendingonwellbeing": "spendingOnWellbeing",
        "retirementbenefits": "retirementBenefits",
        "employeespercent": "employeesPercent",
        "workerspercent": "workersPercent",
        "deducteddeposited": "deductedDeposited",
        "accessibilityofworkplaces": "accessibilityOfWorkplaces",
        "equalopportunitypolicy": "equalOpportunityPolicy",
        "parentalleaverates": "parentalLeaveRates",
        "permanentemployees": "permanentEmployees",
        "permanentworkers": "permanentWorkers",
        "returntoworkrate": "returnToWorkRate",
        "retentionrate": "retentionRate",
        "grievancemechanism": "grievanceMechanism",
        "otherthanpermanentworkers": "otherThanPermanentWorkers",
        "otherthanpermanentemployees": "otherThanPermanentEmployees",
        "otherthanpermanent": "otherThanPermanent",
        "valuechain": "valueChain",
        "membershipunions": "membershipUnions",
        "totalemployees": "totalEmployees",
        "membersinunions": "membersInUnions",
        "totalworkers": "totalWorkers",
        "trainingdetails": "trainingDetails",
        "healthsafety": "healthSafety",
        "skillupgradation": "skillUpgradation",
        "performancereviews": "performanceReviews",
        "healthsafetymanagement": "healthSafetyManagement",
        "safetyincidents": "safetyIncidents",
        "currentyear": "currentYear",
        "previousyear": "previousYear",
        "safetymeasures": "safetyMeasures",
        "lifeinsurance": "lifeInsurance",
        "humanrightstraining": "humanRightsTraining",
        "minimumwages": "minimumWages",
        "medianremuneration": "medianRemuneration",
        "grosswagesfemales": "grossWagesFemales",
        "focalpointhumanrights": "focalPointHumanRights",
        "grievancemechanisms": "grievanceMechanisms",
        "sexualharassment": "sexualHarassment",
        "poshcomplaints": "poshComplaints",
        "totalcomplaints": "totalComplaints",
        "mechanismspreventadverseconsequences": "mechanismsPreventAdverseConsequences",
        "humanrightsincontracts": "humanRightsInContracts",
        "childlabour": "childLabour",
        "businessprocessmodified": "businessProcessModified",
        "energyconsumption": "energyConsumption",
        "nonrenewable": "nonRenewable",
        "totalenergyconsumed": "totalEnergyConsumed",
        "energyintensityperturnover": "energyIntensityPerTurnover",
        "patscheme": "patScheme",
        "patfacilities": "patFacilities",
        "waterdetails": "waterDetails",
        "surfacewater": "surfaceWater",
        "groundwater": "groundWater",
        "waterdischarge": "waterDischarge",
        "notreatment": "noTreatment",
        "withtreatment": "withTreatment",
        "zeroliquiddischarge": "zeroLiquidDischarge",
        "airemissions": "airEmissions",
        "ghgemissions": "ghgEmissions",
        "ghgreductionprojects": "ghgReductionProjects",
        "wastemanagement": "wasteManagement",
        "plasticwaste": "plasticWaste",
        "wastepractices": "wastePractices",
        "ecologicallysensitiveareas": "ecologicallySensitiveAreas",
        "scope3emissions": "scope3Emissions",
        # Principle 5 fields (Human Rights)
        "remuneration": "remuneration",
        "minimumwages": "minimumWages",
        "equaltominwage": "equalToMinWage",
        "morethanminwage": "moreThanMinWage",
        "medianremuneration": "medianRemuneration",
        "keymanagerialpersonnel": "keyManagerialPersonnel",
        "employeesotherthanbodandkmp": "employeesOtherThanBoDAndKMP",
        "grosswagesfemales": "grossWagesFemales",
        "focalpointhumanrights": "focalPointHumanRights",
        "grievancemechanisms": "grievanceMechanisms",
        "sexualharassment": "sexualHarassment",
        "discriminationatworkplace": "discriminationAtWorkplace",
        "childlabour": "childLabour",
        "forcedlabour": "forcedLabour",
        "forcedinvoluntarylabour": "forcedInvoluntaryLabour",
        "forcedlabourinvoluntarylabour": "forcedLabourInvoluntaryLabour",
        "otherhumanrights": "otherHumanRights",
        "poshcomplaints": "poshComplaints",
        "complaintsaspercentfemale": "complaintsAsPercentFemale",
        "complaintsupheld": "complaintsUpheld",
        "mechanismspreventadverseconsequences": "mechanismsPreventAdverseConsequences",
        "humanrightsincontracts": "humanRightsInContracts",
        "businessprocessmodified": "businessProcessModified",
        "humanrightsduediligence": "humanRightsDueDiligence",
        "accessibilitydifferentlyabled": "accessibilityDifferentlyAbled",
        "correctiveactionsvaluechain": "correctiveActionsValueChain",
        # Principle 6 fields (Environment)
        "energyconsumption": "energyConsumption",
        "renewable": "renewable",
        "nonrenewable": "nonRenewable",
        "othersources": "otherSources",
        "totalenergyconsumed": "totalEnergyConsumed",
        "energyintensityperturnover": "energyIntensityPerTurnover",
        "energyintensityppp": "energyIntensityPPP",
        "energyintensityphysicaloutput": "energyIntensityPhysicalOutput",
        "externalassessment": "externalAssessment",
        "patscheme": "patScheme",
        "patfacilities": "patFacilities",
        "waterdetails": "waterDetails",
        "withdrawal": "withdrawal",
        "surfacewater": "surfaceWater",
        "groundwater": "groundwater",
        "thirdpartywater": "thirdPartyWater",
        "seawaterdesalinated": "seawaterDesalinated",
        "consumption": "consumption",
        "waterintensityperturnover": "waterIntensityPerTurnover",
        "waterintensityppp": "waterIntensityPPP",
        "waterintensityphysicaloutput": "waterIntensityPhysicalOutput",
        "waterdischarge": "waterDischarge",
        "notreatment": "noTreatment",
        "withtreatment": "withTreatment",
        "thirdparties": "thirdParties",
        "totalwaterdischarged": "totalWaterDischarged",
        "zeroliquiddischarge": "zeroLiquidDischarge",
        "airemissions": "airEmissions",
        "ghgemissions": "ghgEmissions",
        "scope1and2intensityperturnover": "scope1And2IntensityPerTurnover",
        "scope1and2intensityphysicaloutput": "scope1And2IntensityPhysicalOutput",
        "totalscope1and2": "totalScope1And2",
        "ghgreductionprojects": "ghgReductionProjects",
        "wastemanagement": "wasteManagement",
        "ewaste": "eWaste",
        "biomedicalwaste": "bioMedicalWaste",
        "constructiondemolitionwaste": "constructionDemolitionWaste",
        "batterywaste": "batteryWaste",
        "radioactivewaste": "radioactiveWaste",
        "otherhazardouswaste": "otherHazardousWaste",
        "othernonhazardouswaste": "otherNonHazardousWaste",
        "totalwaste": "totalWaste",
        "wasteintensityperturnover": "wasteIntensityPerTurnover",
        "wasteintensityppp": "wasteIntensityPPP",
        "wasteintensityphysicaloutput": "wasteIntensityPhysicalOutput",
        "recycled": "recycled",
        "reused": "reused",
        "otherrecovery": "otherRecovery",
        "totalrecovered": "totalRecovered",
        "incineration": "incineration",
        "landfilling": "landfilling",
        "otherdisposal": "otherDisposal",
        "totaldisposed": "totalDisposed",
        "ecologicallysensitivedetails": "ecologicallySensitiveDetails",
        "environmentalimpactassessments": "environmentalImpactAssessments",
        "environmentalcompliance": "environmentalCompliance",
        "noncompliances": "nonCompliances",
        "waterstressareas": "waterStressAreas",
        "natureofoperations": "natureOfOperations",
        "scope3emissionsperturnover": "scope3EmissionsPerTurnover",
        "scope3intensityphysicaloutput": "scope3IntensityPhysicalOutput",
        "biodiversityimpact": "biodiversityImpact",
        "resourceefficiencyinitiatives": "resourceEfficiencyInitiatives",
        "businesscontinuityplan": "businessContinuityPlan",
        "valuechainenvironmentalimpact": "valueChainEnvironmentalImpact",
        "valuechainpartnersassessed": "valueChainPartnersAssessed",
        # Principle 7 fields (Public Policy)
        "numberofaffiliations": "numberOfAffiliations",
        "affiliationslist": "affiliationsList",
        "anticompetitiveconduct": "antiCompetitiveConduct",
        "publicpolicyadvocacy": "publicPolicyAdvocacy",
        "policyadvocated": "policyAdvocated",
        "methodresorted": "methodResorted",
        "publicdomain": "publicDomain",
        "frequencyofreview": "frequencyOfReview",
        # Principle 8 fields (Inclusive Growth)
        "socialimpactassessments": "socialImpactAssessments",
        "rehabilitationresettlement": "rehabilitationResettlement",
        "communitygrievancemechanism": "communityGrievanceMechanism",
        "inputmaterialsourcing": "inputMaterialSourcing",
        "neighboringdistricts": "neighboringDistricts",
        "jobcreation": "jobCreation",
        "semiurban": "semiUrban",
        "metropolitan": "metropolitan",
        "negativeimpactmitigation": "negativeImpactMitigation",
        "csrprojects": "csrProjects",
        "aspirationaldistrict": "aspirationalDistrict",
        "amountspent": "amountSpent",
        "preferentialprocurement": "preferentialProcurement",
        "vulnerablegroups": "vulnerableGroups",
        "procurementpercentage": "procurementPercentage",
        "intellectualproperty": "intellectualProperty",
        "ipdisputes": "ipDisputes",
        "csrbeneficiaries": "csrBeneficiaries",
        "percentvulnerable": "percentVulnerable",
        # Principle 9 fields (Consumer Value)
        "consumercomplaintmechanism": "consumerComplaintMechanism",
        "productinformationpercentage": "productInformationPercentage",
        "environmentalparameters": "environmentalParameters",
        "safeusage": "safeUsage",
        "consumercomplaints": "consumerComplaints",
        "dataprivacy": "dataPrivacy",
        "cybersecurity": "cyberSecurity",
        "deliveryofessentialservices": "deliveryOfEssentialServices",
        "restrictivetradepractices": "restrictiveTradePractices",
        "unfairtradepractices": "unfairTradePractices",
        "productrecalls": "productRecalls",
        "voluntary": "voluntary",
        "cybersecuritypolicy": "cyberSecurityPolicy",
        "databreaches": "dataBreaches",
        "numberofinstances": "numberOfInstances",
        "impactonbusiness": "impactOnBusiness",
        "turnoversafety": "turnoverSafety",
        "environmentallysustainableproducts": "environmentallySustainableProducts",
        "saferecyclableproducts": "safeRecyclableProducts",
        "informationchannels": "informationChannels",
        "consumersurveys": "consumerSurveys",
        "trendsinsatisfaction": "trendsInSatisfaction",
        "areasofimpact": "areasOfImpact",
        # Other principles
        "numberofaffiliations": "numberOfAffiliations",
        "affiliationslist": "affiliationsList",
        "anticompetitiveconduct": "antiCompetitiveConduct",
        "publicpolicyadvocacy": "publicPolicyAdvocacy",
        "policyadvocated": "policyAdvocated",
        "methodresorted": "methodResorted",
        "publicdomain": "publicDomain",
        "frequencyofreview": "frequencyOfReview",
        "socialimpactassessments": "socialImpactAssessments",
        "rehabilitationresettlement": "rehabilitationResettlement",
        "communitygrievancemechanism": "communityGrievanceMechanism",
        "inputmaterialsourcing": "inputMaterialSourcing",
        "withindistrict": "withinDistrict",
        "jobcreation": "jobCreation",
        "semiurban": "semiUrban",
        "csrprojects": "csrProjects",
        "aspirationaldistrict": "aspirationalDistrict",
        "amountspent": "amountSpent",
        "consumercomplaintmechanism": "consumerComplaintMechanism",
        "productinformationpercentage": "productInformationPercentage",
        "environmentalparameters": "environmentalParameters",
        "safeusage": "safeUsage",
        "consumercomplaints": "consumerComplaints",
        "dataprivacy": "dataPrivacy",
        "cybersecurity": "cyberSecurity",
        "productrecalls": "productRecalls",
        "cybersecuritypolicy": "cyberSecurityPolicy",
        "databreaches": "dataBreaches",
        "numberofinstances": "numberOfInstances",
        "informationchannels": "informationChannels",
        "stakeholderidentification": "stakeholderIdentification",
        "stakeholderengagement": "stakeholderEngagement",
        "stakeholdergroup": "stakeholderGroup",
        "vulnerablemarginalized": "vulnerableMarginalized",
        "boardconsultation": "boardConsultation",
        "stakeholderconsultationused": "stakeholderConsultationUsed",
        # Principle 3 fields
        "employeewellbeing": "employeeWellbeing",
        "workerwellbeing": "workerWellbeing",
        "spendingonwellbeing": "spendingOnWellbeing",
        "retirementbenefits": "retirementBenefits",
        "accessibilityofworkplaces": "accessibilityOfWorkplaces",
        "equalopportunitypolicy": "equalOpportunityPolicy",
        "parentalleaverates": "parentalLeaveRates",
        "returntoworkrate": "returnToWorkRate",
        "retentionrate": "retentionRate",
        "grievancemechanism": "grievanceMechanism",
        "membershipunions": "membershipUnions",
        "membersinunions": "membersInUnions",
        "trainingdetails": "trainingDetails",
        "healthsafety": "healthSafety",
        "skillupgradation": "skillUpgradation",
        "performancereviews": "performanceReviews",
        "reviewed": "reviewed",
        "healthsafetymanagement": "healthSafetyManagement",
        "safetyincidents": "safetyIncidents",
        "totalrecordableinjuries": "totalRecordableInjuries",
        "highconsequenceinjuries": "highConsequenceInjuries",
        "safetymeasures": "safetyMeasures",
        "complaintsworkingconditions": "complaintsWorkingConditions",
        "workingconditions": "workingConditions",
        "pendingresolution": "pendingResolution",
        "correctiveactions": "correctiveActions",
        "lifeinsurance": "lifeInsurance",
        "statutoryduesvaluechain": "statutoryDuesValueChain",
        "rehabilitation": "rehabilitation",
        "totalaffected": "totalAffected",
        "rehabilitated": "rehabilitated",
        "transitionassistance": "transitionAssistance",
        "valuechainassessment": "valueChainAssessment",
        "healthsafetypractices": "healthSafetyPractices",
        "correctiveactionsvaluechain": "correctiveActionsValueChain",
        # Principle 4 fields
        "vulnerableengagement": "vulnerableEngagement",
        "vulnerablegroup": "vulnerableGroup",
        "actiontaken": "actionTaken",
    }
    
    parts = flat_key.split("_")
    
    if not parts:
        return []
    
    # Handle section prefix (sectiona → sectionA, sectionb → sectionB, etc.)
    section = parts[0]
    if section.startswith("section"):
        section_letter = section[-1].upper()
        nested_path = [f"section{section_letter}"]
        remaining = parts[1:]
    else:
        return []
    
    # Process remaining parts
    i = 0
    while i < len(remaining):
        part = remaining[i]
        
        # Skip array markers
        if part == "array":
            i += 1
            continue
        
        # Special handling for Section C: Detect qN_fieldName pattern
        # If current part matches qN (q1, q2, etc.) and there's a next part
        if part.startswith("q") and len(part) <= 3 and part[1:].isdigit() and i + 1 < len(remaining):
            # Peek at next part to see if it's a field name (not a category like boardofdirectors)
            next_part = remaining[i + 1]
            # Check if next_part is likely a field name by checking if it's in camel_case_map
            # or if it's a known Section C field pattern
            field_name_candidates = [
                # Principle 1 & 2
                "percentagecoveredbytraining", "finespenalties", "appealsoutstanding", 
                "anticorruptionpolicy", "disciplinaryactions", "conflictofinterest",
                "awarenessinitiatives", "accountspayabledays", "opennessbusiness",
                "inputmaterialsourcing", "wasteintensity", "emissions", "biodiversity",
                "waterusage", "energyconsumption", "operationalimpact", "rdcapexinvestments",
                "sustainablesourcing", "reclaimprocesses", "epr", "lcadetails", "significantconcerns",
                "recycledinputmaterial", "productsreclaimed", "reclaimedpercentage",
                # Principle 3 & 4
                "employeewellbeing", "workerwellbeing", "spendingonwellbeing", "retirementbenefits",
                "accessibilityofworkplaces", "equalopportunitypolicy", "parentalleaverates",
                "grievancemechanism", "membershipunions", "trainingdetails", "performancereviews",
                "healthsafetymanagement", "safetyincidents", "safetymeasures", "complaintsworkingconditions",
                "assessments", "correctiveactions", "lifeinsurance", "statutoryduesvaluechain",
                "rehabilitation", "transitionassistance", "valuechainassessment",
                "stakeholderidentification", "stakeholderengagement", "boardconsultation",
                "stakeholderconsultationused", "vulnerableengagement",
                # Principle 5 & 6
                "remuneration", "minimumwages", "medianremuneration", "grosswagesfemales",
                "focalpointhumanrights", "grievancemechanisms", "complaints", "poshcomplaints",
                "mechanismspreventadverseconsequences", "humanrightsincontracts",
                "businessprocessmodified", "humanrightsduediligence", "accessibilitydifferentlyabled",
                "energyconsumption", "patscheme", "patfacilities", "waterdetails",
                "waterdischarge", "zeroliquiddischarge", "airemissions", "ghgemissions",
                "ghgreductionprojects", "wastemanagement", "wastepractices",
                "ecologicallysensitiveareas", "ecologicallysensitivedetails",
                "environmentalimpactassessments", "environmentalcompliance", "noncompliances",
                "waterstressareas", "scope3emissions", "biodiversityimpact",
                "resourceefficiencyinitiatives", "businesscontinuityplan",
                "valuechainenvironmentalimpact", "valuechainpartnersassessed",
                # Principle 7, 8, 9
                "numberofaffiliations", "affiliationslist", "anticompetitiveconduct",
                "publicpolicyadvocacy", "socialimpactassessments", "rehabilitationresettlement",
                "communitygrievancemechanism", "inputmaterialsourcing", "jobcreation",
                "negativeimpactmitigation", "csrprojects", "preferentialprocurement",
                "vulnerablegroups", "procurementpercentage", "intellectualproperty",
                "ipdisputes", "csrbeneficiaries", "consumercomplaintmechanism",
                "productinformationpercentage", "consumercomplaints", "productrecalls",
                "cybersecuritypolicy", "databreaches", "turnoversafety",
                "informationchannels", "consumersurveys",
                "healthsafetymanagement", "safetyincidents", "safetymeasures", "complaintsworkingconditions",
                "assessments", "correctiveactions", "lifeinsurance", "statutoryduesvaluechain",
                "rehabilitation", "transitionassistance", "valuechainassessment", "correctiveactionsvaluechain",
                "stakeholderidentification", "stakeholderengagement", "boardconsultation",
                "stakeholderconsultationused", "vulnerableengagement"
            ]
            
            if next_part.lower() in field_name_candidates or next_part in camel_case_map:
                # Combine qN + fieldName as a single compound key
                compound_key = part + "_" + next_part
                # Apply camelCase to the field part only
                if next_part.lower() in camel_case_map:
                    compound_key = part + "_" + camel_case_map[next_part.lower()]
                else:
                    # Convert field name to camelCase manually
                    field_camel = ''.join(word.capitalize() for word in next_part.split('_'))
                    field_camel = field_camel[0].lower() + field_camel[1:] if field_camel else field_camel
                    compound_key = part + "_" + field_camel
                
                nested_path.append(compound_key)
                i += 2  # Skip both qN and fieldName parts
                continue
        
        # Apply camelCase mapping if exists
        lower_part = part.lower()
        if lower_part in camel_case_map:
            nested_path.append(camel_case_map[lower_part])
        else:
            # Keep as-is (for things like p1, q1, principle1, essential, leadership, etc.)
            nested_path.append(part)
        
        i += 1
    
    return nested_path


def transform_flat_to_nested(flat_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Gemini's flat extraction output to nested frontend structure.
    
    Args:
        flat_data: Dictionary with flat keys like {"sectiona_cin": "L23201...", ...}
    
    Returns:
        Nested dictionary like {"sectionA": {"cin": "L23201..."}, ...}
    
    Example:
        Input:  {"sectiona_cin": "L23201...", "sectiona_employees_permanent_male": "3424"}
        Output: {"sectionA": {"cin": "L23201...", "employees": {"permanent": {"male": "3424"}}}}
    """
    nested_data = {}
    
    for flat_key, value in flat_data.items():
        # Skip None or empty values if desired (optional)
        if value is None or value == "":
            continue
        
        # Convert flat key to nested path
        path = flat_to_nested_path(flat_key)
        
        if not path:
            print(f"Warning: Could not parse flat key: {flat_key}")
            continue
        
        # Handle arrays: keys ending in _array should have their values as arrays
        if flat_key.endswith("_array"):
            # Ensure value is a list
            if not isinstance(value, list):
                value = [value] if value else []
        
        # Set the value in nested structure
        set_nested_value(nested_data, path, value)
    
    return nested_data


def merge_nested_data(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge updates into base dictionary.
    
    Args:
        base: Base dictionary (can be empty {})
        updates: Updates to merge in
    
    Returns:
        Merged dictionary
    """
    result = base.copy()
    
    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_nested_data(result[key], value)
        else:
            result[key] = value
    
    return result


# Example usage and testing
if __name__ == "__main__":
    # Test with sample flat data
    sample_flat_data = {
        "sectiona_cin": "L23201DL1959GOI003948",
        "sectiona_entityName": "Indian Oil Corporation Limited",
        "sectiona_employees_permanent_male": "3424",
        "sectiona_employees_permanent_female": "421",
        "sectiona_businessActivities_array": [
            {"description": "Refining of crude oil", "nicCode": "19201", "revenue": "85%"},
            {"description": "Marketing of petroleum products", "nicCode": "46710", "revenue": "15%"}
        ],
        "sectionb_policymatrix_p1_hasPolicy": "Yes",
        "sectionb_policymatrix_p1_approvedByBoard": "Yes",
        "sectionc_principle1_essential_q3_appealsOutstanding": "None",
    }
    
    print("=== Sample Flat Data ===")
    print(json.dumps(sample_flat_data, indent=2))
    
    print("\n=== Transformed Nested Data ===")
    nested = transform_flat_to_nested(sample_flat_data)
    print(json.dumps(nested, indent=2))
    
    print("\n=== Path Conversion Examples ===")
    test_keys = [
        "sectiona_cin",
        "sectiona_employees_permanent_male",
        "sectionb_policymatrix_p1_hasPolicy",
        "sectionc_principle1_essential_q3_appealsOutstanding",
        "sectiona_businessActivities_array"
    ]
    
    for key in test_keys:
        path = flat_to_nested_path(key)
        print(f"{key:50} → {'.'.join(path)}")
