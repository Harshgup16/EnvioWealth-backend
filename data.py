"""
data.py - BRSR Report Data Mappings
Maps Gemini's flat extraction keys to frontend's nested demoData structure

ARCHITECTURE:
1. agents.py: Prompts tell Gemini to extract FLAT keys (e.g., "sectiona_cin")
2. data.py (THIS FILE): Maps flat keys → nested structure
3. fastapi_brsr_backend.py: Transforms Gemini's flat JSON → nested JSON using this mapping

MAPPING PATTERN:
- Frontend: "sectionA.cin"  →  Gemini: "sectiona_cin"
- Frontend: "sectionA.employees.permanent.male"  →  Gemini: "sectiona_employees_permanent_male"
- Frontend: "sectionA.businessActivities" (array)  →  Gemini: "sectiona_businessActivities_array"
- Frontend: "sectionC.principle1.essential.q3"  →  Gemini: "sectionc_principle1_essential_q3"

WHY: Gemini extracts simple flat keys (easier for LLM) + Python nests (easier for code)

STATUS: ✅ Section A+B mapped | ⏳ Section C principles (400+ fields) - use pattern transform
"""

from typing import Dict, List, Any, Optional

# Complete BRSR JSON Skeleton with mapping from flat extraction keys to nested structure
BRSR_DATA_SKELETON: Dict[str, Any] = {
    "sectionA": {
        "cin": "sectiona_cin",
        "entityName": "sectiona_entityName",
        "yearOfIncorporation": "sectiona_yearOfIncorporation",
        "registeredAddress": "sectiona_registeredAddress",
        "corporateAddress": "sectiona_corporateAddress",
        "email": "sectiona_email",
        "telephone": "sectiona_telephone",
        "website": "sectiona_website",
        "financialYear": "sectiona_financialYear",
        "stockExchanges": "sectiona_stockExchanges",
        "paidUpCapital": "sectiona_paidUpCapital",
        "contactName": "sectiona_contactName",
        "contactDesignation": "sectiona_contactDesignation",
        "contactPhone": "sectiona_contactPhone",
        "contactEmail": "sectiona_contactEmail",
        "reportingBoundary": "sectiona_reportingBoundary",
        "assuranceProvider": "sectiona_assuranceProvider",
        "assuranceType": "sectiona_assuranceType",

        "businessActivities": "sectiona_businessActivities_array",
        "products": "sectiona_products_array",

        "nationalPlants": "sectiona_nationalPlants",
        "nationalOffices": "sectiona_nationalOffices",
        "internationalPlants": "sectiona_internationalPlants",
        "internationalOffices": "sectiona_internationalOffices",
        "nationalStates": "sectiona_nationalStates",
        "internationalCountries": "sectiona_internationalCountries",
        "exportContribution": "sectiona_exportContribution",

        "employees": {
            "permanent": {
                "male": "sectiona_employees_permanent_male",
                "female": "sectiona_employees_permanent_female",
                "total": "sectiona_employees_permanent_total"
            },
            "otherThanPermanent": {
                "male": "sectiona_employees_otherthanpermanent_male",
                "female": "sectiona_employees_otherthanpermanent_female",
                "total": "sectiona_employees_otherthanpermanent_total"
            }
        },

        "workers": {
            "permanent": {
                "male": "sectiona_workers_permanent_male",
                "female": "sectiona_workers_permanent_female",
                "total": "sectiona_workers_permanent_total"
            },
            "otherThanPermanent": {
                "male": "sectiona_workers_otherthanpermanent_male",
                "female": "sectiona_workers_otherthanpermanent_female",
                "total": "sectiona_workers_otherthanpermanent_total"
            }
        },

        "board": {
            "total": "sectiona_board_total",
            "female": "sectiona_board_female",
            "femalePercent": "sectiona_board_femalePercent"
        },
        
        "kmp": {
            "total": "sectiona_kmp_total",
            "female": "sectiona_kmp_female",
            "femalePercent": "sectiona_kmp_femalePercent"
        },

        "turnover": {
            "employees": {
                "male": "sectiona_turnover_employees_male",
                "female": "sectiona_turnover_employees_female",
                "total": "sectiona_turnover_employees_total"
            },
            "workers": {
                "male": "sectiona_turnover_workers_male",
                "female": "sectiona_turnover_workers_female",
                "total": "sectiona_turnover_workers_total"
            }
        },

        "subsidiaries": "sectiona_subsidiaries",

        "csr": {
            "prescribedAmount": "sectiona_csr_prescribedAmount",
            "amountSpent": "sectiona_csr_amountSpent",
            "surplus": "sectiona_csr_surplus"
        },

        "complaints": {
            "communities": {
                "filed": "sectiona_complaints_communities_filed",
                "pending": "sectiona_complaints_communities_pending",
                "remarks": "sectiona_complaints_communities_remarks"
            },
            "investors": {
                "filed": "sectiona_complaints_investors_filed",
                "pending": "sectiona_complaints_investors_pending",
                "remarks": "sectiona_complaints_investors_remarks"
            },
            "shareholders": {
                "filed": "sectiona_complaints_shareholders_filed",
                "pending": "sectiona_complaints_shareholders_pending",
                "remarks": "sectiona_complaints_shareholders_remarks"
            },
            "employees": {
                "filed": "sectiona_complaints_employees_filed",
                "pending": "sectiona_complaints_employees_pending",
                "remarks": "sectiona_complaints_employees_remarks"
            },
            "customers": {
                "filed": "sectiona_complaints_customers_filed",
                "pending": "sectiona_complaints_customers_pending",
                "remarks": "sectiona_complaints_customers_remarks"
            },
            "valueChain": {
                "filed": "sectiona_complaints_valuechain_filed",
                "pending": "sectiona_complaints_valuechain_pending",
                "remarks": "sectiona_complaints_valuechain_remarks"
            }
        },

        "materialIssues": "sectiona_materialIssues_array"
    },

    "sectionB": {
        "policyMatrix": {
            "p1": {
                "hasPolicy": "sectionb_policymatrix_p1_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p1_approvedByBoard",
                "webLink": "sectionb_policymatrix_p1_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p1_translatedToProcedures"
            },
            "p2": {
                "hasPolicy": "sectionb_policymatrix_p2_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p2_approvedByBoard",
                "webLink": "sectionb_policymatrix_p2_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p2_translatedToProcedures"
            },
            "p3": {
                "hasPolicy": "sectionb_policymatrix_p3_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p3_approvedByBoard",
                "webLink": "sectionb_policymatrix_p3_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p3_translatedToProcedures"
            },
            "p4": {
                "hasPolicy": "sectionb_policymatrix_p4_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p4_approvedByBoard",
                "webLink": "sectionb_policymatrix_p4_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p4_translatedToProcedures"
            },
            "p5": {
                "hasPolicy": "sectionb_policymatrix_p5_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p5_approvedByBoard",
                "webLink": "sectionb_policymatrix_p5_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p5_translatedToProcedures"
            },
            "p6": {
                "hasPolicy": "sectionb_policymatrix_p6_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p6_approvedByBoard",
                "webLink": "sectionb_policymatrix_p6_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p6_translatedToProcedures"
            },
            "p7": {
                "hasPolicy": "sectionb_policymatrix_p7_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p7_approvedByBoard",
                "webLink": "sectionb_policymatrix_p7_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p7_translatedToProcedures"
            },
            "p8": {
                "hasPolicy": "sectionb_policymatrix_p8_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p8_approvedByBoard",
                "webLink": "sectionb_policymatrix_p8_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p8_translatedToProcedures"
            },
            "p9": {
                "hasPolicy": "sectionb_policymatrix_p9_hasPolicy",
                "approvedByBoard": "sectionb_policymatrix_p9_approvedByBoard",
                "webLink": "sectionb_policymatrix_p9_webLink",
                "translatedToProcedures": "sectionb_policymatrix_p9_translatedToProcedures"
            }
        },
        
        "policyWebLink": "sectionb_policyWebLink",
        "valueChainExtension": "sectionb_valueChainExtension",
        "certifications": "sectionb_certifications",
        "commitments": "sectionb_commitments",
        "performance": "sectionb_performance",
        "directorStatement": "sectionb_directorStatement",
        
        "highestAuthority": {
            "name": "sectionb_highestAuthority_name",
            "designation": "sectionb_highestAuthority_designation",
            "din": "sectionb_highestAuthority_din",
            "email": "sectionb_highestAuthority_email",
            "phone": "sectionb_highestAuthority_phone"
        },
        
        "sustainabilityCommittee": "sectionb_sustainabilityCommittee",
        
        "review": {
            "performance": {
                "p1": "sectionb_review_performance_p1",
                "p2": "sectionb_review_performance_p2",
                "p3": "sectionb_review_performance_p3",
                "p4": "sectionb_review_performance_p4",
                "p5": "sectionb_review_performance_p5",
                "p6": "sectionb_review_performance_p6",
                "p7": "sectionb_review_performance_p7",
                "p8": "sectionb_review_performance_p8",
                "p9": "sectionb_review_performance_p9"
            },
            "performanceFrequency": "sectionb_review_performanceFrequency",
            "compliance": "sectionb_review_compliance"
        },
        
        "independentAssessment": {
            "p1": "sectionb_independentAssessment_p1",
            "p2": "sectionb_independentAssessment_p2",
            "p3": "sectionb_independentAssessment_p3",
            "p4": "sectionb_independentAssessment_p4",
            "p5": "sectionb_independentAssessment_p5",
            "p6": "sectionb_independentAssessment_p6",
            "p7": "sectionb_independentAssessment_p7",
            "p8": "sectionb_independentAssessment_p8",
            "p9": "sectionb_independentAssessment_p9"
        },
        
        "noPolicyReasons": {
            "notMaterial": {
                "p1": "sectionb_noPolicyReasons_notMaterial_p1",
                "p2": "sectionb_noPolicyReasons_notMaterial_p2",
                "p3": "sectionb_noPolicyReasons_notMaterial_p3",
                "p4": "sectionb_noPolicyReasons_notMaterial_p4",
                "p5": "sectionb_noPolicyReasons_notMaterial_p5",
                "p6": "sectionb_noPolicyReasons_notMaterial_p6",
                "p7": "sectionb_noPolicyReasons_notMaterial_p7",
                "p8": "sectionb_noPolicyReasons_notMaterial_p8",
                "p9": "sectionb_noPolicyReasons_notMaterial_p9"
            },
            "notReady": {
                "p1": "sectionb_noPolicyReasons_notReady_p1",
                "p2": "sectionb_noPolicyReasons_notReady_p2",
                "p3": "sectionb_noPolicyReasons_notReady_p3",
                "p4": "sectionb_noPolicyReasons_notReady_p4",
                "p5": "sectionb_noPolicyReasons_notReady_p5",
                "p6": "sectionb_noPolicyReasons_notReady_p6",
                "p7": "sectionb_noPolicyReasons_notReady_p7",
                "p8": "sectionb_noPolicyReasons_notReady_p8",
                "p9": "sectionb_noPolicyReasons_notReady_p9"
            },
            "noResources": {
                "p1": "sectionb_noPolicyReasons_noResources_p1",
                "p2": "sectionb_noPolicyReasons_noResources_p2",
                "p3": "sectionb_noPolicyReasons_noResources_p3",
                "p4": "sectionb_noPolicyReasons_noResources_p4",
                "p5": "sectionb_noPolicyReasons_noResources_p5",
                "p6": "sectionb_noPolicyReasons_noResources_p6",
                "p7": "sectionb_noPolicyReasons_noResources_p7",
                "p8": "sectionb_noPolicyReasons_noResources_p8",
                "p9": "sectionb_noPolicyReasons_noResources_p9"
            },
            "plannedNextYear": {
                "p1": "sectionb_noPolicyReasons_plannedNextYear_p1",
                "p2": "sectionb_noPolicyReasons_plannedNextYear_p2",
                "p3": "sectionb_noPolicyReasons_plannedNextYear_p3",
                "p4": "sectionb_noPolicyReasons_plannedNextYear_p4",
                "p5": "sectionb_noPolicyReasons_plannedNextYear_p5",
                "p6": "sectionb_noPolicyReasons_plannedNextYear_p6",
                "p7": "sectionb_noPolicyReasons_plannedNextYear_p7",
                "p8": "sectionb_noPolicyReasons_plannedNextYear_p8",
                "p9": "sectionb_noPolicyReasons_plannedNextYear_p9"
            },
            "otherReason": {
                "p1": "sectionb_noPolicyReasons_otherReason_p1",
                "p2": "sectionb_noPolicyReasons_otherReason_p2",
                "p3": "sectionb_noPolicyReasons_otherReason_p3",
                "p4": "sectionb_noPolicyReasons_otherReason_p4",
                "p5": "sectionb_noPolicyReasons_otherReason_p5",
                "p6": "sectionb_noPolicyReasons_otherReason_p6",
                "p7": "sectionb_noPolicyReasons_otherReason_p7",
                "p8": "sectionb_noPolicyReasons_otherReason_p8",
                "p9": "sectionb_noPolicyReasons_otherReason_p9"
            }
        }
    },

    "sectionC": {
        "principle1": {
            "essential": {
                "q1_percentageCoveredByTraining": {
                    "boardOfDirectors": {
                        "totalProgrammes": "sectionc_principle1_essential_q1_percentagecoveredbytraining_boardofdirectors_totalprogrammes",
                        "topicsCovered": "sectionc_principle1_essential_q1_percentagecoveredbytraining_boardofdirectors_topicscovered",
                        "percentageCovered": "sectionc_principle1_essential_q1_percentagecoveredbytraining_boardofdirectors_percentagecovered"
                    },
                    "kmp": {
                        "totalProgrammes": "sectionc_principle1_essential_q1_percentagecoveredbytraining_kmp_totalprogrammes",
                        "topicsCovered": "sectionc_principle1_essential_q1_percentagecoveredbytraining_kmp_topicscovered",
                        "percentageCovered": "sectionc_principle1_essential_q1_percentagecoveredbytraining_kmp_percentagecovered"
                    },
                    "employees": {
                        "totalProgrammes": "sectionc_principle1_essential_q1_percentagecoveredbytraining_employees_totalprogrammes",
                        "topicsCovered": "sectionc_principle1_essential_q1_percentagecoveredbytraining_employees_topicscovered",
                        "percentageCovered": "sectionc_principle1_essential_q1_percentagecoveredbytraining_employees_percentagecovered"
                    },
                    "workers": {
                        "totalProgrammes": "sectionc_principle1_essential_q1_percentagecoveredbytraining_workers_totalprogrammes",
                        "topicsCovered": "sectionc_principle1_essential_q1_percentagecoveredbytraining_workers_topicscovered",
                        "percentageCovered": "sectionc_principle1_essential_q1_percentagecoveredbytraining_workers_percentagecovered"
                    }
                },
                "q2_finesPenalties": {
                    "monetary": "sectionc_principle1_essential_q2_finespenalties_monetary_array",
                    "nonMonetary": "sectionc_principle1_essential_q2_finespenalties_nonmonetary_array"
                },
                "q3_appealsOutstanding": "sectionc_principle1_essential_q3_appealsoutstanding",
                "q4_antiCorruptionPolicy": {
                    "exists": "sectionc_principle1_essential_q4_anticorruptionpolicy_exists",
                    "details": "sectionc_principle1_essential_q4_anticorruptionpolicy_details",
                    "webLink": "sectionc_principle1_essential_q4_anticorruptionpolicy_weblink"
                },
                "q5_disciplinaryActions": {
                    "directors": {
                        "currentFY": "sectionc_principle1_essential_q5_disciplinaryactions_directors_currentfy",
                        "previousFY": "sectionc_principle1_essential_q5_disciplinaryactions_directors_previousfy"
                    },
                    "kmps": {
                        "currentFY": "sectionc_principle1_essential_q5_disciplinaryactions_kmps_currentfy",
                        "previousFY": "sectionc_principle1_essential_q5_disciplinaryactions_kmps_previousfy"
                    },
                    "employees": {
                        "currentFY": "sectionc_principle1_essential_q5_disciplinaryactions_employees_currentfy",
                        "previousFY": "sectionc_principle1_essential_q5_disciplinaryactions_employees_previousfy"
                    },
                    "workers": {
                        "currentFY": "sectionc_principle1_essential_q5_disciplinaryactions_workers_currentfy",
                        "previousFY": "sectionc_principle1_essential_q5_disciplinaryactions_workers_previousfy"
                    }
                },
                "q6_conflictOfInterestComplaints": {
                    "directors": {
                        "currentFY": {
                            "number": "sectionc_principle1_essential_q6_conflictofinterestcomplaints_directors_currentfy_number",
                            "remarks": "sectionc_principle1_essential_q6_conflictofinterestcomplaints_directors_currentfy_remarks"
                        },
                        "previousFY": {
                            "number": "sectionc_principle1_essential_q6_conflictofinterestcomplaints_directors_previousfy_number",
                            "remarks": "sectionc_principle1_essential_q6_conflictofinterestcomplaints_directors_previousfy_remarks"
                        }
                    },
                    "kmps": {
                        "currentFY": {
                            "number": "sectionc_principle1_essential_q6_conflictofinterestcomplaints_kmps_currentfy_number",
                            "remarks": "sectionc_principle1_essential_q6_conflictofinterestcomplaints_kmps_currentfy_remarks"
                        },
                        "previousFY": {
                            "number": "sectionc_principle1_essential_q6_conflictofinterestcomplaints_kmps_previousfy_number",
                            "remarks": "sectionc_principle1_essential_q6_conflictofinterestcomplaints_kmps_previousfy_remarks"
                        }
                    }
                },
                "q7_correctiveActions": "sectionc_principle1_essential_q7_correctiveactions",
                "q8_accountsPayableDays": {
                    "currentFY": "sectionc_principle1_essential_q8_accountspayabledays_currentfy",
                    "previousFY": "sectionc_principle1_essential_q8_accountspayabledays_previousfy"
                },
                "q9_opennessBusiness": {
                    "concentrationPurchases": {
                        "tradingHousesPercent": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationpurchases_tradinghousespercent_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationpurchases_tradinghousespercent_previousfy"
                        },
                        "dealersCount": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationpurchases_dealerscount_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationpurchases_dealerscount_previousfy"
                        },
                        "top10TradingHouses": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationpurchases_top10tradinghouses_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationpurchases_top10tradinghouses_previousfy"
                        }
                    },
                    "concentrationSales": {
                        "dealersDistributorsPercent": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationsales_dealersdistributorspercent_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationsales_dealersdistributorspercent_previousfy"
                        },
                        "dealersCount": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationsales_dealerscount_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationsales_dealerscount_previousfy"
                        },
                        "top10Dealers": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationsales_top10dealers_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_concentrationsales_top10dealers_previousfy"
                        }
                    },
                    "shareRPTs": {
                        "purchases": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_sharerpts_purchases_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_sharerpts_purchases_previousfy"
                        },
                        "sales": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_sharerpts_sales_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_sharerpts_sales_previousfy"
                        },
                        "loansAdvances": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_sharerpts_loansadvances_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_sharerpts_loansadvances_previousfy"
                        },
                        "investments": {
                            "currentFY": "sectionc_principle1_essential_q9_opennessbusiness_sharerpts_investments_currentfy",
                            "previousFY": "sectionc_principle1_essential_q9_opennessbusiness_sharerpts_investments_previousfy"
                        }
                    }
                }
            },
            "leadership": {
                "q1_valueChainAwareness": "sectionc_principle1_leadership_q1_valuechainawareness_array",
                "q2_conflictOfInterestProcess": {
                    "exists": "sectionc_principle1_leadership_q2_conflictofinterestprocess_exists",
                    "details": "sectionc_principle1_leadership_q2_conflictofinterestprocess_details"
                }
            }
        },

        "principle2": {
            "essential": {
                "q1_rdCapexInvestments": {
                    "rd": {
                        "currentFY": "sectionc_principle2_essential_q1_rdcapexinvestments_rd_currentfy",
                        "previousFY": "sectionc_principle2_essential_q1_rdcapexinvestments_rd_previousfy",
                        "improvementDetails": "sectionc_principle2_essential_q1_rdcapexinvestments_rd_improvementdetails"
                    },
                    "capex": {
                        "currentFY": "sectionc_principle2_essential_q1_rdcapexinvestments_capex_currentfy",
                        "previousFY": "sectionc_principle2_essential_q1_rdcapexinvestments_capex_previousfy",
                        "improvementDetails": "sectionc_principle2_essential_q1_rdcapexinvestments_capex_improvementdetails"
                    }
                },
                "q2_sustainableSourcing": {
                    "proceduresInPlace": "sectionc_principle2_essential_q2_sustainablesourcing_proceduresinplace",
                    "percentageSustainablySourced": "sectionc_principle2_essential_q2_sustainablesourcing_percentagesustainablysourced"
                },
                "q3_reclaimProcesses": {
                    "plastics": {
                        "applicable": "sectionc_principle2_essential_q3_reclaimprocesses_plastics_applicable",
                        "process": "sectionc_principle2_essential_q3_reclaimprocesses_plastics_process"
                    },
                    "eWaste": {
                        "applicable": "sectionc_principle2_essential_q3_reclaimprocesses_ewaste_applicable",
                        "process": "sectionc_principle2_essential_q3_reclaimprocesses_ewaste_process"
                    },
                    "hazardousWaste": {
                        "applicable": "sectionc_principle2_essential_q3_reclaimprocesses_hazardouswaste_applicable",
                        "process": "sectionc_principle2_essential_q3_reclaimprocesses_hazardouswaste_process"
                    },
                    "otherWaste": {
                        "applicable": "sectionc_principle2_essential_q3_reclaimprocesses_otherwaste_applicable",
                        "process": "sectionc_principle2_essential_q3_reclaimprocesses_otherwaste_process"
                    }
                },
                "q4_epr": {
                    "applicable": "sectionc_principle2_essential_q4_epr_applicable",
                    "wasteCollectionPlanInLine": "sectionc_principle2_essential_q4_epr_wastecollectionplaninline"
                }
            },
            "leadership": {
                "q1_lcaDetails": "sectionc_principle2_leadership_q1_lcadetails",
                "q2_significantConcerns": "sectionc_principle2_leadership_q2_significantconcerns",
                "q3_recycledInputMaterial": "sectionc_principle2_leadership_q3_recycledinputmaterial_array",
                "q4_productsReclaimed": {
                    "plastics": {
                        "currentFY": {
                            "reUsed": "sectionc_principle2_leadership_q4_productsreclaimed_plastics_currentfy_reused",
                            "recycled": "sectionc_principle2_leadership_q4_productsreclaimed_plastics_currentfy_recycled",
                            "safelyDisposed": "sectionc_principle2_leadership_q4_productsreclaimed_plastics_currentfy_safelydisposed"
                        },
                        "previousFY": {
                            "reUsed": "sectionc_principle2_leadership_q4_productsreclaimed_plastics_previousfy_reused",
                            "recycled": "sectionc_principle2_leadership_q4_productsreclaimed_plastics_previousfy_recycled",
                            "safelyDisposed": "sectionc_principle2_leadership_q4_productsreclaimed_plastics_previousfy_safelydisposed"
                        }
                    },
                    "eWaste": {
                        "currentFY": {
                            "reUsed": "sectionc_principle2_leadership_q4_productsreclaimed_ewaste_currentfy_reused",
                            "recycled": "sectionc_principle2_leadership_q4_productsreclaimed_ewaste_currentfy_recycled",
                            "safelyDisposed": "sectionc_principle2_leadership_q4_productsreclaimed_ewaste_currentfy_safelydisposed"
                        },
                        "previousFY": {
                            "reUsed": "sectionc_principle2_leadership_q4_productsreclaimed_ewaste_previousfy_reused",
                            "recycled": "sectionc_principle2_leadership_q4_productsreclaimed_ewaste_previousfy_recycled",
                            "safelyDisposed": "sectionc_principle2_leadership_q4_productsreclaimed_ewaste_previousfy_safelydisposed"
                        }
                    },
                    "hazardousWaste": {
                        "currentFY": {
                            "reUsed": "sectionc_principle2_leadership_q4_productsreclaimed_hazardouswaste_currentfy_reused",
                            "recycled": "sectionc_principle2_leadership_q4_productsreclaimed_hazardouswaste_currentfy_recycled",
                            "safelyDisposed": "sectionc_principle2_leadership_q4_productsreclaimed_hazardouswaste_currentfy_safelydisposed"
                        },
                        "previousFY": {
                            "reUsed": "sectionc_principle2_leadership_q4_productsreclaimed_hazardouswaste_previousfy_reused",
                            "recycled": "sectionc_principle2_leadership_q4_productsreclaimed_hazardouswaste_previousfy_recycled",
                            "safelyDisposed": "sectionc_principle2_leadership_q4_productsreclaimed_hazardouswaste_previousfy_safelydisposed"
                        }
                    },
                    "otherWaste": {
                        "currentFY": {
                            "reUsed": "sectionc_principle2_leadership_q4_productsreclaimed_otherwaste_currentfy_reused",
                            "recycled": "sectionc_principle2_leadership_q4_productsreclaimed_otherwaste_currentfy_recycled",
                            "safelyDisposed": "sectionc_principle2_leadership_q4_productsreclaimed_otherwaste_currentfy_safelydisposed"
                        },
                        "previousFY": {
                            "reUsed": "sectionc_principle2_leadership_q4_productsreclaimed_otherwaste_previousfy_reused",
                            "recycled": "sectionc_principle2_leadership_q4_productsreclaimed_otherwaste_previousfy_recycled",
                            "safelyDisposed": "sectionc_principle2_leadership_q4_productsreclaimed_otherwaste_previousfy_safelydisposed"
                        }
                    }
                },
                "q5_reclaimedPercentage": "sectionc_principle2_leadership_q5_reclaimedpercentage"
            }
        },

        "principle3": {
            "essential": {
                "q1a_employeeWellbeing": {
                    "permanentMale": {
                        "total": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_healthinsurance_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_daycare_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentmale_daycare_percent"}
                    },
                    "permanentFemale": {
                        "total": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_healthinsurance_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_daycare_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanentfemale_daycare_percent"}
                    },
                    "permanentTotal": {
                        "total": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_healthinsurance_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_daycare_no", "percent": "sectionc_principle3_essential_q1a_employeewellbeing_permanenttotal_daycare_percent"}
                    },
                    "otherMale": "sectionc_principle3_essential_q1a_employeewellbeing_othermale",
                    "otherFemale": "sectionc_principle3_essential_q1a_employeewellbeing_otherfemale",
                    "otherTotal": "sectionc_principle3_essential_q1a_employeewellbeing_othertotal"
                },
                "q1b_workerWellbeing": {
                    "permanentMale": {
                        "total": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_healthinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_daycare_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentmale_daycare_percent"}
                    },
                    "permanentFemale": {
                        "total": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_healthinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_daycare_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanentfemale_daycare_percent"}
                    },
                    "permanentTotal": {
                        "total": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_healthinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_daycare_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_permanenttotal_daycare_percent"}
                    },
                    "otherMale": {
                        "total": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_healthinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_daycare_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othermale_daycare_percent"}
                    },
                    "otherFemale": {
                        "total": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_healthinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_daycare_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_otherfemale_daycare_percent"}
                    },
                    "otherTotal": {
                        "total": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_total",
                        "healthInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_healthinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_healthinsurance_percent"},
                        "accidentInsurance": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_accidentinsurance_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_accidentinsurance_percent"},
                        "maternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_maternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_maternitybenefits_percent"},
                        "paternityBenefits": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_paternitybenefits_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_paternitybenefits_percent"},
                        "dayCare": {"no": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_daycare_no", "percent": "sectionc_principle3_essential_q1b_workerwellbeing_othertotal_daycare_percent"}
                    }
                },
                "q1c_spendingOnWellbeing": {"currentFY": "sectionc_principle3_essential_q1c_spendingonwellbeing_currentfy", "previousFY": "sectionc_principle3_essential_q1c_spendingonwellbeing_previousfy"},
                "q2_retirementBenefits": {
                    "pf": {
                        "currentFY": {"employeesPercent": "sectionc_principle3_essential_q2_retirementbenefits_pf_currentfy_employeespercent", "workersPercent": "sectionc_principle3_essential_q2_retirementbenefits_pf_currentfy_workerspercent", "deductedDeposited": "sectionc_principle3_essential_q2_retirementbenefits_pf_currentfy_deducteddeposited"},
                        "previousFY": {"employeesPercent": "sectionc_principle3_essential_q2_retirementbenefits_pf_previousfy_employeespercent", "workersPercent": "sectionc_principle3_essential_q2_retirementbenefits_pf_previousfy_workerspercent", "deductedDeposited": "sectionc_principle3_essential_q2_retirementbenefits_pf_previousfy_deducteddeposited"}
                    },
                    "gratuity": {
                        "currentFY": {"employeesPercent": "sectionc_principle3_essential_q2_retirementbenefits_gratuity_currentfy_employeespercent", "workersPercent": "sectionc_principle3_essential_q2_retirementbenefits_gratuity_currentfy_workerspercent", "deductedDeposited": "sectionc_principle3_essential_q2_retirementbenefits_gratuity_currentfy_deducteddeposited"},
                        "previousFY": {"employeesPercent": "sectionc_principle3_essential_q2_retirementbenefits_gratuity_previousfy_employeespercent", "workersPercent": "sectionc_principle3_essential_q2_retirementbenefits_gratuity_previousfy_workerspercent", "deductedDeposited": "sectionc_principle3_essential_q2_retirementbenefits_gratuity_previousfy_deducteddeposited"}
                    },
                    "esi": {
                        "currentFY": {"employeesPercent": "sectionc_principle3_essential_q2_retirementbenefits_esi_currentfy_employeespercent", "workersPercent": "sectionc_principle3_essential_q2_retirementbenefits_esi_currentfy_workerspercent", "deductedDeposited": "sectionc_principle3_essential_q2_retirementbenefits_esi_currentfy_deducteddeposited"},
                        "previousFY": {"employeesPercent": "sectionc_principle3_essential_q2_retirementbenefits_esi_previousfy_employeespercent", "workersPercent": "sectionc_principle3_essential_q2_retirementbenefits_esi_previousfy_workerspercent", "deductedDeposited": "sectionc_principle3_essential_q2_retirementbenefits_esi_previousfy_deducteddeposited"}
                    },
                    "nps": {
                        "currentFY": {"employeesPercent": "sectionc_principle3_essential_q2_retirementbenefits_nps_currentfy_employeespercent", "workersPercent": "sectionc_principle3_essential_q2_retirementbenefits_nps_currentfy_workerspercent", "deductedDeposited": "sectionc_principle3_essential_q2_retirementbenefits_nps_currentfy_deducteddeposited"},
                        "previousFY": {"employeesPercent": "sectionc_principle3_essential_q2_retirementbenefits_nps_previousfy_employeespercent", "workersPercent": "sectionc_principle3_essential_q2_retirementbenefits_nps_previousfy_workerspercent", "deductedDeposited": "sectionc_principle3_essential_q2_retirementbenefits_nps_previousfy_deducteddeposited"}
                    }
                },
                "q3_accessibilityOfWorkplaces": "sectionc_principle3_essential_q3_accessibilityofworkplaces",
                "q4_equalOpportunityPolicy": {"exists": "sectionc_principle3_essential_q4_equalopportunitypolicy_exists", "details": "sectionc_principle3_essential_q4_equalopportunitypolicy_details"},
                "q5_parentalLeaveRates": {
                    "permanentEmployees": {
                        "male": {"returnToWorkRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentemployees_male_returntoworkrate", "retentionRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentemployees_male_retentionrate"},
                        "female": {"returnToWorkRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentemployees_female_returntoworkrate", "retentionRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentemployees_female_retentionrate"},
                        "total": {"returnToWorkRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentemployees_total_returntoworkrate", "retentionRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentemployees_total_retentionrate"}
                    },
                    "permanentWorkers": {
                        "male": {"returnToWorkRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentworkers_male_returntoworkrate", "retentionRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentworkers_male_retentionrate"},
                        "female": {"returnToWorkRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentworkers_female_returntoworkrate", "retentionRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentworkers_female_retentionrate"},
                        "total": {"returnToWorkRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentworkers_total_returntoworkrate", "retentionRate": "sectionc_principle3_essential_q5_parentalleaverates_permanentworkers_total_retentionrate"}
                    }
                },
                "q6_grievanceMechanism": {
                    "permanentWorkers": "sectionc_principle3_essential_q6_grievancemechanism_permanentworkers",
                    "otherThanPermanentWorkers": "sectionc_principle3_essential_q6_grievancemechanism_otherthanpermanentworkers",
                    "permanentEmployees": "sectionc_principle3_essential_q6_grievancemechanism_permanentemployees",
                    "otherThanPermanentEmployees": "sectionc_principle3_essential_q6_grievancemechanism_otherthanpermanentemployees",
                    "details": "sectionc_principle3_essential_q6_grievancemechanism_details"
                },
                "q7_membershipUnions": {
                    "permanentEmployees": {
                        "currentFY": {"totalEmployees": "sectionc_principle3_essential_q7_membershipunions_permanentemployees_currentfy_totalemployees", "membersInUnions": "sectionc_principle3_essential_q7_membershipunions_permanentemployees_currentfy_membersinunions", "percentage": "sectionc_principle3_essential_q7_membershipunions_permanentemployees_currentfy_percentage"},
                        "previousFY": {"totalEmployees": "sectionc_principle3_essential_q7_membershipunions_permanentemployees_previousfy_totalemployees", "membersInUnions": "sectionc_principle3_essential_q7_membershipunions_permanentemployees_previousfy_membersinunions", "percentage": "sectionc_principle3_essential_q7_membershipunions_permanentemployees_previousfy_percentage"}
                    },
                    "permanentWorkers": {
                        "currentFY": {"totalWorkers": "sectionc_principle3_essential_q7_membershipunions_permanentworkers_currentfy_totalworkers", "membersInUnions": "sectionc_principle3_essential_q7_membershipunions_permanentworkers_currentfy_membersinunions", "percentage": "sectionc_principle3_essential_q7_membershipunions_permanentworkers_currentfy_percentage"},
                        "previousFY": {"totalWorkers": "sectionc_principle3_essential_q7_membershipunions_permanentworkers_previousfy_totalworkers", "membersInUnions": "sectionc_principle3_essential_q7_membershipunions_permanentworkers_previousfy_membersinunions", "percentage": "sectionc_principle3_essential_q7_membershipunions_permanentworkers_previousfy_percentage"}
                    }
                },
                "q8_trainingDetails": {
                    "employees": {
                        "currentFY": {
                            "male": {"total": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_male_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_male_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_male_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_male_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_male_skillupgradation_percent"}},
                            "female": {"total": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_female_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_female_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_female_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_female_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_female_skillupgradation_percent"}},
                            "total": {"total": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_total_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_total_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_total_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_total_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_currentfy_total_skillupgradation_percent"}}
                        },
                        "previousFY": {
                            "male": {"total": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_male_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_male_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_male_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_male_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_male_skillupgradation_percent"}},
                            "female": {"total": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_female_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_female_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_female_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_female_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_female_skillupgradation_percent"}},
                            "total": {"total": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_total_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_total_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_total_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_total_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_employees_previousfy_total_skillupgradation_percent"}}
                        }
                    },
                    "workers": {
                        "currentFY": {
                            "male": {"total": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_male_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_male_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_male_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_male_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_male_skillupgradation_percent"}},
                            "female": {"total": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_female_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_female_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_female_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_female_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_female_skillupgradation_percent"}},
                            "total": {"total": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_total_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_total_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_total_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_total_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_currentfy_total_skillupgradation_percent"}}
                        },
                        "previousFY": {
                            "male": {"total": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_male_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_male_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_male_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_male_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_male_skillupgradation_percent"}},
                            "female": {"total": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_female_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_female_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_female_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_female_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_female_skillupgradation_percent"}},
                            "total": {"total": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_total_total", "healthSafety": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_total_healthsafety_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_total_healthsafety_percent"}, "skillUpgradation": {"no": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_total_skillupgradation_no", "percent": "sectionc_principle3_essential_q8_trainingdetails_workers_previousfy_total_skillupgradation_percent"}}
                        }
                    }
                },
                "q9_performanceReviews": {
                    "employees": {
                        "currentFY": {
                            "male": {"total": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_male_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_male_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_male_percentage"},
                            "female": {"total": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_female_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_female_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_female_percentage"},
                            "total": {"total": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_total_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_total_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_employees_currentfy_total_percentage"}
                        },
                        "previousFY": {
                            "male": {"total": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_male_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_male_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_male_percentage"},
                            "female": {"total": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_female_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_female_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_female_percentage"},
                            "total": {"total": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_total_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_total_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_employees_previousfy_total_percentage"}
                        }
                    },
                    "workers": {
                        "currentFY": {
                            "male": {"total": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_male_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_male_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_male_percentage"},
                            "female": {"total": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_female_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_female_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_female_percentage"},
                            "total": {"total": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_total_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_total_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_workers_currentfy_total_percentage"}
                        },
                        "previousFY": {
                            "male": {"total": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_male_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_male_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_male_percentage"},
                            "female": {"total": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_female_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_female_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_female_percentage"},
                            "total": {"total": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_total_total", "reviewed": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_total_reviewed", "percentage": "sectionc_principle3_essential_q9_performancereviews_workers_previousfy_total_percentage"}
                        }
                    }
                },
                "q10_healthSafetyManagement": {"a": "sectionc_principle3_essential_q10_healthsafetymanagement_a", "b": "sectionc_principle3_essential_q10_healthsafetymanagement_b", "c": "sectionc_principle3_essential_q10_healthsafetymanagement_c", "d": "sectionc_principle3_essential_q10_healthsafetymanagement_d"},
                "q11_safetyIncidents": {
                    "ltifr": {
                        "employees": {"currentYear": "sectionc_principle3_essential_q11_safetyincidents_ltifr_employees_currentyear", "previousYear": "sectionc_principle3_essential_q11_safetyincidents_ltifr_employees_previousyear"},
                        "workers": {"currentYear": "sectionc_principle3_essential_q11_safetyincidents_ltifr_workers_currentyear", "previousYear": "sectionc_principle3_essential_q11_safetyincidents_ltifr_workers_previousyear"}
                    },
                    "totalRecordableInjuries": {
                        "employees": {"currentYear": "sectionc_principle3_essential_q11_safetyincidents_totalrecordableinjuries_employees_currentyear", "previousYear": "sectionc_principle3_essential_q11_safetyincidents_totalrecordableinjuries_employees_previousyear"},
                        "workers": {"currentYear": "sectionc_principle3_essential_q11_safetyincidents_totalrecordableinjuries_workers_currentyear", "previousYear": "sectionc_principle3_essential_q11_safetyincidents_totalrecordableinjuries_workers_previousyear"}
                    },
                    "fatalities": {
                        "employees": {"currentYear": "sectionc_principle3_essential_q11_safetyincidents_fatalities_employees_currentyear", "previousYear": "sectionc_principle3_essential_q11_safetyincidents_fatalities_employees_previousyear"},
                        "workers": {"currentYear": "sectionc_principle3_essential_q11_safetyincidents_fatalities_workers_currentyear", "previousYear": "sectionc_principle3_essential_q11_safetyincidents_fatalities_workers_previousyear"}
                    },
                    "highConsequenceInjuries": {
                        "employees": {"currentYear": "sectionc_principle3_essential_q11_safetyincidents_highconsequenceinjuries_employees_currentyear", "previousYear": "sectionc_principle3_essential_q11_safetyincidents_highconsequenceinjuries_employees_previousyear"},
                        "workers": {"currentYear": "sectionc_principle3_essential_q11_safetyincidents_highconsequenceinjuries_workers_currentyear", "previousYear": "sectionc_principle3_essential_q11_safetyincidents_highconsequenceinjuries_workers_previousyear"}
                    }
                },
                "q12_safetyMeasures": "sectionc_principle3_essential_q12_safetymeasures",
                "q13_complaintsWorkingConditions": {
                    "workingConditions": {
                        "currentFY": {"filed": "sectionc_principle3_essential_q13_complaintsworkingconditions_workingconditions_currentfy_filed", "pendingResolution": "sectionc_principle3_essential_q13_complaintsworkingconditions_workingconditions_currentfy_pendingresolution", "remarks": "sectionc_principle3_essential_q13_complaintsworkingconditions_workingconditions_currentfy_remarks"},
                        "previousFY": {"filed": "sectionc_principle3_essential_q13_complaintsworkingconditions_workingconditions_previousfy_filed", "pendingResolution": "sectionc_principle3_essential_q13_complaintsworkingconditions_workingconditions_previousfy_pendingresolution", "remarks": "sectionc_principle3_essential_q13_complaintsworkingconditions_workingconditions_previousfy_remarks"}
                    },
                    "healthSafety": {
                        "currentFY": {"filed": "sectionc_principle3_essential_q13_complaintsworkingconditions_healthsafety_currentfy_filed", "pendingResolution": "sectionc_principle3_essential_q13_complaintsworkingconditions_healthsafety_currentfy_pendingresolution", "remarks": "sectionc_principle3_essential_q13_complaintsworkingconditions_healthsafety_currentfy_remarks"},
                        "previousFY": {"filed": "sectionc_principle3_essential_q13_complaintsworkingconditions_healthsafety_previousfy_filed", "pendingResolution": "sectionc_principle3_essential_q13_complaintsworkingconditions_healthsafety_previousfy_pendingresolution", "remarks": "sectionc_principle3_essential_q13_complaintsworkingconditions_healthsafety_previousfy_remarks"}
                    }
                },
                "q14_assessments": {"healthSafetyPractices": "sectionc_principle3_essential_q14_assessments_healthsafetypractices", "workingConditions": "sectionc_principle3_essential_q14_assessments_workingconditions"},
                "q15_correctiveActions": "sectionc_principle3_essential_q15_correctiveactions"
            },
            "leadership": {
                "q1_lifeInsurance": "sectionc_principle3_leadership_q1_lifeinsurance",
                "q2_statutoryDuesValueChain": "sectionc_principle3_leadership_q2_statutoryduesvaluechain",
                "q3_rehabilitation": {
                    "employees": {
                        "currentFY": {"totalAffected": "sectionc_principle3_leadership_q3_rehabilitation_employees_currentfy_totalaffected", "rehabilitated": "sectionc_principle3_leadership_q3_rehabilitation_employees_currentfy_rehabilitated"},
                        "previousFY": {"totalAffected": "sectionc_principle3_leadership_q3_rehabilitation_employees_previousfy_totalaffected", "rehabilitated": "sectionc_principle3_leadership_q3_rehabilitation_employees_previousfy_rehabilitated"}
                    },
                    "workers": {
                        "currentFY": {"totalAffected": "sectionc_principle3_leadership_q3_rehabilitation_workers_currentfy_totalaffected", "rehabilitated": "sectionc_principle3_leadership_q3_rehabilitation_workers_currentfy_rehabilitated"},
                        "previousFY": {"totalAffected": "sectionc_principle3_leadership_q3_rehabilitation_workers_previousfy_totalaffected", "rehabilitated": "sectionc_principle3_leadership_q3_rehabilitation_workers_previousfy_rehabilitated"}
                    }
                },
                "q4_transitionAssistance": "sectionc_principle3_leadership_q4_transitionassistance",
                "q5_valueChainAssessment": {"healthSafetyPractices": "sectionc_principle3_leadership_q5_valuechainassessment_healthsafetypractices", "workingConditions": "sectionc_principle3_leadership_q5_valuechainassessment_workingconditions"},
                "q6_correctiveActionsValueChain": "sectionc_principle3_leadership_q6_correctiveactionsvaluechain"
            }
        },

        "principle4": {
            "essential": {
                "q1_stakeholderIdentification": "sectionc_principle4_essential_q1_stakeholderidentification",
                "q2_stakeholderEngagement": "sectionc_principle4_essential_q2_stakeholderengagement_array"
            },
            "leadership": {
                "q1_boardConsultation": "sectionc_principle4_leadership_q1_boardconsultation",
                "q2_stakeholderConsultationUsed": "sectionc_principle4_leadership_q2_stakeholderconsultationused",
                "q2_details": {"a": "sectionc_principle4_leadership_q2_details_a", "b": "sectionc_principle4_leadership_q2_details_b", "c": "sectionc_principle4_leadership_q2_details_c"},
                "q3_vulnerableEngagement": "sectionc_principle4_leadership_q3_vulnerableengagement_array"
            }
        },

        "principle5": {
            "essential": {
                "q1_humanRightsTraining": {
                    "employees": {
                        "permanent": {"total": "sectionc_principle5_essential_q1_humanrightstraining_employees_permanent_total", "covered": "sectionc_principle5_essential_q1_humanrightstraining_employees_permanent_covered", "percentage": "sectionc_principle5_essential_q1_humanrightstraining_employees_permanent_percentage"},
                        "otherThanPermanent": {"total": "sectionc_principle5_essential_q1_humanrightstraining_employees_otherthanpermanent_total", "covered": "sectionc_principle5_essential_q1_humanrightstraining_employees_otherthanpermanent_covered", "percentage": "sectionc_principle5_essential_q1_humanrightstraining_employees_otherthanpermanent_percentage"},
                        "totalEmployees": {"total": "sectionc_principle5_essential_q1_humanrightstraining_employees_totalemployees_total", "covered": "sectionc_principle5_essential_q1_humanrightstraining_employees_totalemployees_covered", "percentage": "sectionc_principle5_essential_q1_humanrightstraining_employees_totalemployees_percentage"}
                    },
                    "workers": {
                        "permanent": {"total": "sectionc_principle5_essential_q1_humanrightstraining_workers_permanent_total", "covered": "sectionc_principle5_essential_q1_humanrightstraining_workers_permanent_covered", "percentage": "sectionc_principle5_essential_q1_humanrightstraining_workers_permanent_percentage"},
                        "otherThanPermanent": {"total": "sectionc_principle5_essential_q1_humanrightstraining_workers_otherthanpermanent_total", "covered": "sectionc_principle5_essential_q1_humanrightstraining_workers_otherthanpermanent_covered", "percentage": "sectionc_principle5_essential_q1_humanrightstraining_workers_otherthanpermanent_percentage"},
                        "totalWorkers": {"total": "sectionc_principle5_essential_q1_humanrightstraining_workers_totalworkers_total", "covered": "sectionc_principle5_essential_q1_humanrightstraining_workers_totalworkers_covered", "percentage": "sectionc_principle5_essential_q1_humanrightstraining_workers_totalworkers_percentage"}
                    }
                },
                "q2_minimumWages": {
                    "employees": {
                        "permanent": {
                            "male": {
                                "currentFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_currentfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_currentfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_currentfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_currentfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_currentfy_morethanminwage_percent"
                                    }
                                },
                                "previousFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_previousfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_previousfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_previousfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_previousfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_male_previousfy_morethanminwage_percent"
                                    }
                                }
                            },
                            "female": {
                                "currentFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_currentfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_currentfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_currentfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_currentfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_currentfy_morethanminwage_percent"
                                    }
                                },
                                "previousFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_previousfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_previousfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_previousfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_previousfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_permanent_female_previousfy_morethanminwage_percent"
                                    }
                                }
                            }
                        },
                        "otherThanPermanent": {
                            "male": {
                                "currentFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_currentfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_currentfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_currentfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_currentfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_currentfy_morethanminwage_percent"
                                    }
                                },
                                "previousFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_previousfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_previousfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_previousfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_previousfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_male_previousfy_morethanminwage_percent"
                                    }
                                }
                            },
                            "female": {
                                "currentFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_currentfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_currentfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_currentfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_currentfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_currentfy_morethanminwage_percent"
                                    }
                                },
                                "previousFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_previousfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_previousfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_previousfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_previousfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_employees_otherthanpermanent_female_previousfy_morethanminwage_percent"
                                    }
                                }
                            }
                        }
                    },
                    "workers": {
                        "permanent": {
                            "male": {
                                "currentFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_currentfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_currentfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_currentfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_currentfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_currentfy_morethanminwage_percent"
                                    }
                                },
                                "previousFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_previousfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_previousfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_previousfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_previousfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_male_previousfy_morethanminwage_percent"
                                    }
                                }
                            },
                            "female": {
                                "currentFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_currentfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_currentfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_currentfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_currentfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_currentfy_morethanminwage_percent"
                                    }
                                },
                                "previousFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_previousfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_previousfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_previousfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_previousfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_permanent_female_previousfy_morethanminwage_percent"
                                    }
                                }
                            }
                        },
                        "otherThanPermanent": {
                            "male": {
                                "currentFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_currentfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_currentfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_currentfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_currentfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_currentfy_morethanminwage_percent"
                                    }
                                },
                                "previousFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_previousfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_previousfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_previousfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_previousfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_male_previousfy_morethanminwage_percent"
                                    }
                                }
                            },
                            "female": {
                                "currentFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_currentfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_currentfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_currentfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_currentfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_currentfy_morethanminwage_percent"
                                    }
                                },
                                "previousFY": {
                                    "total": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_previousfy_total",
                                    "equalToMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_previousfy_equaltominwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_previousfy_equaltominwage_percent"
                                    },
                                    "moreThanMinWage": {
                                        "no": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_previousfy_morethanminwage_no",
                                        "percent": "sectionc_principle5_essential_q2_minimumwages_workers_otherthanpermanent_female_previousfy_morethanminwage_percent"
                                    }
                                }
                            }
                        }
                    }
                },
                "q3_medianRemuneration": {
                    "boardOfDirectors": {"male": {"number": "sectionc_principle5_essential_q3_medianremuneration_boardofdirectors_male_number", "median": "sectionc_principle5_essential_q3_medianremuneration_boardofdirectors_male_median"}, "female": {"number": "sectionc_principle5_essential_q3_medianremuneration_boardofdirectors_female_number", "median": "sectionc_principle5_essential_q3_medianremuneration_boardofdirectors_female_median"}},
                    "keyManagerialPersonnel": {"male": {"number": "sectionc_principle5_essential_q3_medianremuneration_keymanagerialpersonnel_male_number", "median": "sectionc_principle5_essential_q3_medianremuneration_keymanagerialpersonnel_male_median"}, "female": {"number": "sectionc_principle5_essential_q3_medianremuneration_keymanagerialpersonnel_female_number", "median": "sectionc_principle5_essential_q3_medianremuneration_keymanagerialpersonnel_female_median"}},
                    "employeesOtherThanBoDAndKMP": {"male": {"number": "sectionc_principle5_essential_q3_medianremuneration_employeesotherthanbodandkmp_male_number", "median": "sectionc_principle5_essential_q3_medianremuneration_employeesotherthanbodandkmp_male_median"}, "female": {"number": "sectionc_principle5_essential_q3_medianremuneration_employeesotherthanbodandkmp_female_number", "median": "sectionc_principle5_essential_q3_medianremuneration_employeesotherthanbodandkmp_female_median"}},
                    "workers": {"male": {"number": "sectionc_principle5_essential_q3_medianremuneration_workers_male_number", "median": "sectionc_principle5_essential_q3_medianremuneration_workers_male_median"}, "female": {"number": "sectionc_principle5_essential_q3_medianremuneration_workers_female_number", "median": "sectionc_principle5_essential_q3_medianremuneration_workers_female_median"}}
                },
                "q3a_grossWagesFemales": {"currentFY": "sectionc_principle5_essential_q3a_grosswagesfemales_currentfy", "previousFY": "sectionc_principle5_essential_q3a_grosswagesfemales_previousfy"},
                "q4_focalPointHumanRights": "sectionc_principle5_essential_q4_focalpointhumanrights",
                "q5_grievanceMechanisms": "sectionc_principle5_essential_q5_grievancemechanisms",
                "q6_complaints": {
                    "sexualHarassment": {
                        "currentFY": {"filed": "sectionc_principle5_essential_q6_complaints_sexualharassment_currentfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_sexualharassment_currentfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_sexualharassment_currentfy_remarks"},
                        "previousFY": {"filed": "sectionc_principle5_essential_q6_complaints_sexualharassment_previousfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_sexualharassment_previousfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_sexualharassment_previousfy_remarks"}
                    },
                    "discriminationAtWorkplace": {
                        "currentFY": {"filed": "sectionc_principle5_essential_q6_complaints_discriminationatworkplace_currentfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_discriminationatworkplace_currentfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_discriminationatworkplace_currentfy_remarks"},
                        "previousFY": {"filed": "sectionc_principle5_essential_q6_complaints_discriminationatworkplace_previousfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_discriminationatworkplace_previousfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_discriminationatworkplace_previousfy_remarks"}
                    },
                    "childLabour": {
                        "currentFY": {"filed": "sectionc_principle5_essential_q6_complaints_childlabour_currentfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_childlabour_currentfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_childlabour_currentfy_remarks"},
                        "previousFY": {"filed": "sectionc_principle5_essential_q6_complaints_childlabour_previousfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_childlabour_previousfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_childlabour_previousfy_remarks"}
                    },
                    "forcedLabour": {
                        "currentFY": {"filed": "sectionc_principle5_essential_q6_complaints_forcedlabour_currentfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_forcedlabour_currentfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_forcedlabour_currentfy_remarks"},
                        "previousFY": {"filed": "sectionc_principle5_essential_q6_complaints_forcedlabour_previousfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_forcedlabour_previousfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_forcedlabour_previousfy_remarks"}
                    },
                    "wages": {
                        "currentFY": {"filed": "sectionc_principle5_essential_q6_complaints_wages_currentfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_wages_currentfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_wages_currentfy_remarks"},
                        "previousFY": {"filed": "sectionc_principle5_essential_q6_complaints_wages_previousfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_wages_previousfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_wages_previousfy_remarks"}
                    },
                    "otherHumanRights": {
                        "currentFY": {"filed": "sectionc_principle5_essential_q6_complaints_otherhumanrights_currentfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_otherhumanrights_currentfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_otherhumanrights_currentfy_remarks"},
                        "previousFY": {"filed": "sectionc_principle5_essential_q6_complaints_otherhumanrights_previousfy_filed", "pending": "sectionc_principle5_essential_q6_complaints_otherhumanrights_previousfy_pending", "remarks": "sectionc_principle5_essential_q6_complaints_otherhumanrights_previousfy_remarks"}
                    }
                },
                "q7_poshComplaints": {
                    "totalComplaints": {"currentFY": "sectionc_principle5_essential_q7_poshcomplaints_totalcomplaints_currentfy", "previousFY": "sectionc_principle5_essential_q7_poshcomplaints_totalcomplaints_previousfy"},
                    "complaintsAsPercentFemale": {"currentFY": "sectionc_principle5_essential_q7_poshcomplaints_complaintsaspercentfemale_currentfy", "previousFY": "sectionc_principle5_essential_q7_poshcomplaints_complaintsaspercentfemale_previousfy"},
                    "complaintsUpheld": {"currentFY": "sectionc_principle5_essential_q7_poshcomplaints_complaintsupheld_currentfy", "previousFY": "sectionc_principle5_essential_q7_poshcomplaints_complaintsupheld_previousfy"}
                },
                "q8_mechanismsPreventAdverseConsequences": "sectionc_principle5_essential_q8_mechanismspreventadverseconsequences",
                "q9_humanRightsInContracts": "sectionc_principle5_essential_q9_humanrightsincontracts",
                "q10_assessments": {
                    "childLabour": "sectionc_principle5_essential_q10_assessments_childlabour",
                    "forcedInvoluntaryLabour": "sectionc_principle5_essential_q10_assessments_forcedinvoluntarylabour",
                    "sexualHarassment": "sectionc_principle5_essential_q10_assessments_sexualharassment",
                    "discriminationAtWorkplace": "sectionc_principle5_essential_q10_assessments_discriminationatworkplace",
                    "wages": "sectionc_principle5_essential_q10_assessments_wages",
                    "othersSpecify": "sectionc_principle5_essential_q10_assessments_othersspecify"
                },
                "q11_correctiveActions": "sectionc_principle5_essential_q11_correctiveactions"
            },
            "leadership": {
                "q1_businessProcessModified": "sectionc_principle5_leadership_q1_businessprocessmodified",
                "q2_humanRightsDueDiligence": "sectionc_principle5_leadership_q2_humanrightsduediligence",
                "q3_accessibilityDifferentlyAbled": "sectionc_principle5_leadership_q3_accessibilitydifferentlyabled",
                "q4_valueChainAssessment": {
                    "sexualHarassment": "sectionc_principle5_leadership_q4_valuechainassessment_sexualharassment",
                    "discriminationAtWorkplace": "sectionc_principle5_leadership_q4_valuechainassessment_discriminationatworkplace",
                    "childLabour": "sectionc_principle5_leadership_q4_valuechainassessment_childlabour",
                    "forcedLabourInvoluntaryLabour": "sectionc_principle5_leadership_q4_valuechainassessment_forcedlabourinvoluntarylabour",
                    "wages": "sectionc_principle5_leadership_q4_valuechainassessment_wages",
                    "othersSpecify": "sectionc_principle5_leadership_q4_valuechainassessment_othersspecify"
                },
                "q5_correctiveActionsValueChain": "sectionc_principle5_leadership_q5_correctiveactionsvaluechain"
            }
        },

        "principle6": {
            "essential": {
                "q1_energyConsumption": {
                    "renewable": {
                        "electricity": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_renewable_electricity_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_renewable_electricity_previousfy"},
                        "fuel": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_renewable_fuel_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_renewable_fuel_previousfy"},
                        "otherSources": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_renewable_othersources_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_renewable_othersources_previousfy"},
                        "total": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_renewable_total_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_renewable_total_previousfy"}
                    },
                    "nonRenewable": {
                        "electricity": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_nonrenewable_electricity_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_nonrenewable_electricity_previousfy"},
                        "fuel": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_nonrenewable_fuel_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_nonrenewable_fuel_previousfy"},
                        "otherSources": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_nonrenewable_othersources_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_nonrenewable_othersources_previousfy"},
                        "total": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_nonrenewable_total_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_nonrenewable_total_previousfy"}
                    },
                    "totalEnergyConsumed": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_totalenergyconsumed_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_totalenergyconsumed_previousfy"},
                    "energyIntensityPerTurnover": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_energyintensityperturnover_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_energyintensityperturnover_previousfy"},
                    "energyIntensityPPP": {"currentFY": "sectionc_principle6_essential_q1_energyconsumption_energyintensityppp_currentfy", "previousFY": "sectionc_principle6_essential_q1_energyconsumption_energyintensityppp_previousfy"},
                    "energyIntensityPhysicalOutput": "sectionc_principle6_essential_q1_energyconsumption_energyintensityphysicaloutput",
                    "externalAssessment": "sectionc_principle6_essential_q1_energyconsumption_externalassessment"
                },
                "q2_patScheme": "sectionc_principle6_essential_q2_patscheme",
                "q2_patFacilities": "sectionc_principle6_essential_q2_patfacilities_array",
                "q3_waterDetails": {
                    "withdrawal": {
                        "surfaceWater": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_surfacewater_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_surfacewater_previousfy"},
                        "groundwater": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_groundwater_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_groundwater_previousfy"},
                        "thirdPartyWater": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_thirdpartywater_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_thirdpartywater_previousfy"},
                        "seawaterDesalinated": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_seawaterdesalinated_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_seawaterdesalinated_previousfy"},
                        "others": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_others_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_others_previousfy"},
                        "total": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_total_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_withdrawal_total_previousfy"}
                    },
                    "consumption": {"total": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_consumption_total_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_consumption_total_previousfy"}},
                    "waterIntensityPerTurnover": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_waterintensityperturnover_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_waterintensityperturnover_previousfy"},
                    "waterIntensityPPP": {"currentFY": "sectionc_principle6_essential_q3_waterdetails_waterintensityppp_currentfy", "previousFY": "sectionc_principle6_essential_q3_waterdetails_waterintensityppp_previousfy"},
                    "waterIntensityPhysicalOutput": "sectionc_principle6_essential_q3_waterdetails_waterintensityphysicaloutput",
                    "externalAssessment": "sectionc_principle6_essential_q3_waterdetails_externalassessment"
                },
                "q4_waterDischarge": {
                    "surfaceWater": {"noTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_surfacewater_notreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_surfacewater_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_surfacewater_withtreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_surfacewater_withtreatment_previousfy"}},
                    "groundwater": {"noTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_groundwater_notreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_groundwater_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_groundwater_withtreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_groundwater_withtreatment_previousfy"}},
                    "seawater": {"noTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_seawater_notreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_seawater_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_seawater_withtreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_seawater_withtreatment_previousfy"}},
                    "thirdParties": {"noTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_thirdparties_notreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_thirdparties_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_thirdparties_withtreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_thirdparties_withtreatment_previousfy"}},
                    "others": {"noTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_others_notreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_others_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_others_withtreatment_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_others_withtreatment_previousfy"}},
                    "totalWaterDischarged": {"currentFY": "sectionc_principle6_essential_q4_waterdischarge_totalwaterdischarged_currentfy", "previousFY": "sectionc_principle6_essential_q4_waterdischarge_totalwaterdischarged_previousfy"},
                    "externalAssessment": "sectionc_principle6_essential_q4_waterdischarge_externalassessment"
                },
                "q5_zeroLiquidDischarge": "sectionc_principle6_essential_q5_zeroliquiddischarge",
                "q6_airEmissions": {
                    "nox": {"unit": "sectionc_principle6_essential_q6_airemissions_nox_unit", "currentFY": "sectionc_principle6_essential_q6_airemissions_nox_currentfy", "previousFY": "sectionc_principle6_essential_q6_airemissions_nox_previousfy"},
                    "sox": {"unit": "sectionc_principle6_essential_q6_airemissions_sox_unit", "currentFY": "sectionc_principle6_essential_q6_airemissions_sox_currentfy", "previousFY": "sectionc_principle6_essential_q6_airemissions_sox_previousfy"},
                    "pm": {"unit": "sectionc_principle6_essential_q6_airemissions_pm_unit", "currentFY": "sectionc_principle6_essential_q6_airemissions_pm_currentfy", "previousFY": "sectionc_principle6_essential_q6_airemissions_pm_previousfy"},
                    "pop": {"unit": "sectionc_principle6_essential_q6_airemissions_pop_unit", "currentFY": "sectionc_principle6_essential_q6_airemissions_pop_currentfy", "previousFY": "sectionc_principle6_essential_q6_airemissions_pop_previousfy"},
                    "voc": {"unit": "sectionc_principle6_essential_q6_airemissions_voc_unit", "currentFY": "sectionc_principle6_essential_q6_airemissions_voc_currentfy", "previousFY": "sectionc_principle6_essential_q6_airemissions_voc_previousfy"},
                    "hap": {"unit": "sectionc_principle6_essential_q6_airemissions_hap_unit", "currentFY": "sectionc_principle6_essential_q6_airemissions_hap_currentfy", "previousFY": "sectionc_principle6_essential_q6_airemissions_hap_previousfy"},
                    "others": {"unit": "sectionc_principle6_essential_q6_airemissions_others_unit", "currentFY": "sectionc_principle6_essential_q6_airemissions_others_currentfy", "previousFY": "sectionc_principle6_essential_q6_airemissions_others_previousfy"},
                    "externalAssessment": "sectionc_principle6_essential_q6_airemissions_externalassessment"
                },
                "q7_ghgEmissions": {
                    "scope1": {"unit": "sectionc_principle6_essential_q7_ghgemissions_scope1_unit", "currentFY": "sectionc_principle6_essential_q7_ghgemissions_scope1_currentfy", "previousFY": "sectionc_principle6_essential_q7_ghgemissions_scope1_previousfy"},
                    "scope2": {"unit": "sectionc_principle6_essential_q7_ghgemissions_scope2_unit", "currentFY": "sectionc_principle6_essential_q7_ghgemissions_scope2_currentfy", "previousFY": "sectionc_principle6_essential_q7_ghgemissions_scope2_previousfy"},
                    "totalScope1And2": {"unit": "sectionc_principle6_essential_q7_ghgemissions_totalscope1and2_unit", "currentFY": "sectionc_principle6_essential_q7_ghgemissions_totalscope1and2_currentfy", "previousFY": "sectionc_principle6_essential_q7_ghgemissions_totalscope1and2_previousfy"},
                    "scope1And2IntensityPerTurnover": {"unit": "sectionc_principle6_essential_q7_ghgemissions_scope1and2intensityperturnover_unit", "currentFY": "sectionc_principle6_essential_q7_ghgemissions_scope1and2intensityperturnover_currentfy", "previousFY": "sectionc_principle6_essential_q7_ghgemissions_scope1and2intensityperturnover_previousfy"},
                    "scope1And2IntensityPhysicalOutput": "sectionc_principle6_essential_q7_ghgemissions_scope1and2intensityphysicaloutput",
                    "externalAssessment": "sectionc_principle6_essential_q7_ghgemissions_externalassessment"
                },
                "q8_ghgReductionProjects": "sectionc_principle6_essential_q8_ghgreductionprojects",
                "q9_wasteManagement": {
                    "plasticWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_plasticwaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_plasticwaste_previousfy"},
                    "eWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_ewaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_ewaste_previousfy"},
                    "bioMedicalWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_biomedicalwaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_biomedicalwaste_previousfy"},
                    "constructionDemolitionWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_constructiondemolitionwaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_constructiondemolitionwaste_previousfy"},
                    "batteryWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_batterywaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_batterywaste_previousfy"},
                    "radioactiveWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_radioactivewaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_radioactivewaste_previousfy"},
                    "otherHazardousWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_otherhazardouswaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_otherhazardouswaste_previousfy"},
                    "otherNonHazardousWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_othernonhazardouswaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_othernonhazardouswaste_previousfy"},
                    "totalWaste": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_totalwaste_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_totalwaste_previousfy"},
                    "wasteIntensityPerTurnover": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_wasteintensityperturnover_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_wasteintensityperturnover_previousfy"},
                    "wasteIntensityPPP": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_wasteintensityppp_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_wasteintensityppp_previousfy"},
                    "wasteIntensityPhysicalOutput": "sectionc_principle6_essential_q9_wastemanagement_wasteintensityphysicaloutput",
                    "recycled": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_recycled_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_recycled_previousfy"},
                    "reused": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_reused_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_reused_previousfy"},
                    "otherRecovery": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_otherrecovery_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_otherrecovery_previousfy"},
                    "totalRecovered": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_totalrecovered_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_totalrecovered_previousfy"},
                    "incineration": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_incineration_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_incineration_previousfy"},
                    "landfilling": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_landfilling_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_landfilling_previousfy"},
                    "otherDisposal": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_otherdisposal_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_otherdisposal_previousfy"},
                    "totalDisposed": {"currentFY": "sectionc_principle6_essential_q9_wastemanagement_totaldisposed_currentfy", "previousFY": "sectionc_principle6_essential_q9_wastemanagement_totaldisposed_previousfy"},
                    "externalAssessment": "sectionc_principle6_essential_q9_wastemanagement_externalassessment"
                },
                "q10_wastePractices": "sectionc_principle6_essential_q10_wastepractices",
                "q11_ecologicallySensitiveAreas": "sectionc_principle6_essential_q11_ecologicallysensitiveareas",
                "q11_ecologicallySensitiveDetails": "sectionc_principle6_essential_q11_ecologicallysensitivedetails",
                "q12_environmentalImpactAssessments": "sectionc_principle6_essential_q12_environmentalimpactassessments",
                "q13_environmentalCompliance": "sectionc_principle6_essential_q13_environmentalcompliance",
                "q13_nonCompliances": "sectionc_principle6_essential_q13_noncompliances"
            },
            "leadership": {
                "q1_waterStressAreas": {
                    "name": "sectionc_principle6_leadership_q1_waterstressareas_name",
                    "natureOfOperations": "sectionc_principle6_leadership_q1_waterstressareas_natureofoperations",
                    "withdrawal": {
                        "surfaceWater": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_surfacewater_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_surfacewater_previousfy"},
                        "groundwater": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_groundwater_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_groundwater_previousfy"},
                        "thirdPartyWater": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_thirdpartywater_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_thirdpartywater_previousfy"},
                        "seawaterDesalinated": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_seawaterdesalinated_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_seawaterdesalinated_previousfy"},
                        "others": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_others_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_others_previousfy"},
                        "total": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_total_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_withdrawal_total_previousfy"}
                    },
                    "consumption": {"total": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_consumption_total_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_consumption_total_previousfy"}},
                    "waterIntensityPerTurnover": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_waterintensityperturnover_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_waterintensityperturnover_previousfy"},
                    "waterIntensityPhysicalOutput": "sectionc_principle6_leadership_q1_waterstressareas_waterintensityphysicaloutput",
                    "discharge": {
                        "surfaceWater": {"noTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_surfacewater_notreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_surfacewater_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_surfacewater_withtreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_surfacewater_withtreatment_previousfy"}},
                        "groundwater": {"noTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_groundwater_notreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_groundwater_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_groundwater_withtreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_groundwater_withtreatment_previousfy"}},
                        "seawater": {"noTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_seawater_notreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_seawater_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_seawater_withtreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_seawater_withtreatment_previousfy"}},
                        "thirdParties": {"noTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_thirdparties_notreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_thirdparties_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_thirdparties_withtreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_thirdparties_withtreatment_previousfy"}},
                        "others": {"noTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_others_notreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_others_notreatment_previousfy"}, "withTreatment": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_others_withtreatment_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_others_withtreatment_previousfy"}},
                        "total": {"currentFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_total_currentfy", "previousFY": "sectionc_principle6_leadership_q1_waterstressareas_discharge_total_previousfy"}
                    },
                    "externalAssessment": "sectionc_principle6_leadership_q1_waterstressareas_externalassessment"
                },
                "q2_scope3Emissions": "sectionc_principle6_leadership_q2_scope3emissions",
                "q2_scope3EmissionsPerTurnover": "sectionc_principle6_leadership_q2_scope3emissionsperturnover",
                "q2_scope3IntensityPhysicalOutput": "sectionc_principle6_leadership_q2_scope3intensityphysicaloutput",
                "q2_externalAssessment": "sectionc_principle6_leadership_q2_externalassessment",
                "q3_biodiversityImpact": "sectionc_principle6_leadership_q3_biodiversityimpact",
                "q4_resourceEfficiencyInitiatives": "sectionc_principle6_leadership_q4_resourceefficiencyinitiatives_array",
                "q5_businessContinuityPlan": "sectionc_principle6_leadership_q5_businesscontinuityplan",
                "q6_valueChainEnvironmentalImpact": "sectionc_principle6_leadership_q6_valuechainenvironmentalimpact",
                "q7_valueChainPartnersAssessed": "sectionc_principle6_leadership_q7_valuechainpartnersassessed"
            }
        },

        "principle7": {
            "essential": {
                "q1a_numberOfAffiliations": "sectionc_principle7_essential_q1a_numberofaffiliations",
                "q1b_affiliationsList": "sectionc_principle7_essential_q1b_affiliationslist_array",
                "q2_antiCompetitiveConduct": "sectionc_principle7_essential_q2_anticompetitiveconduct"
            },
            "leadership": {
                "q1_publicPolicyAdvocacy": "sectionc_principle7_leadership_q1_publicpolicyadvocacy_array"
            }
        },

        "principle8": {
            "essential": {
                "q1_socialImpactAssessments": "sectionc_principle8_essential_q1_socialimpactassessments",
                "q2_rehabilitationResettlement": "sectionc_principle8_essential_q2_rehabilitationresettlement",
                "q3_communityGrievanceMechanism": "sectionc_principle8_essential_q3_communitygrievancemechanism",
                "q4_inputMaterialSourcing": {
                    "msmes": {
                        "currentFY": "sectionc_principle8_essential_q4_inputmaterialsourcing_msmes_currentfy",
                        "previousFY": "sectionc_principle8_essential_q4_inputmaterialsourcing_msmes_previousfy"
                    },
                    "withinDistrict": {
                        "currentFY": "sectionc_principle8_essential_q4_inputmaterialsourcing_withindistrict_currentfy",
                        "previousFY": "sectionc_principle8_essential_q4_inputmaterialsourcing_withindistrict_previousfy"
                    },
                    "neighboringDistricts": {
                        "currentFY": "sectionc_principle8_essential_q4_inputmaterialsourcing_neighboringdistricts_currentfy",
                        "previousFY": "sectionc_principle8_essential_q4_inputmaterialsourcing_neighboringdistricts_previousfy"
                    }
                },
                "q5_jobCreation": {
                    "rural": {
                        "currentFY": "sectionc_principle8_essential_q5_jobcreation_rural_currentfy",
                        "previousFY": "sectionc_principle8_essential_q5_jobcreation_rural_previousfy"
                    },
                    "semiUrban": {
                        "currentFY": "sectionc_principle8_essential_q5_jobcreation_semiurban_currentfy",
                        "previousFY": "sectionc_principle8_essential_q5_jobcreation_semiurban_previousfy"
                    },
                    "urban": {
                        "currentFY": "sectionc_principle8_essential_q5_jobcreation_urban_currentfy",
                        "previousFY": "sectionc_principle8_essential_q5_jobcreation_urban_previousfy"
                    },
                    "metropolitan": {
                        "currentFY": "sectionc_principle8_essential_q5_jobcreation_metropolitan_currentfy",
                        "previousFY": "sectionc_principle8_essential_q5_jobcreation_metropolitan_previousfy"
                    }
                }
            },
            "leadership": {
                "q1_negativeImpactMitigation": "sectionc_principle8_leadership_q1_negativeimpactmitigation",
                "q2_csrProjects": "sectionc_principle8_leadership_q2_csrprojects_array",
                "q3a_preferentialProcurement": "sectionc_principle8_leadership_q3a_preferentialprocurement",
                "q3b_vulnerableGroups": "sectionc_principle8_leadership_q3b_vulnerablegroups",
                "q3c_procurementPercentage": "sectionc_principle8_leadership_q3c_procurementpercentage",
                "q4_intellectualProperty": "sectionc_principle8_leadership_q4_intellectualproperty",
                "q5_ipDisputes": "sectionc_principle8_leadership_q5_ipdisputes",
                "q6_csrBeneficiaries": "sectionc_principle8_leadership_q6_csrbeneficiaries_array"
            }
        },

        "principle9": {
            "essential": {
                "q1_consumerComplaintMechanism": "sectionc_principle9_essential_q1_consumercomplaintmechanism",
                "q2_productInformationPercentage": {
                    "environmentalParameters": "sectionc_principle9_essential_q2_productinformationpercentage_environmentalparameters",
                    "safeUsage": "sectionc_principle9_essential_q2_productinformationpercentage_safeusage",
                    "recycling": "sectionc_principle9_essential_q2_productinformationpercentage_recycling"
                },
                "q3_consumerComplaints": {
                    "dataPrivacy": {
                        "currentFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_dataprivacy_currentfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_dataprivacy_currentfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_dataprivacy_currentfy_remarks"
                        },
                        "previousFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_dataprivacy_previousfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_dataprivacy_previousfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_dataprivacy_previousfy_remarks"
                        }
                    },
                    "advertising": {
                        "currentFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_advertising_currentfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_advertising_currentfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_advertising_currentfy_remarks"
                        },
                        "previousFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_advertising_previousfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_advertising_previousfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_advertising_previousfy_remarks"
                        }
                    },
                    "cyberSecurity": {
                        "currentFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_cybersecurity_currentfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_cybersecurity_currentfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_cybersecurity_currentfy_remarks"
                        },
                        "previousFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_cybersecurity_previousfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_cybersecurity_previousfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_cybersecurity_previousfy_remarks"
                        }
                    },
                    "deliveryOfServices": {
                        "currentFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_deliveryofservices_currentfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_deliveryofservices_currentfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_deliveryofservices_currentfy_remarks"
                        },
                        "previousFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_deliveryofservices_previousfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_deliveryofservices_previousfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_deliveryofservices_previousfy_remarks"
                        }
                    },
                    "restrictiveTradePractices": {
                        "currentFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_restrictivetradepractices_currentfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_restrictivetradepractices_currentfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_restrictivetradepractices_currentfy_remarks"
                        },
                        "previousFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_restrictivetradepractices_previousfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_restrictivetradepractices_previousfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_restrictivetradepractices_previousfy_remarks"
                        }
                    },
                    "unfairTradePractices": {
                        "currentFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_unfairtradepractices_currentfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_unfairtradepractices_currentfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_unfairtradepractices_currentfy_remarks"
                        },
                        "previousFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_unfairtradepractices_previousfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_unfairtradepractices_previousfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_unfairtradepractices_previousfy_remarks"
                        }
                    },
                    "other": {
                        "currentFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_other_currentfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_other_currentfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_other_currentfy_remarks"
                        },
                        "previousFY": {
                            "received": "sectionc_principle9_essential_q3_consumercomplaints_other_previousfy_received",
                            "pending": "sectionc_principle9_essential_q3_consumercomplaints_other_previousfy_pending",
                            "remarks": "sectionc_principle9_essential_q3_consumercomplaints_other_previousfy_remarks"
                        }
                    }
                },
                "q4_productRecalls": {
                    "voluntary": {
                        "number": "sectionc_principle9_essential_q4_productrecalls_voluntary_number",
                        "reasons": "sectionc_principle9_essential_q4_productrecalls_voluntary_reasons"
                    },
                    "forced": {
                        "number": "sectionc_principle9_essential_q4_productrecalls_forced_number",
                        "reasons": "sectionc_principle9_essential_q4_productrecalls_forced_reasons"
                    }
                },
                "q5_cyberSecurityPolicy": "sectionc_principle9_essential_q5_cybersecuritypolicy",
                "q6_correctiveActions": "sectionc_principle9_essential_q6_correctiveactions",
                "q7_dataBreaches": {
                    "a_numberOfInstances": "sectionc_principle9_essential_q7_databreaches_a_numberofinstances",
                    "b_percentageWithPII": "sectionc_principle9_essential_q7_databreaches_b_percentagewithpii",
                    "c_impact": "sectionc_principle9_essential_q7_databreaches_c_impact"
                }
            },
            "leadership": {
                "q1_informationChannels": "sectionc_principle9_leadership_q1_informationchannels",
                "q2_consumerEducation": "sectionc_principle9_leadership_q2_consumereducation",
                "q3_disruptionMechanisms": "sectionc_principle9_leadership_q3_disruptionmechanisms",
                "q4_productInformationDisplay": "sectionc_principle9_leadership_q4_productinformationdisplay",
                "q5_dataBreaches": {
                    "a_numberOfInstances": "sectionc_principle9_leadership_q5_databreaches_a_numberofinstances",
                    "b_percentageWithPII": "sectionc_principle9_leadership_q5_databreaches_b_percentagewithpii"
                }
            }
        }
    }
}


def get_section(section_name: str) -> Dict[str, Any]:
    """Get a specific section from the skeleton"""
    return BRSR_DATA_SKELETON.get(section_name, {})


def get_principle(principle_num: int) -> Dict[str, Any]:
    """Get a specific principle from Section C"""
    return BRSR_DATA_SKELETON.get("sectionC", {}).get(f"principle{principle_num}", {})
