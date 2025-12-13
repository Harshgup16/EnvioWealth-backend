"""
Template-based extraction for BRSR data
This approach uses predefined JSON templates and asks Gemini to return only values
"""

# Section A Template - Complete company information
SECTION_A_TEMPLATE = {
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
    "businessActivities": [],
    "products": [],
    "locations": {
        "national": {"plants": "", "offices": ""},
        "international": {"plants": "", "offices": ""}
    },
    "marketsServed": {
        "national": "",
        "international": ""
    },
    "customers": [],
    "employees": {
        "permanent": {"male": 0, "female": 0, "total": 0},
        "other": {"male": 0, "female": 0, "total": 0}
    },
    "workers": {
        "permanent": {"male": 0, "female": 0, "total": 0},
        "other": {"male": 0, "female": 0, "total": 0}
    },
    "participationDiversityArea": {
        "board": {"total": 0, "female": 0, "percent": ""},
        "kmp": {"total": 0, "female": 0, "percent": ""}
    },
    "turnover": {
        "employees": {
            "permanent": {
                "male": {"current": "", "previous": ""},
                "female": {"current": "", "previous": ""},
                "total": {"current": "", "previous": ""}
            },
            "other": {
                "male": {"current": "", "previous": ""},
                "female": {"current": "", "previous": ""},
                "total": {"current": "", "previous": ""}
            }
        },
        "workers": {
            "permanent": {
                "male": {"current": "", "previous": ""},
                "female": {"current": "", "previous": ""},
                "total": {"current": "", "previous": ""}
            },
            "other": {
                "male": {"current": "", "previous": ""},
                "female": {"current": "", "previous": ""},
                "total": {"current": "", "previous": ""}
            }
        }
    },
    "holdingSubsidiary": {
        "hasParticipation": "",
        "details": []
    },
    "csrDetails": {
        "csrRegistrationNumber": "",
        "csrProjectDetails": ""
    },
    "transparencyDisclosures": {
        "complaintsStakeholders": "",
        "penaltiesEnvironment": "",
        "appealsPending": ""
    }
}

# Section B Template - Policies and Governance
SECTION_B_TEMPLATE = {
    "policies": {
        "p1": {"exists": "", "weblink": "", "approved": ""},
        "p2": {"exists": "", "weblink": "", "approved": ""},
        "p3": {"exists": "", "weblink": "", "approved": ""},
        "p4": {"exists": "", "weblink": "", "approved": ""},
        "p5": {"exists": "", "weblink": "", "approved": ""},
        "p6": {"exists": "", "weblink": "", "approved": ""},
        "p7": {"exists": "", "weblink": "", "approved": ""},
        "p8": {"exists": "", "weblink": "", "approved": ""},
        "p9": {"exists": "", "weblink": "", "approved": ""}
    },
    "governance": {
        "committeeDetails": "",
        "performanceReview": "",
        "grievanceRedressal": ""
    }
}

# Section C Templates - Split into 4 chunks for 9 principles
SECTION_C_P1_P2_TEMPLATE = {
    "principle1": {
        "essential": {
            "trainingCoverage": "",
            "codeOfConduct": "",
            "conflictDisclosure": ""
        }
    },
    "principle2": {
        "essential": {
            "productLifecycle": "",
            "sustainableSourcing": "",
            "recycledInputs": ""
        }
    }
}

SECTION_C_P3_P4_TEMPLATE = {
    "principle3": {
        "essential": {
            "employeeWellbeing": "",
            "safetyIncidents": "",
            "workingConditions": ""
        }
    },
    "principle4": {
        "essential": {
            "stakeholderMapping": "",
            "vulnerableGroups": "",
            "socialImpact": ""
        }
    }
}

SECTION_C_P5_P6_TEMPLATE = {
    "principle5": {
        "essential": {
            "humanRightsPolicy": "",
            "dueDiligence": "",
            "assessmentCoverage": ""
        }
    },
    "principle6": {
        "essential": {
            "energyConsumption": "",
            "waterConsumption": "",
            "airEmissions": "",
            "wasteManagement": ""
        }
    }
}

SECTION_C_P7_P8_P9_TEMPLATE = {
    "principle7": {
        "essential": {
            "publicPolicyAdvocacy": "",
            "antiCompetitiveConduct": ""
        }
    },
    "principle8": {
        "essential": {
            "socialImpactAssessment": "",
            "rehabilitationResettlement": ""
        }
    },
    "principle9": {
        "essential": {
            "consumerComplaints": "",
            "productRecall": "",
            "cyberSecurityBreaches": ""
        }
    }
}
