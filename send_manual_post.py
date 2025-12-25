"""
Simple test client to POST a PDF and manual Section C JSON to the backend.
Usage:
    python send_manual_post.py ../data/sample.pdf

Requires `requests`: pip install requests
"""
import sys
import json
import requests

if len(sys.argv) < 2:
    print("Usage: python send_manual_post.py <path-to-pdf>")
    sys.exit(1)

pdf_path = sys.argv[1]
url = "http://127.0.0.1:8000/api/extract"

p1 = {
    "essential": {
        "q1_percentageCoveredByTraining": {
            "boardOfDirectors": {"totalProgrammes": "4", "topicsCovered": "Yarn & Fabric", "percentageCovered": "100%"},
            "kmp": {"totalProgrammes": "4", "topicsCovered": "Yarn & Fabric", "percentageCovered": "100%"},
            "employees": {"totalProgrammes": "250", "topicsCovered": "Labour Laws", "percentageCovered": "74%"},
            "workers": {"totalProgrammes": "6701", "topicsCovered": "PACE", "percentageCovered": "85%"}
        }
    }
}

p2 = {
    "essential": {
        "q1_rdCapexInvestments": {
            "rd": {"currentFY": "5.5", "previousFY": "4.8", "improvementDetails": "Sustainable materials research"},
            "capex": {"currentFY": "12.3", "previousFY": "10.5", "improvementDetails": "Green infrastructure"}
        },
        "q2_sustainableSourcing": {"proceduresInPlace": "Yes", "percentageSustainablySourced": "65%"}
    }
}

with open(pdf_path, 'rb') as f:
    files = {'files': (pdf_path, f, 'application/pdf')}
    # Include a Section A manual payload to verify it reaches the backend
    section_a = {
        "contactName": "Auto Tester",
        "contactDesignation": "QA",
        "contactPhone": "0000000000",
        "contactEmail": "qa@example.com",
        "reportingBoundary": "Standalone",
        "employees": {
            "permanent": {"male": 150, "female": 75, "total": 225},
            "otherThanPermanent": {"male": 50, "female": 25, "total": 75}
        },
        "workers": {
            "permanent": {"male": 300, "female": 100, "total": 400},
            "otherThanPermanent": {"male": 100, "female": 50, "total": 150}
        }
    }
    p4 = {
        "essential": {
            "q1_stakeholderIdentification": "Test stakeholders: investors, employees, communities",
            "q2_stakeholderEngagement": [{"stakeholderGroup": "Investors","vulnerableMarginalized": "No","channels": "AGM, Reports","frequency": "As & when required","purpose": "Performance"}]
        },
        "leadership": {
            "q1_boardConsultation": "Board committees review stakeholder matters regularly.",
            "q2_stakeholderConsultationUsed": "Yes",
            "q2_details": {"a": "Env policy consults", "b": "Social impact consults", "c": "Supply chain consults"},
            "q3_vulnerableEngagement": [{"vulnerableGroup": "Underprivileged","concerns": "Healthcare","actionTaken": "Health camps"}]
        }
    }

    data = {
        'sectionAManualData': json.dumps(section_a),
        'sectionCP1ManualData': json.dumps(p1),
        'sectionCP4ManualData': json.dumps(p4)
    }
    print(f"Posting to {url} with file {pdf_path} and manual Section A keys: {list(data.keys())}")
    r = requests.post(url, files=files, data=data)
    print("Status:", r.status_code)
    try:
        print(r.json())
    except Exception:
        print(r.text)
