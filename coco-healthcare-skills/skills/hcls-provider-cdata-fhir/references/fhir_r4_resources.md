# FHIR R4 Resource Reference

Quick reference for common FHIR R4 resources and their key fields.

## Patient

Core demographic information about a patient.

```json
{
  "resourceType": "Patient",
  "id": "example",
  "identifier": [
    {
      "system": "http://hospital.example.org/MRN",
      "value": "12345"
    }
  ],
  "name": [
    {
      "family": "Smith",
      "given": ["John", "Michael"]
    }
  ],
  "gender": "male",
  "birthDate": "1970-01-25",
  "address": [
    {
      "line": ["123 Main St"],
      "city": "Boston",
      "state": "MA",
      "postalCode": "02101"
    }
  ],
  "telecom": [
    {"system": "phone", "value": "555-1234"},
    {"system": "email", "value": "john@example.com"}
  ]
}
```

## Observation

Measurements and simple assertions (vitals, labs, etc.).

```json
{
  "resourceType": "Observation",
  "id": "blood-pressure",
  "status": "final",
  "code": {
    "coding": [
      {
        "system": "http://loinc.org",
        "code": "85354-9",
        "display": "Blood pressure panel"
      }
    ]
  },
  "subject": {"reference": "Patient/example"},
  "effectiveDateTime": "2024-01-15T10:30:00Z",
  "valueQuantity": {
    "value": 120,
    "unit": "mmHg",
    "system": "http://unitsofmeasure.org",
    "code": "mm[Hg]"
  }
}
```

## Condition

Clinical conditions, problems, or diagnoses.

```json
{
  "resourceType": "Condition",
  "id": "diabetes",
  "clinicalStatus": {
    "coding": [{"code": "active"}]
  },
  "verificationStatus": {
    "coding": [{"code": "confirmed"}]
  },
  "code": {
    "coding": [
      {
        "system": "http://snomed.info/sct",
        "code": "44054006",
        "display": "Type 2 diabetes mellitus"
      }
    ]
  },
  "subject": {"reference": "Patient/example"},
  "onsetDateTime": "2020-03-15"
}
```

## MedicationRequest

Orders for medications.

```json
{
  "resourceType": "MedicationRequest",
  "id": "med-order",
  "status": "active",
  "intent": "order",
  "medicationCodeableConcept": {
    "coding": [
      {
        "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
        "code": "860975",
        "display": "Metformin 500 MG Oral Tablet"
      }
    ]
  },
  "subject": {"reference": "Patient/example"},
  "authoredOn": "2024-01-15",
  "dosageInstruction": [
    {"text": "Take one tablet twice daily with meals"}
  ]
}
```

## Encounter

Healthcare interaction (visit, admission, etc.).

```json
{
  "resourceType": "Encounter",
  "id": "office-visit",
  "status": "finished",
  "class": {
    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
    "code": "AMB",
    "display": "ambulatory"
  },
  "type": [
    {
      "coding": [
        {
          "system": "http://snomed.info/sct",
          "code": "308335008",
          "display": "Patient encounter procedure"
        }
      ]
    }
  ],
  "subject": {"reference": "Patient/example"},
  "period": {
    "start": "2024-01-15T09:00:00Z",
    "end": "2024-01-15T09:30:00Z"
  }
}
```

## Procedure

Actions performed on a patient.

```json
{
  "resourceType": "Procedure",
  "id": "colonoscopy",
  "status": "completed",
  "code": {
    "coding": [
      {
        "system": "http://snomed.info/sct",
        "code": "73761001",
        "display": "Colonoscopy"
      }
    ]
  },
  "subject": {"reference": "Patient/example"},
  "performedDateTime": "2024-01-15T10:00:00Z"
}
```

## Bundle

Container for multiple resources.

```json
{
  "resourceType": "Bundle",
  "type": "collection",
  "entry": [
    {"resource": {"resourceType": "Patient", "id": "1", ...}},
    {"resource": {"resourceType": "Observation", "id": "2", ...}}
  ]
}
```

## Key Patterns

### References
Resources reference each other using `reference` fields:
```json
"subject": {"reference": "Patient/123"}
```

### CodeableConcept
Coded values use the CodeableConcept structure:
```json
"code": {
  "coding": [
    {"system": "http://loinc.org", "code": "12345", "display": "Test Name"}
  ],
  "text": "Human-readable text"
}
```

### Period
Time ranges use the Period structure:
```json
"period": {
  "start": "2024-01-15T09:00:00Z",
  "end": "2024-01-15T10:00:00Z"
}
```

## Resources Reference

- [FHIR R4 Specification](https://hl7.org/fhir/R4/)
- [US Core Implementation Guide](https://hl7.org/fhir/us/core/)
