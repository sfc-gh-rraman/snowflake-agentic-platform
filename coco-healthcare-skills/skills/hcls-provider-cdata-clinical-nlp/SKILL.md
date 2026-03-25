---
name: hcls-provider-cdata-clinical-nlp
description: Extract structured information from clinical text using NLP. Use when processing clinical notes, discharge summaries, or pathology reports to extract diagnoses, medications, procedures, and other clinical entities. Triggers include clinical NLP, NER, named entity recognition, clinical notes, discharge summary, text extraction, medical NLP, unstructured data, ICD coding, medication extraction.
---

# Clinical NLP

Extract structured information from clinical text including diagnoses, medications, procedures, and clinical findings.

## When to Use This Skill

- Extracting entities from clinical notes
- Processing discharge summaries
- Automating ICD code assignment
- Extracting medication lists from text
- Structuring pathology/radiology reports

## NLP Libraries

| Library | Best For | Snowflake Compatible |
|---------|----------|---------------------|
| **spaCy + scispaCy** | Fast, production NER | Yes (UDF) |
| **medspaCy** | Clinical context detection | Yes (UDF) |
| **Amazon Comprehend Medical** | AWS managed service | Via External Function |
| **Azure Text Analytics for Health** | Azure managed service | Via External Function |
| **Snowflake Cortex** | Built-in LLM extraction | Native |

## Quick Start

### Option 1: Snowflake Cortex (Recommended)

```sql
-- Extract entities using Cortex LLM
SELECT 
    note_id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'Extract all medications, diagnoses, and procedures from this clinical note. 
         Return as JSON with keys: medications, diagnoses, procedures.
         
         Note: ' || note_text
    ) AS extracted_entities
FROM clinical_notes
LIMIT 10;

-- Structured extraction with prompt engineering
SELECT 
    note_id,
    PARSE_JSON(
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-70b',
            $$Extract clinical entities from this note. Return ONLY valid JSON:
            {
                "medications": [{"name": "", "dose": "", "frequency": ""}],
                "diagnoses": [{"condition": "", "icd10": ""}],
                "procedures": [{"name": "", "date": ""}],
                "vitals": {"bp": "", "hr": "", "temp": ""}
            }
            
            Note: $$ || note_text
        )
    ) AS entities
FROM clinical_notes;
```

### Option 2: spaCy/scispaCy UDF

```python
# scripts/clinical_ner.py
import spacy
import scispacy
from scispacy.linking import EntityLinker

nlp = spacy.load("en_core_sci_lg")
nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": "umls"})

def extract_entities(text: str) -> dict:
    doc = nlp(text)
    
    entities = {
        'problems': [],
        'treatments': [],
        'tests': [],
        'anatomy': []
    }
    
    for ent in doc.ents:
        entity_data = {
            'text': ent.text,
            'label': ent.label_,
            'start': ent.start_char,
            'end': ent.end_char,
            'umls_cui': None,
            'umls_name': None
        }
        
        if ent._.kb_ents:
            top_match = ent._.kb_ents[0]
            entity_data['umls_cui'] = top_match[0]
            entity_data['umls_score'] = top_match[1]
        
        if ent.label_ in ['DISEASE', 'PROBLEM']:
            entities['problems'].append(entity_data)
        elif ent.label_ in ['TREATMENT', 'DRUG']:
            entities['treatments'].append(entity_data)
        elif ent.label_ in ['TEST', 'PROCEDURE']:
            entities['tests'].append(entity_data)
        elif ent.label_ == 'ANATOMY':
            entities['anatomy'].append(entity_data)
    
    return entities
```

### Option 3: medspaCy for Context

```python
import medspacy
from medspacy.context import ConTextComponent
from medspacy.ner import TargetRule

nlp = medspacy.load()

target_rules = [
    TargetRule("diabetes", "PROBLEM"),
    TargetRule("hypertension", "PROBLEM"),
    TargetRule("metformin", "MEDICATION"),
    TargetRule("lisinopril", "MEDICATION"),
]
nlp.get_pipe("medspacy_target_matcher").add(target_rules)

def extract_with_context(text: str) -> list:
    doc = nlp(text)
    
    results = []
    for ent in doc.ents:
        results.append({
            'text': ent.text,
            'label': ent.label_,
            'is_negated': ent._.is_negated,
            'is_historical': ent._.is_historical,
            'is_family': ent._.is_family,
            'is_hypothetical': ent._.is_hypothetical,
            'is_uncertain': ent._.is_uncertain
        })
    
    return results
```

## Entity Types

### Standard Clinical Entities

| Entity Type | Examples |
|-------------|----------|
| PROBLEM | diabetes, hypertension, chest pain |
| MEDICATION | metformin 500mg, lisinopril |
| PROCEDURE | colonoscopy, MRI, appendectomy |
| TEST | HbA1c, CBC, BMP |
| ANATOMY | heart, left lung, abdomen |
| FINDING | elevated, normal, decreased |

### Context Attributes

| Attribute | Meaning | Example |
|-----------|---------|---------|
| is_negated | Entity is absent | "no chest pain" |
| is_historical | Past occurrence | "history of MI" |
| is_family | Family member | "mother had breast cancer" |
| is_hypothetical | Possible/future | "if patient develops fever" |
| is_uncertain | Uncertain assertion | "possible pneumonia" |

## Snowflake UDF Deployment

```sql
-- Create stage for Python packages
CREATE OR REPLACE STAGE nlp_stage;

-- Upload model files
PUT file://./en_core_sci_lg-0.5.3.tar.gz @nlp_stage;

-- Create UDF
CREATE OR REPLACE FUNCTION extract_clinical_entities(note_text VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('spacy', 'scispacy')
IMPORTS = ('@nlp_stage/en_core_sci_lg-0.5.3.tar.gz')
HANDLER = 'extract_entities'
AS
$$
import spacy
import json

nlp = None

def extract_entities(note_text):
    global nlp
    if nlp is None:
        import sys
        sys.path.insert(0, '/tmp')
        nlp = spacy.load("en_core_sci_lg")
    
    doc = nlp(note_text)
    
    entities = []
    for ent in doc.ents:
        entities.append({
            'text': ent.text,
            'label': ent.label_,
            'start': ent.start_char,
            'end': ent.end_char
        })
    
    return entities
$$;

-- Use the UDF
SELECT 
    note_id,
    extract_clinical_entities(note_text) AS entities
FROM clinical_notes;
```

## Regex-Based Extraction

For simple patterns, regex can be faster:

```sql
-- Extract medication mentions (simplified)
SELECT 
    note_id,
    REGEXP_SUBSTR_ALL(
        note_text,
        '\\b(metformin|lisinopril|atorvastatin|omeprazole|amlodipine)\\s*\\d*\\s*(mg|mcg)?\\b',
        1, 1, 'i'
    ) AS medications
FROM clinical_notes;

-- Extract vitals
SELECT 
    note_id,
    REGEXP_SUBSTR(note_text, 'BP[:\\s]*(\\d{2,3}/\\d{2,3})', 1, 1, 'e', 1) AS blood_pressure,
    REGEXP_SUBSTR(note_text, 'HR[:\\s]*(\\d{2,3})', 1, 1, 'e', 1) AS heart_rate,
    REGEXP_SUBSTR(note_text, 'Temp[:\\s]*([\\d.]+)', 1, 1, 'e', 1) AS temperature
FROM clinical_notes;

-- Extract ICD-10 code mentions
SELECT 
    note_id,
    REGEXP_SUBSTR_ALL(note_text, '[A-Z]\\d{2}(\\.\\d{1,4})?', 1, 1, 'e') AS icd_codes
FROM clinical_notes;
```

## Section Detection

Clinical notes have standard sections:

```python
SECTION_PATTERNS = {
    'chief_complaint': r'(?:CHIEF COMPLAINT|CC)[:\s]*(.+?)(?=\n[A-Z]|\Z)',
    'history_present_illness': r'(?:HISTORY OF PRESENT ILLNESS|HPI)[:\s]*(.+?)(?=\n[A-Z]|\Z)',
    'past_medical_history': r'(?:PAST MEDICAL HISTORY|PMH)[:\s]*(.+?)(?=\n[A-Z]|\Z)',
    'medications': r'(?:MEDICATIONS|MEDS)[:\s]*(.+?)(?=\n[A-Z]|\Z)',
    'allergies': r'(?:ALLERGIES)[:\s]*(.+?)(?=\n[A-Z]|\Z)',
    'physical_exam': r'(?:PHYSICAL EXAM|PE)[:\s]*(.+?)(?=\n[A-Z]|\Z)',
    'assessment_plan': r'(?:ASSESSMENT AND PLAN|A/P)[:\s]*(.+?)(?=\n[A-Z]|\Z)',
}
```

## Best Practices

1. **Preprocessing**: Normalize text (lowercase, expand abbreviations)
2. **Section-aware**: Extract entities within relevant sections
3. **Context detection**: Check negation, family history, hypothetical
4. **Validation**: Review sample of extractions manually
5. **PHI handling**: Ensure de-identification compliance

## Model Options

| Model | Size | Entities | UMLS Linking |
|-------|------|----------|--------------|
| en_core_sci_sm | 100MB | Basic | No |
| en_core_sci_md | 200MB | Standard | No |
| en_core_sci_lg | 400MB | Full | Yes |
| en_ner_bc5cdr_md | 200MB | Drugs, Diseases | No |
| en_ner_bionlp13cg_md | 200MB | Cancer genetics | No |

## Reference Files

- `references/section_patterns.md` - Note section regex
- `references/abbreviations.md` - Common clinical abbreviations
- `references/umls_semantic_types.md` - UMLS type hierarchy

## Requirements

```
spacy>=3.5.0
scispacy>=0.5.0
medspacy>=1.0.0
```

## Installation

```bash
pip install spacy scispacy medspacy

# Download models
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
```

## Evidence Grounding: PubMed CKE

Invoke `$cke-pubmed` when biomedical literature context improves NLP accuracy:

- Entity disambiguation: look up biomedical terms, drug names, or disease concepts
- Terminology validation: verify extracted ICD/SNOMED/UMLS mappings against published usage
- Prompt grounding: ground LLM prompts with PubMed evidence for more accurate clinical entity extraction

See `$cke-pubmed` for setup, query patterns, and the LLM prompt grounding SQL pattern.
