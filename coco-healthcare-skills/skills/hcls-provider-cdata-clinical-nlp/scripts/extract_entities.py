#!/usr/bin/env python3
"""
Clinical NLP Entity Extraction

Extract medications, diagnoses, procedures from clinical text.
"""

import argparse
import json
import re
from pathlib import Path
from typing import List, Dict, Any

try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False


MEDICATION_PATTERNS = [
    r'\b(metformin|lisinopril|atorvastatin|omeprazole|amlodipine|losartan|'
    r'gabapentin|hydrochlorothiazide|levothyroxine|pantoprazole|prednisone|'
    r'albuterol|fluticasone|montelukast|duloxetine|escitalopram|sertraline|'
    r'trazodone|alprazolam|lorazepam|zolpidem|oxycodone|tramadol|ibuprofen|'
    r'acetaminophen|aspirin|warfarin|apixaban|rivaroxaban|clopidogrel|'
    r'insulin|glipizide|sitagliptin|empagliflozin|semaglutide|ozempic|'
    r'humira|enbrel|remicade|keytruda|opdivo)\b',
]

VITAL_PATTERNS = {
    'blood_pressure': r'(?:BP|Blood Pressure)[:\s]*(\d{2,3}/\d{2,3})',
    'heart_rate': r'(?:HR|Heart Rate|Pulse)[:\s]*(\d{2,3})',
    'temperature': r'(?:Temp|Temperature)[:\s]*([\d.]+)',
    'respiratory_rate': r'(?:RR|Resp(?:iratory)? Rate)[:\s]*(\d{1,2})',
    'oxygen_saturation': r'(?:SpO2|O2 Sat|Oxygen)[:\s]*(\d{2,3})%?',
    'weight': r'(?:Weight|Wt)[:\s]*([\d.]+)\s*(?:kg|lb|pounds)?',
    'height': r'(?:Height|Ht)[:\s]*([\d.]+)\s*(?:cm|in|inches)?',
}

SECTION_PATTERNS = {
    'chief_complaint': r'(?:CHIEF COMPLAINT|C\.?C\.?|Reason for Visit)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'hpi': r'(?:HISTORY OF PRESENT ILLNESS|H\.?P\.?I\.?)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'pmh': r'(?:PAST MEDICAL HISTORY|P\.?M\.?H\.?|Medical History)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'medications': r'(?:MEDICATIONS|MEDS|Current Medications)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'allergies': r'(?:ALLERGIES|DRUG ALLERGIES)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'social_history': r'(?:SOCIAL HISTORY|S\.?H\.?)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'family_history': r'(?:FAMILY HISTORY|F\.?H\.?)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'physical_exam': r'(?:PHYSICAL EXAM|P\.?E\.?|Examination)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'assessment': r'(?:ASSESSMENT|IMPRESSION|DIAGNOSIS)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
    'plan': r'(?:PLAN|TREATMENT PLAN)[:\s]*(.+?)(?=\n[A-Z]{2,}|\Z)',
}

NEGATION_PATTERNS = [
    r'\bno\s+',
    r'\bdenies\s+',
    r'\bwithout\s+',
    r'\bnegative\s+for\s+',
    r'\brules?\s+out\s+',
    r'\bnot\s+',
    r'\babsent\s+',
]


def extract_vitals(text: str) -> Dict[str, str]:
    """Extract vital signs from clinical text"""
    vitals = {}
    for vital_name, pattern in VITAL_PATTERNS.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            vitals[vital_name] = match.group(1)
    return vitals


def extract_medications_regex(text: str) -> List[Dict[str, Any]]:
    """Extract medications using regex patterns"""
    medications = []
    
    for pattern in MEDICATION_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            med_text = match.group(0)
            
            context_start = max(0, match.start() - 50)
            context = text[context_start:match.start()].lower()
            is_negated = any(re.search(neg, context) for neg in NEGATION_PATTERNS)
            
            dose_match = re.search(
                rf'{med_text}\s*(\d+(?:\.\d+)?)\s*(mg|mcg|g|ml|units?)?',
                text[match.start():match.end() + 30],
                re.IGNORECASE
            )
            
            medications.append({
                'text': med_text,
                'start': match.start(),
                'end': match.end(),
                'dose': dose_match.group(1) if dose_match else None,
                'unit': dose_match.group(2) if dose_match else None,
                'is_negated': is_negated,
            })
    
    return medications


def extract_sections(text: str) -> Dict[str, str]:
    """Extract standard clinical note sections"""
    sections = {}
    for section_name, pattern in SECTION_PATTERNS.items():
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            sections[section_name] = match.group(1).strip()
    return sections


def extract_entities_spacy(text: str, nlp) -> Dict[str, List[Dict]]:
    """Extract entities using spaCy/scispaCy"""
    doc = nlp(text)
    
    entities = {
        'problems': [],
        'treatments': [],
        'tests': [],
        'anatomy': [],
        'other': []
    }
    
    label_mapping = {
        'DISEASE': 'problems',
        'PROBLEM': 'problems',
        'CHEMICAL': 'treatments',
        'DRUG': 'treatments',
        'PROCEDURE': 'tests',
        'TEST': 'tests',
        'ANATOMY': 'anatomy',
        'ORGAN': 'anatomy',
    }
    
    for ent in doc.ents:
        entity_data = {
            'text': ent.text,
            'label': ent.label_,
            'start': ent.start_char,
            'end': ent.end_char,
        }
        
        category = label_mapping.get(ent.label_, 'other')
        entities[category].append(entity_data)
    
    return entities


def process_note(text: str, use_spacy: bool = False) -> Dict[str, Any]:
    """Process a clinical note and extract all entities"""
    result = {
        'sections': extract_sections(text),
        'vitals': extract_vitals(text),
        'medications': extract_medications_regex(text),
    }
    
    if use_spacy and SPACY_AVAILABLE:
        try:
            nlp = spacy.load("en_core_sci_lg")
            result['entities'] = extract_entities_spacy(text, nlp)
        except OSError:
            print("Warning: scispaCy model not found. Using regex only.")
    
    return result


def main():
    parser = argparse.ArgumentParser(description='Extract entities from clinical notes')
    parser.add_argument('--input', '-i', type=Path, help='Input file (text or JSON)')
    parser.add_argument('--text', '-t', type=str, help='Direct text input')
    parser.add_argument('--output', '-o', type=Path, help='Output JSON file')
    parser.add_argument('--use-spacy', action='store_true', help='Use spaCy for NER')
    
    args = parser.parse_args()
    
    if args.text:
        text = args.text
    elif args.input:
        with open(args.input, 'r') as f:
            if args.input.suffix == '.json':
                data = json.load(f)
                text = data.get('text', data.get('note_text', ''))
            else:
                text = f.read()
    else:
        text = """
        CHIEF COMPLAINT: Chest pain
        
        HISTORY OF PRESENT ILLNESS: 65 year old male with history of diabetes and 
        hypertension presents with chest pain for 2 days. Denies shortness of breath.
        No fever or cough.
        
        MEDICATIONS: Metformin 500mg BID, Lisinopril 10mg daily, Atorvastatin 40mg daily
        
        ALLERGIES: No known drug allergies
        
        VITALS: BP 145/92, HR 78, Temp 98.6, RR 16, SpO2 98%
        
        PHYSICAL EXAM: Alert and oriented. Heart regular rate and rhythm. 
        Lungs clear to auscultation bilaterally.
        
        ASSESSMENT: Chest pain, likely musculoskeletal. Hypertension uncontrolled.
        
        PLAN: 
        1. EKG and troponin to rule out cardiac cause
        2. Increase Lisinopril to 20mg daily
        3. Follow up in 1 week
        """
    
    result = process_note(text, args.use_spacy)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"Results saved to {args.output}")
    else:
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
