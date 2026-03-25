#!/usr/bin/env python3
"""
FHIR to Relational Tables Transformer

Transforms FHIR R4 Bundle or NDJSON files into flattened CSV tables
ready for loading into Snowflake or other data warehouses.
"""

import json
import argparse
import csv
from pathlib import Path
from datetime import datetime
from typing import Any


def parse_reference(ref: str | None) -> str | None:
    """Extract ID from FHIR reference like 'Patient/123'"""
    if ref and '/' in ref:
        return ref.split('/')[-1]
    return ref


def get_nested(data: dict, path: str, default=None) -> Any:
    """Safely get nested dictionary value using dot notation"""
    keys = path.split('.')
    value = data
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key, default)
        elif isinstance(value, list) and key.isdigit():
            idx = int(key)
            value = value[idx] if idx < len(value) else default
        else:
            return default
        if value is None:
            return default
    return value


def extract_coding(codeable_concept: dict | None) -> dict:
    """Extract first coding from CodeableConcept"""
    if not codeable_concept:
        return {'system': None, 'code': None, 'display': None}
    
    codings = codeable_concept.get('coding', [])
    if codings:
        coding = codings[0]
        return {
            'system': coding.get('system'),
            'code': coding.get('code'),
            'display': coding.get('display')
        }
    return {'system': None, 'code': None, 'display': codeable_concept.get('text')}


def extract_identifier(identifiers: list | None, system_filter: str = None) -> str | None:
    """Extract identifier value, optionally filtering by system"""
    if not identifiers:
        return None
    
    for ident in identifiers:
        if system_filter is None or ident.get('system', '').endswith(system_filter):
            return ident.get('value')
    
    return identifiers[0].get('value') if identifiers else None


def transform_patient(resource: dict, source_file: str) -> dict:
    """Transform Patient resource to flat row"""
    names = resource.get('name', [{}])
    name = names[0] if names else {}
    
    addresses = resource.get('address', [{}])
    address = addresses[0] if addresses else {}
    
    telecoms = resource.get('telecom', [])
    phone = next((t.get('value') for t in telecoms if t.get('system') == 'phone'), None)
    email = next((t.get('value') for t in telecoms if t.get('system') == 'email'), None)
    
    identifiers = resource.get('identifier', [])
    mrn = extract_identifier(identifiers, 'MR')
    ssn = extract_identifier(identifiers, 'SSN')
    
    return {
        'id': resource.get('id'),
        'family_name': name.get('family'),
        'given_name': ' '.join(name.get('given', [])),
        'birth_date': resource.get('birthDate'),
        'gender': resource.get('gender'),
        'address_line': ' '.join(address.get('line', [])),
        'address_city': address.get('city'),
        'address_state': address.get('state'),
        'address_postal_code': address.get('postalCode'),
        'address_country': address.get('country'),
        'phone': phone,
        'email': email,
        'mrn': mrn,
        'ssn': ssn,
        'deceased': resource.get('deceasedBoolean', False),
        'deceased_datetime': resource.get('deceasedDateTime'),
        '_source_file': source_file
    }


def transform_observation(resource: dict, source_file: str) -> dict:
    """Transform Observation resource to flat row"""
    code = extract_coding(resource.get('code'))
    
    value = None
    value_unit = None
    value_string = None
    
    if 'valueQuantity' in resource:
        vq = resource['valueQuantity']
        value = vq.get('value')
        value_unit = vq.get('unit') or vq.get('code')
    elif 'valueString' in resource:
        value_string = resource['valueString']
    elif 'valueCodeableConcept' in resource:
        value_string = extract_coding(resource['valueCodeableConcept'])['display']
    
    return {
        'id': resource.get('id'),
        'patient_id': parse_reference(get_nested(resource, 'subject.reference')),
        'encounter_id': parse_reference(get_nested(resource, 'encounter.reference')),
        'status': resource.get('status'),
        'code_system': code['system'],
        'code_code': code['code'],
        'code_display': code['display'],
        'value_quantity': value,
        'value_unit': value_unit,
        'value_string': value_string,
        'effective_datetime': resource.get('effectiveDateTime'),
        'issued': resource.get('issued'),
        '_source_file': source_file
    }


def transform_condition(resource: dict, source_file: str) -> dict:
    """Transform Condition resource to flat row"""
    code = extract_coding(resource.get('code'))
    clinical_status = extract_coding(resource.get('clinicalStatus'))
    verification_status = extract_coding(resource.get('verificationStatus'))
    
    return {
        'id': resource.get('id'),
        'patient_id': parse_reference(get_nested(resource, 'subject.reference')),
        'encounter_id': parse_reference(get_nested(resource, 'encounter.reference')),
        'code_system': code['system'],
        'code_code': code['code'],
        'code_display': code['display'],
        'clinical_status': clinical_status['code'],
        'verification_status': verification_status['code'],
        'onset_datetime': resource.get('onsetDateTime'),
        'abatement_datetime': resource.get('abatementDateTime'),
        'recorded_date': resource.get('recordedDate'),
        '_source_file': source_file
    }


def transform_medication_request(resource: dict, source_file: str) -> dict:
    """Transform MedicationRequest resource to flat row"""
    med_code = None
    if 'medicationCodeableConcept' in resource:
        med_code = extract_coding(resource['medicationCodeableConcept'])
    elif 'medicationReference' in resource:
        med_code = {'system': None, 'code': parse_reference(resource['medicationReference'].get('reference')), 'display': None}
    else:
        med_code = {'system': None, 'code': None, 'display': None}
    
    dosage = resource.get('dosageInstruction', [{}])
    dosage_text = dosage[0].get('text') if dosage else None
    
    return {
        'id': resource.get('id'),
        'patient_id': parse_reference(get_nested(resource, 'subject.reference')),
        'encounter_id': parse_reference(get_nested(resource, 'encounter.reference')),
        'status': resource.get('status'),
        'intent': resource.get('intent'),
        'medication_system': med_code['system'],
        'medication_code': med_code['code'],
        'medication_display': med_code['display'],
        'authored_on': resource.get('authoredOn'),
        'dosage_text': dosage_text,
        'requester_id': parse_reference(get_nested(resource, 'requester.reference')),
        '_source_file': source_file
    }


def transform_encounter(resource: dict, source_file: str) -> dict:
    """Transform Encounter resource to flat row"""
    type_code = extract_coding(resource.get('type', [{}])[0] if resource.get('type') else None)
    period = resource.get('period', {})
    
    return {
        'id': resource.get('id'),
        'patient_id': parse_reference(get_nested(resource, 'subject.reference')),
        'status': resource.get('status'),
        'class_code': get_nested(resource, 'class.code'),
        'type_system': type_code['system'],
        'type_code': type_code['code'],
        'type_display': type_code['display'],
        'period_start': period.get('start'),
        'period_end': period.get('end'),
        'reason_code': extract_coding(get_nested(resource, 'reasonCode.0'))['code'],
        'reason_display': extract_coding(get_nested(resource, 'reasonCode.0'))['display'],
        '_source_file': source_file
    }


def transform_procedure(resource: dict, source_file: str) -> dict:
    """Transform Procedure resource to flat row"""
    code = extract_coding(resource.get('code'))
    
    return {
        'id': resource.get('id'),
        'patient_id': parse_reference(get_nested(resource, 'subject.reference')),
        'encounter_id': parse_reference(get_nested(resource, 'encounter.reference')),
        'status': resource.get('status'),
        'code_system': code['system'],
        'code_code': code['code'],
        'code_display': code['display'],
        'performed_datetime': resource.get('performedDateTime') or get_nested(resource, 'performedPeriod.start'),
        'performed_end': get_nested(resource, 'performedPeriod.end'),
        '_source_file': source_file
    }


TRANSFORMERS = {
    'Patient': transform_patient,
    'Observation': transform_observation,
    'Condition': transform_condition,
    'MedicationRequest': transform_medication_request,
    'Encounter': transform_encounter,
    'Procedure': transform_procedure,
}


def load_fhir_file(filepath: Path, format: str = 'bundle') -> list[dict]:
    """Load FHIR resources from file"""
    resources = []
    
    with open(filepath, 'r') as f:
        if format == 'ndjson':
            for line in f:
                line = line.strip()
                if line:
                    resources.append(json.loads(line))
        else:
            data = json.load(f)
            if data.get('resourceType') == 'Bundle':
                for entry in data.get('entry', []):
                    if 'resource' in entry:
                        resources.append(entry['resource'])
            else:
                resources.append(data)
    
    return resources


def transform_fhir(input_path: Path, output_dir: Path, resource_types: list[str] = None, format: str = 'bundle'):
    """Transform FHIR file to CSV tables"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    resources = load_fhir_file(input_path, format)
    source_file = input_path.name
    
    tables = {}
    
    for resource in resources:
        resource_type = resource.get('resourceType')
        
        if resource_types and resource_type not in resource_types:
            continue
        
        if resource_type not in TRANSFORMERS:
            continue
        
        transformer = TRANSFORMERS[resource_type]
        row = transformer(resource, source_file)
        
        table_name = resource_type.lower()
        if table_name not in tables:
            tables[table_name] = []
        tables[table_name].append(row)
    
    for table_name, rows in tables.items():
        if not rows:
            continue
        
        output_path = output_dir / f"{table_name}.csv"
        fieldnames = list(rows[0].keys())
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"  {table_name}: {len(rows)} rows -> {output_path}")
    
    return tables


def main():
    parser = argparse.ArgumentParser(description='Transform FHIR data to relational tables')
    parser.add_argument('input', type=Path, help='Input FHIR file (Bundle JSON or NDJSON)')
    parser.add_argument('--output-dir', '-o', type=Path, default=Path('./fhir_output'), help='Output directory for CSV files')
    parser.add_argument('--format', '-f', choices=['bundle', 'ndjson'], default='bundle', help='Input format')
    parser.add_argument('--resources', '-r', type=str, help='Comma-separated list of resource types to extract')
    
    args = parser.parse_args()
    
    resource_types = None
    if args.resources:
        resource_types = [r.strip() for r in args.resources.split(',')]
    
    print(f"Transforming FHIR data: {args.input}")
    print(f"Output directory: {args.output_dir}")
    
    tables = transform_fhir(args.input, args.output_dir, resource_types, args.format)
    
    print(f"\nTransformation complete. {len(tables)} tables created.")


if __name__ == '__main__':
    main()
