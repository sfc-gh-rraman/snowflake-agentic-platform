#!/usr/bin/env python3
"""
DICOM Metadata Parser - Extract metadata from DICOM files for Snowflake loading.
"""

import os
import sys
import glob
import json
import argparse
from datetime import datetime
from pathlib import Path

import pydicom
import pandas as pd


def safe_get(ds, tag, default=None):
    """Safely get a DICOM tag value."""
    try:
        val = ds.get(tag, default)
        if val is None or val == '':
            return default
        if hasattr(val, 'value'):
            val = val.value
        if isinstance(val, bytes):
            val = val.decode('utf-8', errors='ignore')
        if isinstance(val, pydicom.valuerep.PersonName):
            return str(val)
        if isinstance(val, pydicom.multival.MultiValue):
            return list(val)
        return val
    except Exception:
        return default


def parse_dicom_file(filepath):
    """Parse a single DICOM file and extract metadata."""
    try:
        ds = pydicom.dcmread(filepath, stop_before_pixels=True)
    except Exception as e:
        print(f"Error reading {filepath}: {e}", file=sys.stderr)
        return None
    
    patient = {
        'patient_id': safe_get(ds, 'PatientID'),
        'patient_name': safe_get(ds, 'PatientName'),
        'birth_date': safe_get(ds, 'PatientBirthDate'),
        'sex': safe_get(ds, 'PatientSex'),
    }
    
    study = {
        'study_instance_uid': safe_get(ds, 'StudyInstanceUID'),
        'patient_id': safe_get(ds, 'PatientID'),
        'study_date': safe_get(ds, 'StudyDate'),
        'study_time': safe_get(ds, 'StudyTime'),
        'study_description': safe_get(ds, 'StudyDescription'),
        'accession_number': safe_get(ds, 'AccessionNumber'),
        'referring_physician': safe_get(ds, 'ReferringPhysicianName'),
        'institution_name': safe_get(ds, 'InstitutionName'),
        'study_id': safe_get(ds, 'StudyID'),
    }
    
    series = {
        'series_instance_uid': safe_get(ds, 'SeriesInstanceUID'),
        'study_instance_uid': safe_get(ds, 'StudyInstanceUID'),
        'series_number': safe_get(ds, 'SeriesNumber'),
        'series_date': safe_get(ds, 'SeriesDate'),
        'series_time': safe_get(ds, 'SeriesTime'),
        'series_description': safe_get(ds, 'SeriesDescription'),
        'modality': safe_get(ds, 'Modality'),
        'body_part_examined': safe_get(ds, 'BodyPartExamined'),
        'patient_position': safe_get(ds, 'PatientPosition'),
        'protocol_name': safe_get(ds, 'ProtocolName'),
    }
    
    instance = {
        'sop_instance_uid': safe_get(ds, 'SOPInstanceUID'),
        'series_instance_uid': safe_get(ds, 'SeriesInstanceUID'),
        'sop_class_uid': safe_get(ds, 'SOPClassUID'),
        'instance_number': safe_get(ds, 'InstanceNumber'),
        'acquisition_date': safe_get(ds, 'AcquisitionDate'),
        'acquisition_time': safe_get(ds, 'AcquisitionTime'),
        'content_date': safe_get(ds, 'ContentDate'),
        'content_time': safe_get(ds, 'ContentTime'),
        'image_type': safe_get(ds, 'ImageType'),
        'photometric_interpretation': safe_get(ds, 'PhotometricInterpretation'),
        'rows': safe_get(ds, 'Rows'),
        'columns': safe_get(ds, 'Columns'),
        'bits_allocated': safe_get(ds, 'BitsAllocated'),
        'bits_stored': safe_get(ds, 'BitsStored'),
        'pixel_spacing': safe_get(ds, 'PixelSpacing'),
        'slice_thickness': safe_get(ds, 'SliceThickness'),
        'slice_location': safe_get(ds, 'SliceLocation'),
        'window_center': safe_get(ds, 'WindowCenter'),
        'window_width': safe_get(ds, 'WindowWidth'),
        'rescale_intercept': safe_get(ds, 'RescaleIntercept'),
        'rescale_slope': safe_get(ds, 'RescaleSlope'),
        'file_path': str(filepath),
        'file_size_bytes': os.path.getsize(filepath),
        'transfer_syntax_uid': safe_get(ds, 'TransferSyntaxUID') or str(ds.file_meta.TransferSyntaxUID) if hasattr(ds, 'file_meta') else None,
    }
    
    equipment = {
        'manufacturer': safe_get(ds, 'Manufacturer'),
        'manufacturer_model_name': safe_get(ds, 'ManufacturerModelName'),
        'station_name': safe_get(ds, 'StationName'),
        'institution_name': safe_get(ds, 'InstitutionName'),
        'institutional_department_name': safe_get(ds, 'InstitutionalDepartmentName'),
        'software_versions': safe_get(ds, 'SoftwareVersions'),
        'device_serial_number': safe_get(ds, 'DeviceSerialNumber'),
    }
    
    return {
        'patient': patient,
        'study': study,
        'series': series,
        'instance': instance,
        'equipment': equipment,
    }


def parse_dicom_directory(directory, recursive=False):
    """Parse all DICOM files in a directory."""
    pattern = '**/*.dcm' if recursive else '*.dcm'
    files = list(Path(directory).glob(pattern))
    files += list(Path(directory).glob(pattern.upper()))
    
    patients = {}
    studies = {}
    series_dict = {}
    instances = []
    equipment_dict = {}
    
    for filepath in files:
        result = parse_dicom_file(filepath)
        if result is None:
            continue
        
        p = result['patient']
        if p['patient_id'] and p['patient_id'] not in patients:
            patients[p['patient_id']] = p
        
        s = result['study']
        if s['study_instance_uid'] and s['study_instance_uid'] not in studies:
            studies[s['study_instance_uid']] = s
        
        sr = result['series']
        if sr['series_instance_uid'] and sr['series_instance_uid'] not in series_dict:
            series_dict[sr['series_instance_uid']] = sr
        
        instances.append(result['instance'])
        
        e = result['equipment']
        equip_key = f"{e['manufacturer']}_{e['manufacturer_model_name']}_{e['station_name']}"
        if equip_key not in equipment_dict:
            e['equipment_id'] = equip_key
            equipment_dict[equip_key] = e
    
    return {
        'patients': list(patients.values()),
        'studies': list(studies.values()),
        'series': list(series_dict.values()),
        'instances': instances,
        'equipment': list(equipment_dict.values()),
    }


def main():
    parser = argparse.ArgumentParser(description='Parse DICOM files and extract metadata')
    parser.add_argument('path', help='Path to DICOM file or directory')
    parser.add_argument('--recursive', '-r', action='store_true', help='Recursively search directories')
    parser.add_argument('--output-dir', '-o', default='.', help='Output directory for CSV files')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv', help='Output format')
    
    args = parser.parse_args()
    
    path = Path(args.path)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if path.is_file():
        result = parse_dicom_file(path)
        if result:
            data = {
                'patients': [result['patient']],
                'studies': [result['study']],
                'series': [result['series']],
                'instances': [result['instance']],
                'equipment': [result['equipment']],
            }
    else:
        data = parse_dicom_directory(path, recursive=args.recursive)
    
    print(f"\nParsed DICOM Summary:")
    print(f"  Patients: {len(data['patients'])}")
    print(f"  Studies: {len(data['studies'])}")
    print(f"  Series: {len(data['series'])}")
    print(f"  Instances: {len(data['instances'])}")
    print(f"  Equipment: {len(data['equipment'])}")
    
    if args.format == 'csv':
        for table_name, records in data.items():
            if records:
                df = pd.DataFrame(records)
                filepath = output_dir / f"{table_name}.csv"
                df.to_csv(filepath, index=False)
                print(f"  Wrote {filepath}")
    else:
        filepath = output_dir / 'dicom_metadata.json'
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"  Wrote {filepath}")


if __name__ == '__main__':
    main()
