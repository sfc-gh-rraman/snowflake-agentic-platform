#!/usr/bin/env python3
"""
Variant Annotation Script

Annotates VCF files with ClinVar and gnomAD data.
For production use, consider VEP or snpEff for comprehensive annotation.
"""

import argparse
import gzip
from pathlib import Path
from collections import defaultdict


def parse_vcf_line(line: str) -> dict:
    """Parse a VCF data line into components"""
    fields = line.strip().split('\t')
    if len(fields) < 8:
        return None
    
    return {
        'chrom': fields[0],
        'pos': int(fields[1]),
        'id': fields[2],
        'ref': fields[3],
        'alt': fields[4],
        'qual': fields[5],
        'filter': fields[6],
        'info': fields[7],
        'rest': fields[8:] if len(fields) > 8 else []
    }


def make_variant_key(chrom: str, pos: int, ref: str, alt: str) -> str:
    """Create unique key for variant lookup"""
    chrom = chrom.replace('chr', '')
    return f"{chrom}:{pos}:{ref}:{alt}"


def load_clinvar(clinvar_path: Path) -> dict:
    """Load ClinVar annotations into memory"""
    print(f"Loading ClinVar from {clinvar_path}...")
    clinvar = {}
    
    opener = gzip.open if str(clinvar_path).endswith('.gz') else open
    
    with opener(clinvar_path, 'rt') as f:
        for line in f:
            if line.startswith('#'):
                continue
            
            variant = parse_vcf_line(line)
            if not variant:
                continue
            
            info = parse_info_field(variant['info'])
            
            for alt in variant['alt'].split(','):
                key = make_variant_key(variant['chrom'], variant['pos'], variant['ref'], alt)
                clinvar[key] = {
                    'CLNSIG': info.get('CLNSIG', ''),
                    'CLNDN': info.get('CLNDN', ''),
                    'CLNREVSTAT': info.get('CLNREVSTAT', ''),
                    'CLNVC': info.get('CLNVC', ''),
                }
    
    print(f"  Loaded {len(clinvar):,} ClinVar variants")
    return clinvar


def load_gnomad(gnomad_path: Path, max_variants: int = None) -> dict:
    """Load gnomAD allele frequencies"""
    print(f"Loading gnomAD from {gnomad_path}...")
    gnomad = {}
    count = 0
    
    opener = gzip.open if str(gnomad_path).endswith('.gz') else open
    
    with opener(gnomad_path, 'rt') as f:
        for line in f:
            if line.startswith('#'):
                continue
            
            variant = parse_vcf_line(line)
            if not variant:
                continue
            
            info = parse_info_field(variant['info'])
            
            for i, alt in enumerate(variant['alt'].split(',')):
                key = make_variant_key(variant['chrom'], variant['pos'], variant['ref'], alt)
                
                af = info.get('AF', '').split(',')
                af_value = float(af[i]) if i < len(af) and af[i] != '.' else None
                
                gnomad[key] = {
                    'AF': af_value,
                    'AF_popmax': info.get('AF_popmax'),
                    'nhomalt': info.get('nhomalt'),
                }
            
            count += 1
            if max_variants and count >= max_variants:
                break
    
    print(f"  Loaded {len(gnomad):,} gnomAD variants")
    return gnomad


def parse_info_field(info: str) -> dict:
    """Parse VCF INFO field into dictionary"""
    result = {}
    for item in info.split(';'):
        if '=' in item:
            key, value = item.split('=', 1)
            result[key] = value
        else:
            result[item] = True
    return result


def annotate_variant(variant: dict, clinvar: dict, gnomad: dict) -> dict:
    """Add annotations to a variant"""
    annotations = {}
    
    for alt in variant['alt'].split(','):
        key = make_variant_key(variant['chrom'], variant['pos'], variant['ref'], alt)
        
        if key in clinvar:
            cv = clinvar[key]
            annotations['CLNSIG'] = cv['CLNSIG']
            annotations['CLNDN'] = cv['CLNDN']
            annotations['CLNREVSTAT'] = cv['CLNREVSTAT']
        
        if key in gnomad:
            gn = gnomad[key]
            if gn['AF'] is not None:
                annotations['gnomAD_AF'] = f"{gn['AF']:.6g}"
            if gn['AF_popmax']:
                annotations['gnomAD_AF_popmax'] = gn['AF_popmax']
            if gn['nhomalt']:
                annotations['gnomAD_nhomalt'] = gn['nhomalt']
    
    return annotations


def format_info_additions(annotations: dict) -> str:
    """Format annotations as INFO field additions"""
    if not annotations:
        return ''
    return ';'.join(f"{k}={v}" for k, v in annotations.items() if v)


def annotate_vcf(input_path: Path, output_path: Path, clinvar: dict = None, gnomad: dict = None):
    """Annotate VCF file with ClinVar and gnomAD"""
    print(f"Annotating {input_path}...")
    
    opener = gzip.open if str(input_path).endswith('.gz') else open
    out_opener = gzip.open if str(output_path).endswith('.gz') else open
    
    annotated_count = 0
    total_count = 0
    
    with opener(input_path, 'rt') as fin, out_opener(output_path, 'wt') as fout:
        for line in fin:
            if line.startswith('##'):
                fout.write(line)
                continue
            
            if line.startswith('#CHROM'):
                if clinvar:
                    fout.write('##INFO=<ID=CLNSIG,Number=.,Type=String,Description="ClinVar clinical significance">\n')
                    fout.write('##INFO=<ID=CLNDN,Number=.,Type=String,Description="ClinVar disease name">\n')
                    fout.write('##INFO=<ID=CLNREVSTAT,Number=.,Type=String,Description="ClinVar review status">\n')
                if gnomad:
                    fout.write('##INFO=<ID=gnomAD_AF,Number=A,Type=Float,Description="gnomAD allele frequency">\n')
                    fout.write('##INFO=<ID=gnomAD_AF_popmax,Number=A,Type=Float,Description="gnomAD max population AF">\n')
                    fout.write('##INFO=<ID=gnomAD_nhomalt,Number=A,Type=Integer,Description="gnomAD homozygote count">\n')
                fout.write(line)
                continue
            
            variant = parse_vcf_line(line)
            if not variant:
                fout.write(line)
                continue
            
            total_count += 1
            annotations = annotate_variant(variant, clinvar or {}, gnomad or {})
            
            if annotations:
                annotated_count += 1
                info_add = format_info_additions(annotations)
                new_info = variant['info'] + ';' + info_add if variant['info'] != '.' else info_add
                
                fields = [
                    variant['chrom'],
                    str(variant['pos']),
                    variant['id'],
                    variant['ref'],
                    variant['alt'],
                    variant['qual'],
                    variant['filter'],
                    new_info
                ] + variant['rest']
                
                fout.write('\t'.join(fields) + '\n')
            else:
                fout.write(line)
    
    print(f"  Annotated {annotated_count:,} of {total_count:,} variants ({100*annotated_count/total_count:.1f}%)")
    print(f"  Output: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Annotate VCF with ClinVar and gnomAD')
    parser.add_argument('input', type=Path, help='Input VCF file')
    parser.add_argument('--output', '-o', type=Path, help='Output VCF file')
    parser.add_argument('--clinvar', type=Path, help='ClinVar VCF file')
    parser.add_argument('--gnomad', type=Path, help='gnomAD VCF file')
    parser.add_argument('--gnomad-max', type=int, help='Max gnomAD variants to load (for testing)')
    
    args = parser.parse_args()
    
    if not args.output:
        args.output = args.input.with_suffix('.annotated.vcf')
    
    clinvar = load_clinvar(args.clinvar) if args.clinvar else None
    gnomad = load_gnomad(args.gnomad, args.gnomad_max) if args.gnomad else None
    
    if not clinvar and not gnomad:
        print("Error: Provide at least one of --clinvar or --gnomad")
        return 1
    
    annotate_vcf(args.input, args.output, clinvar, gnomad)
    return 0


if __name__ == '__main__':
    exit(main())
