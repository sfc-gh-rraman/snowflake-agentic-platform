#!/usr/bin/env python3
"""
FAERS Signal Detection

Calculate disproportionality metrics (PRR, ROR) for drug-event combinations.
"""

import argparse
import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path


def load_faers_tables(data_dir: Path) -> dict:
    """Load FAERS ASCII files"""
    tables = {}
    
    for name in ['DEMO', 'DRUG', 'REAC', 'OUTC']:
        filepath = data_dir / f"{name}*.txt"
        files = list(data_dir.glob(f"{name}*.txt"))
        if files:
            df = pd.read_csv(files[0], sep='$', low_memory=False, encoding='latin-1')
            tables[name.lower()] = df
            print(f"Loaded {name}: {len(df):,} rows")
    
    return tables


def calculate_prr(drug_event: int, drug_total: int, event_total: int, total: int) -> dict:
    """Calculate PRR and chi-square"""
    a = drug_event
    b = drug_total - drug_event
    c = event_total - drug_event
    d = total - drug_total - event_total + drug_event
    
    if b == 0 or c == 0 or d == 0:
        return {'prr': None, 'chi2': None, 'p_value': None}
    
    prr = (a / drug_total) / (c / (total - drug_total))
    
    expected = (drug_total * event_total) / total
    chi2 = ((a - expected) ** 2) / expected if expected > 0 else 0
    p_value = 1 - stats.chi2.cdf(chi2, df=1)
    
    return {'prr': prr, 'chi2': chi2, 'p_value': p_value}


def calculate_ror(drug_event: int, drug_total: int, event_total: int, total: int) -> dict:
    """Calculate ROR with 95% CI"""
    a = drug_event
    b = drug_total - drug_event
    c = event_total - drug_event
    d = total - drug_total - event_total + drug_event
    
    if b == 0 or c == 0 or d == 0 or a == 0:
        return {'ror': None, 'ror_lower': None, 'ror_upper': None}
    
    ror = (a * d) / (b * c)
    
    se_log_ror = np.sqrt(1/a + 1/b + 1/c + 1/d)
    ror_lower = np.exp(np.log(ror) - 1.96 * se_log_ror)
    ror_upper = np.exp(np.log(ror) + 1.96 * se_log_ror)
    
    return {'ror': ror, 'ror_lower': ror_lower, 'ror_upper': ror_upper}


def detect_signals(tables: dict, drug_filter: str = None, min_cases: int = 3) -> pd.DataFrame:
    """Detect signals for drug-event combinations"""
    drug_df = tables['drug']
    reac_df = tables['reac']
    
    drug_df = drug_df[drug_df['role_cod'] == 'PS'].copy()
    
    if drug_filter:
        drug_df = drug_df[drug_df['drugname'].str.upper().str.contains(drug_filter.upper(), na=False)]
    
    merged = drug_df.merge(reac_df, on='primaryid')
    
    drug_event_counts = merged.groupby(['drugname', 'pt'])['primaryid'].nunique().reset_index()
    drug_event_counts.columns = ['drugname', 'reaction', 'de_count']
    
    drug_totals = drug_df.groupby('drugname')['primaryid'].nunique().to_dict()
    event_totals = reac_df.groupby('pt')['primaryid'].nunique().to_dict()
    total = drug_df['primaryid'].nunique()
    
    results = []
    for _, row in drug_event_counts.iterrows():
        if row['de_count'] < min_cases:
            continue
        
        drug_total = drug_totals.get(row['drugname'], 0)
        event_total = event_totals.get(row['reaction'], 0)
        
        prr_result = calculate_prr(row['de_count'], drug_total, event_total, total)
        ror_result = calculate_ror(row['de_count'], drug_total, event_total, total)
        
        results.append({
            'drugname': row['drugname'],
            'reaction': row['reaction'],
            'case_count': row['de_count'],
            'drug_total': drug_total,
            'event_total': event_total,
            **prr_result,
            **ror_result
        })
    
    results_df = pd.DataFrame(results)
    
    if len(results_df) > 0:
        results_df['signal'] = (
            (results_df['prr'] >= 2) & 
            (results_df['chi2'] >= 4) & 
            (results_df['case_count'] >= 3)
        )
        results_df = results_df.sort_values('prr', ascending=False)
    
    return results_df


def main():
    parser = argparse.ArgumentParser(description='FAERS Signal Detection')
    parser.add_argument('--data-dir', '-d', type=Path, required=True, help='Directory with FAERS ASCII files')
    parser.add_argument('--drug', '-g', help='Filter by drug name (substring match)')
    parser.add_argument('--min-cases', '-m', type=int, default=3, help='Minimum case count')
    parser.add_argument('--output', '-o', type=Path, default=Path('signals.csv'), help='Output CSV')
    parser.add_argument('--signals-only', '-s', action='store_true', help='Only output signals')
    
    args = parser.parse_args()
    
    print("Loading FAERS data...")
    tables = load_faers_tables(args.data_dir)
    
    print("\nDetecting signals...")
    results = detect_signals(tables, args.drug, args.min_cases)
    
    if args.signals_only and 'signal' in results.columns:
        results = results[results['signal']]
    
    results.to_csv(args.output, index=False)
    
    print(f"\nResults saved to {args.output}")
    print(f"Total drug-event pairs: {len(results)}")
    if 'signal' in results.columns:
        print(f"Signals detected: {results['signal'].sum()}")
    
    if len(results) > 0:
        print("\nTop 10 by PRR:")
        print(results.head(10)[['drugname', 'reaction', 'case_count', 'prr', 'ror', 'signal']].to_string(index=False))


if __name__ == '__main__':
    main()
