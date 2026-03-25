#!/usr/bin/env python3
"""
Survival Analysis Script

Performs Kaplan-Meier survival analysis with optional group comparisons.
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

try:
    from lifelines import KaplanMeierFitter
    from lifelines.statistics import logrank_test, multivariate_logrank_test
except ImportError:
    print("Please install lifelines: pip install lifelines")
    exit(1)


def load_data(input_path: Path, time_col: str, event_col: str, group_col: str = None) -> pd.DataFrame:
    """Load and validate survival data"""
    df = pd.read_csv(input_path)
    
    required_cols = [time_col, event_col]
    if group_col:
        required_cols.append(group_col)
    
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    
    df = df.dropna(subset=required_cols)
    
    print(f"Loaded {len(df)} patients")
    print(f"  Events: {df[event_col].sum()} ({100*df[event_col].mean():.1f}%)")
    print(f"  Median follow-up: {df[time_col].median():.1f}")
    
    return df


def kaplan_meier_single(df: pd.DataFrame, time_col: str, event_col: str, 
                        output_path: Path, title: str = 'Survival Curve'):
    """Generate single KM curve"""
    kmf = KaplanMeierFitter()
    kmf.fit(df[time_col], df[event_col], label='Overall')
    
    fig, ax = plt.subplots(figsize=(10, 6))
    kmf.plot_survival_function(ax=ax, ci_show=True)
    
    median = kmf.median_survival_time_
    ax.axhline(y=0.5, color='gray', linestyle='--', alpha=0.5)
    ax.axvline(x=median, color='gray', linestyle='--', alpha=0.5)
    
    ax.set_xlabel('Time')
    ax.set_ylabel('Survival Probability')
    ax.set_title(title)
    ax.set_ylim(0, 1)
    
    textstr = f'Median: {median:.1f}\nn={len(df)}, events={df[event_col].sum()}'
    ax.text(0.95, 0.95, textstr, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\nResults:")
    print(f"  Median survival: {median:.1f}")
    print(f"  Saved plot to: {output_path}")
    
    return kmf


def kaplan_meier_grouped(df: pd.DataFrame, time_col: str, event_col: str, 
                         group_col: str, output_path: Path, title: str = 'Survival by Group'):
    """Generate KM curves comparing groups with log-rank test"""
    groups = df[group_col].unique()
    print(f"\nGroups: {list(groups)}")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    kmf_dict = {}
    for group in sorted(groups):
        mask = df[group_col] == group
        n = mask.sum()
        events = df.loc[mask, event_col].sum()
        
        kmf = KaplanMeierFitter()
        kmf.fit(df.loc[mask, time_col], df.loc[mask, event_col], 
                label=f'{group} (n={n}, events={events})')
        kmf.plot_survival_function(ax=ax, ci_show=True)
        kmf_dict[group] = kmf
        
        print(f"  {group}: n={n}, events={events}, median={kmf.median_survival_time_:.1f}")
    
    if len(groups) == 2:
        g1, g2 = sorted(groups)
        results = logrank_test(
            df.loc[df[group_col] == g1, time_col],
            df.loc[df[group_col] == g2, time_col],
            df.loc[df[group_col] == g1, event_col],
            df.loc[df[group_col] == g2, event_col]
        )
        p_value = results.p_value
    else:
        results = multivariate_logrank_test(df[time_col], df[group_col], df[event_col])
        p_value = results.p_value
    
    ax.set_xlabel('Time')
    ax.set_ylabel('Survival Probability')
    ax.set_title(title)
    ax.set_ylim(0, 1)
    ax.legend(loc='lower left')
    
    p_text = f'p < 0.001' if p_value < 0.001 else f'p = {p_value:.3f}'
    ax.text(0.95, 0.95, f'Log-rank test\n{p_text}', transform=ax.transAxes, fontsize=10,
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\nLog-rank test p-value: {p_value:.4f}")
    print(f"Saved plot to: {output_path}")
    
    return kmf_dict, p_value


def main():
    parser = argparse.ArgumentParser(description='Kaplan-Meier Survival Analysis')
    parser.add_argument('--input', '-i', type=Path, required=True, help='Input CSV file')
    parser.add_argument('--time-col', '-t', required=True, help='Column name for time to event')
    parser.add_argument('--event-col', '-e', required=True, help='Column name for event indicator (0/1)')
    parser.add_argument('--group-col', '-g', help='Column name for group comparison')
    parser.add_argument('--output', '-o', type=Path, default=Path('km_plot.png'), help='Output plot path')
    parser.add_argument('--title', default='Kaplan-Meier Survival Curve', help='Plot title')
    
    args = parser.parse_args()
    
    df = load_data(args.input, args.time_col, args.event_col, args.group_col)
    
    if args.group_col:
        kaplan_meier_grouped(df, args.time_col, args.event_col, args.group_col, 
                            args.output, args.title)
    else:
        kaplan_meier_single(df, args.time_col, args.event_col, args.output, args.title)


if __name__ == '__main__':
    main()
