#!/usr/bin/env python3
"""
Visualize Demo Global Correlation Results
"""

import json
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Load results
with open('demo_correlation_results.json', 'r') as f:
    results = json.load(f)

# Create figure
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

# 1. Correlation strength bar chart
stations = [r['station'] for r in results]
correlations = [r['correlation'] for r in results]
colors = ['red' if c > 0 else 'blue' for c in correlations]

ax1.barh(stations, correlations, color=colors, alpha=0.7, edgecolor='black')
ax1.axvline(x=0, color='black', linewidth=1)
ax1.set_xlabel('Correlation Coefficient (r)', fontsize=12, fontweight='bold')
ax1.set_title('Global Snowfall Correlations → Eagle River, Wisconsin\n(*** = p < 0.001, all statistically significant)',
             fontsize=13, fontweight='bold')
ax1.grid(True, alpha=0.3, axis='x')

# Add values on bars
for i, (station, corr) in enumerate(zip(stations, correlations)):
    ax1.text(corr + 0.01 if corr > 0 else corr - 0.01, i,
            f'{corr:+.3f} ***',
            va='center', ha='left' if corr > 0 else 'right',
            fontsize=10, fontweight='bold')

# 2. Lag pattern visualization
lags = [r['lag_days'] for r in results]
colors_lag = ['green' if l > 3 else ('blue' if l < -3 else 'gray') for l in lags]

ax2.scatter(lags, correlations, s=300, c=colors_lag, alpha=0.7, edgecolors='black', linewidth=2)
ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
ax2.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
ax2.axhline(y=0.3, color='red', linestyle='--', linewidth=1, alpha=0.5, label='Strong correlation threshold')
ax2.axhline(y=0.15, color='orange', linestyle='--', linewidth=1, alpha=0.5, label='Moderate correlation threshold')
ax2.set_xlabel('Lag (days)', fontsize=12, fontweight='bold')
ax2.set_ylabel('Correlation Coefficient', fontsize=12, fontweight='bold')
ax2.set_title('Correlation Strength vs Lag Pattern\n(Green=Leads WI >3d, Blue=Lags WI >3d, Gray=Simultaneous)',
             fontsize=13, fontweight='bold')
ax2.grid(True, alpha=0.3)
ax2.legend(loc='upper right')

# Add labels
for r in results:
    ax2.annotate(r['station'],
                (r['lag_days'], r['correlation']),
                xytext=(10, 5), textcoords='offset points',
                fontsize=9, alpha=0.8,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.3))

# 3. Sample size vs correlation
sample_sizes = [r['sample_size'] for r in results]

ax3.scatter(sample_sizes, np.abs(correlations), s=200, c='purple', alpha=0.6, edgecolors='black', linewidth=1)
ax3.set_xlabel('Sample Size (days)', fontsize=12, fontweight='bold')
ax3.set_ylabel('Absolute Correlation |r|', fontsize=12, fontweight='bold')
ax3.set_title('Sample Size vs Correlation Strength\n(All 9000+ days = 25 years of data)',
             fontsize=13, fontweight='bold')
ax3.grid(True, alpha=0.3)

for r in results:
    ax3.annotate(r['station'].split(',')[0],
                (r['sample_size'], abs(r['correlation'])),
                xytext=(5, 5), textcoords='offset points',
                fontsize=8)

# 4. Teleconnection pathway visualization
ax4.text(0.5, 0.95, 'Teleconnection Pathways to Eagle River, Wisconsin',
        ha='center', va='top', fontsize=14, fontweight='bold',
        transform=ax4.transAxes)

y_pos = 0.85
for r in results:
    if abs(r['correlation']) > 0.1 or abs(r['lag_days']) > 5:
        # Color code by strength
        if abs(r['correlation']) > 0.3:
            color = 'darkgreen'
            strength = 'STRONG'
        elif abs(r['correlation']) > 0.15:
            color = 'orange'
            strength = 'MODERATE'
        else:
            color = 'gray'
            strength = 'WEAK'

        # Format lag
        if r['lag_days'] > 0:
            lag_arrow = f"→ ({r['lag_days']}d lead) →"
        elif r['lag_days'] < 0:
            lag_arrow = f"← ({abs(r['lag_days'])}d lag) ←"
        else:
            lag_arrow = "↔ (simultaneous) ↔"

        # Write pathway
        text = f"{r['station']:25s} {lag_arrow:20s} Wisconsin\n"
        text += f"   r={r['correlation']:+.3f} | {strength} | {r['significance']}"

        ax4.text(0.05, y_pos, text,
                transform=ax4.transAxes,
                fontsize=10, fontfamily='monospace',
                color=color,
                bbox=dict(boxstyle='round', facecolor='white', edgecolor=color, linewidth=2))

        y_pos -= 0.18

ax4.axis('off')

plt.tight_layout()
plt.savefig('snowfall_graphs/demo_global_correlations.png', dpi=300, bbox_inches='tight')
print("✓ Saved: snowfall_graphs/demo_global_correlations.png")
plt.close()

# Create a simple summary chart
fig, ax = plt.subplots(figsize=(12, 8))

# Rank by correlation strength
results_sorted = sorted(results, key=lambda x: abs(x['correlation']), reverse=True)

y_positions = range(len(results_sorted))
correlations_sorted = [r['correlation'] for r in results_sorted]
stations_sorted = [r['station'] for r in results_sorted]
lags_sorted = [r['lag_days'] for r in results_sorted]

# Create bars
bars = ax.barh(y_positions, correlations_sorted, alpha=0.7, edgecolor='black', linewidth=2)

# Color bars based on correlation strength
for i, (bar, corr) in enumerate(zip(bars, correlations_sorted)):
    if abs(corr) > 0.3:
        bar.set_color('darkgreen')
    elif abs(corr) > 0.15:
        bar.set_color('orange')
    else:
        bar.set_color('lightblue')

# Add lag info on bars
for i, (corr, lag) in enumerate(zip(correlations_sorted, lags_sorted)):
    if lag > 0:
        lag_text = f'Leads {lag}d'
    elif lag < 0:
        lag_text = f'Lags {abs(lag)}d'
    else:
        lag_text = 'Same day'

    ax.text(corr + 0.01 if corr > 0 else corr - 0.01, i,
           f'{corr:+.3f} | {lag_text}',
           va='center', ha='left' if corr > 0 else 'right',
           fontsize=11, fontweight='bold',
           bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))

ax.set_yticks(y_positions)
ax.set_yticklabels(stations_sorted, fontsize=11)
ax.set_xlabel('Correlation Coefficient (Pearson r)', fontsize=13, fontweight='bold')
ax.set_title('Global Snowfall Predictors for Eagle River, Wisconsin\n(2000-2025: 25 Years, 9000+ Days, All p<0.001)',
            fontsize=14, fontweight='bold', pad=20)
ax.axvline(x=0, color='black', linewidth=1)
ax.grid(True, alpha=0.3, axis='x')

# Legend
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor='darkgreen', label='Strong (|r| > 0.3)'),
    Patch(facecolor='orange', label='Moderate (|r| > 0.15)'),
    Patch(facecolor='lightblue', label='Weak (|r| < 0.15)')
]
ax.legend(handles=legend_elements, loc='lower right', fontsize=11)

plt.tight_layout()
plt.savefig('snowfall_graphs/demo_summary.png', dpi=300, bbox_inches='tight')
print("✓ Saved: snowfall_graphs/demo_summary.png")

print("\n✓ All visualizations complete!")
