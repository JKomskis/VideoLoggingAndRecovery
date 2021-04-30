import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from src.config.constants import BENCHMARK_DATA_FOLDER

benchmark_labels = {
    'num_updates': 'Number of Updates',
    'num_commits': 'Number of Committed Transactions',
    'num_aborts': 'Number of Aborted Transactions'
}

def plot(benchmark_name: str, x_label: str):
    data_df = pd.read_csv(f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_time.csv')
    sns.set_theme(style="ticks")
    fig, line_plot = plt.subplots(figsize = (16,9))
    line_plot = sns.lineplot(x=benchmark_name,
                            y='time',
                            hue='protocol',
                            marker='o',
                            ci=None,
                            data=data_df)
    line_plot.set_xlabel(x_label, fontsize = 18)
    line_plot.set_ylabel('Time (s)', fontsize = 18)
    plt.tight_layout()
    sns.despine()
    plt.savefig(f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_time.png')
    data_df = pd.read_csv(f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_disk.csv')
    sns.set_theme(style="ticks")
    fig, line_plot = plt.subplots(figsize = (16,9))
    line_plot = sns.lineplot(x=benchmark_name,
                            y='disk',
                            hue='protocol',
                            marker='o',
                            ci=None,
                            data=data_df)
    line_plot.set_xlabel(x_label, fontsize = 18)
    line_plot.set_ylabel('Disk Overhead (B)', fontsize = 18)
    plt.tight_layout()
    sns.despine()
    plt.savefig(f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_disk.png')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <plot to make> ...")
    else:
        for arg in sys.argv[1:]:
            plot(arg, benchmark_labels[arg])
