import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from src.config.constants import BENCHMARK_DATA_FOLDER

benchmark_labels = {
    'num_updates': ('Number of Updates'),
    'num_commits': ('Number of Committed Transactions')
}

def plot(benchmark_name: str, x_label: str, y_label: str = 'Time (s)'):
    data_df = pd.read_csv(f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}.csv')
    sns.set_theme(style="ticks")
    fig, line_plot = plt.subplots(figsize = (16,9))
    line_plot = sns.lineplot(x=benchmark_name,
                            y='time',
                            hue='protocol',
                            marker='o',
                            ci=None,
                            data=data_df)
    line_plot.set_xlabel(x_label, fontsize = 18)
    line_plot.set_ylabel (y_label, fontsize = 18)
    plt.tight_layout()
    sns.despine()
    plt.savefig(f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}.png')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <plot to make> ...")
    else:
        for arg in sys.argv[1:]:
            plot(arg, benchmark_labels[arg][0])
