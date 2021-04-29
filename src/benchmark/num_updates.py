import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from src.config.constants import BENCHMARK_DATA_FOLDER

def plot(data_df):
    sns.set_theme(style="ticks")
    fig, line_plot = plt.subplots(figsize = (16,9))
    line_plot = sns.lineplot(x='num_updates',
                            y='time',
                            hue='protocol',
                            marker='o',
                            ci=None,
                            data=data_df)
    line_plot.set_xlabel('Number of Updates', fontsize = 18)
    line_plot.set_ylabel ('Time (sec)', fontsize = 18)
    plt.tight_layout()
    sns.despine()
    plt.savefig(f'{BENCHMARK_DATA_FOLDER}/num_updates.png')

if __name__ == '__main__':
    df = pd.read_csv(f'{BENCHMARK_DATA_FOLDER}/num_updates.csv')
    plot(df)