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
    sns.lineplot(x='num_updates',
                y='time',
                hue='protocol',
                marker='o',
                ci=None,
                data=data_df)
    sns.despine()
    plt.savefig(f'{BENCHMARK_DATA_FOLDER}/num_updates.png')

if __name__ == '__main__':
    data = [
        {'protocol': 'Logical', 'num_updates': 1, 'time': 1},
        {'protocol': 'Logical', 'num_updates': 2, 'time': 2},
        {'protocol': 'Logical', 'num_updates': 3, 'time': 5},
        {'protocol': 'Logical', 'num_updates': 10, 'time': 30},
        {'protocol': 'Hybrid', 'num_updates': 1, 'time': 2},
        {'protocol': 'Hybrid', 'num_updates': 2, 'time': 5},
        {'protocol': 'Hybrid', 'num_updates': 3, 'time': 7},
        {'protocol': 'Hybrid', 'num_updates': 10, 'time': 20},
        {'protocol': 'Logical', 'num_updates': 1, 'time': 10},
        {'protocol': 'Logical', 'num_updates': 2, 'time': 20},
        {'protocol': 'Logical', 'num_updates': 3, 'time': 50},
        {'protocol': 'Logical', 'num_updates': 10, 'time': 300},
        {'protocol': 'Hybrid', 'num_updates': 1, 'time': 20},
        {'protocol': 'Hybrid', 'num_updates': 2, 'time': 50},
        {'protocol': 'Hybrid', 'num_updates': 3, 'time': 70},
        {'protocol': 'Hybrid', 'num_updates': 10, 'time': 200}
    ]
    df = pd.DataFrame(data)
    plot(df)