import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path

from src.config.constants import BENCHMARK_DATA_FOLDER

benchmark_labels = {
    'num_updates': 'Number of Updates',
    'percent_updated': 'Percent of Video Updated',
    'video_length_update': 'Length of Video Updated (s)',
    'num_commits': 'Number of Updates',
    'num_aborts': 'Number of Updates'
}

def do_plot(file_name: str,
            context: str,
            x_axis_column: str,
            y_axis_column: str,
            x_label: str,
            y_label: str):
    fig_size = (16, 9)
    dpi = 300

    data_df = pd.read_csv(file_name)
    sns.set_theme(style='ticks')
    sns.set_context(context)
    # fig, line_plot = plt.subplots(figsize = fig_size, dpi = dpi)
    fig, line_plot = plt.subplots(figsize = fig_size)
    line_plot.grid(axis='y')
    line_plot = sns.lineplot(x=x_axis_column,
                            y=y_axis_column,
                            hue='protocol',
                            marker='o',
                            ci=None,
                            data=data_df)
    # line_plot.set_xlabel(x_label, fontsize = font_size)
    line_plot.set_xlabel(x_label)
    # line_plot.set_ylabel('Time (s)', fontsize = font_size)
    line_plot.set_ylabel(y_label)
    # line_plot.legend(bbox_to_anchor=(1,1), loc='upper left', title = 'Protocol', title_fontsize = font_size, fontsize = font_size)
    line_plot.legend(bbox_to_anchor=(1,1), loc='upper left', title = 'Protocol')
    sns.despine()
    plt.savefig(f'{BENCHMARK_DATA_FOLDER}/{Path(file_name).stem}_{context}.png', bbox_inches='tight', dpi=dpi)

def plot(benchmark_name: str, x_label: str):
    file_name = f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_time_final.csv'
    if not os.path.isfile(file_name):
        file_name = f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_final.csv'
    if os.path.isfile(file_name):
        do_plot(file_name, 'talk', benchmark_name, 'time', x_label, 'Time (s)')
        do_plot(file_name, 'poster', benchmark_name, 'time', x_label, 'Time (s)')

    file_name = f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_disk_final.csv'
    if os.path.isfile(file_name):
        do_plot(file_name, 'talk', benchmark_name, 'disk', x_label, 'Disk Overhead (B)')
        do_plot(file_name, 'poster', benchmark_name, 'disk', x_label, 'Disk Overhead (B)')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <plot to make> ...")
    else:
        for arg in sys.argv[1:]:
            plot(arg, benchmark_labels[arg])
