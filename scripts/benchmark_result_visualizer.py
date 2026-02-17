import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

HERE = Path(__file__).absolute().parent
DEFAULT_CSV_PATH = HERE / 'benchmark' / 'result.csv'


def visualize(csv_path: Path):
	df = pd.read_csv(csv_path)

	file_names = df['file_name'].unique()
	num_files = len(file_names)
	if num_files == 0:
		raise ValueError(f'No rows found in CSV: {csv_path}')

	cols = min(3, num_files)
	rows = (num_files + cols - 1) // cols

	subplot_width = 8
	subplot_height = subplot_width / 1.5
	fig_width = subplot_width * cols
	fig_height = subplot_height * rows

	fig, axes = plt.subplots(
		rows, cols,
		figsize=(fig_width, fig_height),
		dpi=150,
		squeeze=False,
	)
	axes = axes.ravel()

	for idx, file_name in enumerate(file_names):
		group = df[df['file_name'] == file_name]
		file_size = group['file_size'].iloc[0]
		file_size_mib = file_size / 1024 / 1024
		title = f'{file_name} ({file_size_mib:.0f} MiB)'

		ax = axes[idx]
		ax.set_title(title, fontsize=16, fontweight='bold')
		ax.set_xlabel('avg_size', fontsize=14)
		ax.set_ylabel('MiB/s', fontsize=14)

		for func_name, func_group in group.groupby('func'):
			func_group = func_group.sort_values('avg_size')
			ax.plot(
				func_group['avg_size'],
				func_group['mib_per_sec'],
				marker='o',
				markersize=8,
				linewidth=2,
				label=func_name,
			)

		ax.legend(fontsize=12)
		ax.set_xscale('log', base=2)

		all_avg_sizes = sorted(group['avg_size'].unique())
		ax.set_xticks(all_avg_sizes)
		ax.set_xticklabels(
			[
				f'{int(x / 1024)}K' if x < 1024 * 1024 else f'{int(x / 1024 / 1024)}M'
				for x in all_avg_sizes
			],
			fontsize=12,
		)

		ax.tick_params(axis='y', labelsize=12)
		ax.grid(True, alpha=0.3, linestyle='--')

	for idx in range(num_files, len(axes)):
		axes[idx].set_visible(False)

	plt.tight_layout()
	plt.show()


def main():
	parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--csv', type=Path, default=DEFAULT_CSV_PATH)
	args = parser.parse_args()

	visualize(args.csv)


if __name__ == '__main__':
	main()
