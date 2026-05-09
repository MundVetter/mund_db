#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def read_data(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["series"] = frame["series"].astype(str)
    frame["mode"] = frame["mode"].astype(str)
    frame["metric"] = frame["metric"].astype(str)
    frame["readers"] = frame["readers"].astype(int)
    return frame


def series_palette(series: list[str]) -> dict[str, tuple[float, float, float]]:
    palette = sns.color_palette("deep", n_colors=len(series))
    return {name: color for name, color in zip(series, palette, strict=True)}


def plot_with_band(ax, frame: pd.DataFrame, value_col: str, title: str, ylabel: str, palette: dict[str, tuple[float, float, float]]) -> None:
    sns.lineplot(
        data=frame,
        x="readers",
        y=value_col,
        hue="series",
        marker="o",
        linewidth=2.5,
        palette=palette,
        ax=ax,
    )
    for series, group in frame.groupby("series", sort=False):
        ordered = group.sort_values("readers")
        color = palette[series]
        ax.fill_between(
            ordered["readers"],
            ordered[f"{value_col}_min"],
            ordered[f"{value_col}_max"],
            color=color,
            alpha=0.16,
            linewidth=0,
        )

    ax.set_title(title)
    ax.set_xlabel("readers")
    ax.set_ylabel(ylabel)
    ax.set_xscale("log", base=2)
    ax.set_xticks(sorted(frame["readers"].unique()))
    ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    ax.grid(True, axis="y", alpha=0.25)


def plot_mixed(frame: pd.DataFrame, out: Path) -> None:
    data = frame[frame["mode"] == "mixed"].copy()
    palette = series_palette(sorted(data["series"].unique()))

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True)
    plot_with_band(
        axes[0],
        data[data["metric"] == "reads"].rename(columns={"p50": "value", "min": "value_min", "max": "value_max"}),
        "value",
        "Mixed Workload Reads",
        "reads/sec",
        palette,
    )
    plot_with_band(
        axes[1],
        data[data["metric"] == "writes"].rename(columns={"p50": "value", "min": "value_min", "max": "value_max"}),
        "value",
        "Mixed Workload Writes",
        "writes/sec",
        palette,
    )

    for ax in axes:
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(handles, labels, title="series", loc="best")

    sns.despine(fig)
    fig.suptitle("Median benchmark results, mixed workload")
    fig.savefig(out, format="svg")
    plt.close(fig)


def plot_read_only(frame: pd.DataFrame, out: Path) -> None:
    data = frame[(frame["mode"] == "read_only") & (frame["metric"] == "reads")].copy()
    palette = series_palette(sorted(data["series"].unique()))

    fig, ax = plt.subplots(1, 1, figsize=(8, 5), constrained_layout=True)
    plot_with_band(ax, data.rename(columns={"p50": "value", "min": "value_min", "max": "value_max"}), "value", "Read-Only Reads", "reads/sec", palette)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, title="series", loc="best")
    sns.despine(fig)
    fig.suptitle("Median benchmark results, read-only workload")
    fig.savefig(out, format="svg")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("bench/median_results.csv"))
    parser.add_argument("--outdir", type=Path, default=Path("docs/assets"))
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    frame = read_data(args.input)
    plot_mixed(frame, args.outdir / "benchmark-mixed.svg")
    plot_read_only(frame, args.outdir / "benchmark-read-only.svg")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
