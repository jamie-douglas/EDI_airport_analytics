
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def plot_distribution(hist_df: pd.DataFrame, mode: str, output_path: str) -> None:
    """
        Plot the FastPark entry/exit distribution histogram with average and median overlays.

        This function takes the histogram dataframe produced by `entry_exit_histogram`
        and generates a clean vertical-bar plot with:
            - Bars showing entry/exit counts per bin
            - Vertical lines for: On-time (0 mins), average, and median
            - HH:MM tick labels at -180, -120, -60, 0, 60, 120, 180 mins

        Parameters
        ----------
        hist_df : pd.DataFrame
            The histogram output including:
                ['Bin Start', 'Bin End', 'Bin Midpoint',
                'Entry Count', 'Exit Count',
                'Zero Line', 'Avg Entry Line', 'Median Entry Line',
                'Avg Exit Line',  'Median Exit Line']
        mode : str
            Either "entry" or "exit" — selects which counts and which overlay lines to plot.
        output_path : str
            File path (PNG) to save the generated chart.

        Returns
        -------
        None
            Saves a PNG file to the given path.
        """

    df = hist_df.copy()

    # Series + colors per mode
    counts_col = "Entry Count" if mode == "entry" else "Exit Count"
    avg_line = df["Avg Entry Line"].iloc[0] if mode == "entry" else df["Avg Exit Line"].iloc[0]
    med_line = df["Median Entry Line"].iloc[0] if mode == "entry" else df["Median Exit Line"].iloc[0]
    bar_color = "#D2B0C8" if mode == "entry" else "#C1C1FF"

    # Bar geometry
    starts = df["Bin Start"].to_numpy()
    ends = df["Bin End"].to_numpy()
    widths = ends - starts
    counts = df[counts_col].to_numpy()

    fig, ax = plt.subplots(figsize=(12, 9), dpi=200)
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    # Small gaps between bars
    gap = 0.05
    adj = widths * (1 - gap)

    # Bars
    ax.bar(starts + widths * gap / 2, counts, width=adj, align="edge",
           color=bar_color, edgecolor="white")

    # Reference lines
    max_h = counts.max() if counts.size else 0
    if max_h > 0:
        ax.set_ylim(0, max_h * 1.05)
        ax.axvline(0, color="#ce007f", linewidth=2)
        ax.axvline(avg_line, color="#22a3b5", linewidth=2)
        ax.axvline(med_line, color="#27a06b", linewidth=2)

    # Formatter
    def hhmm(v):
        sign = "-" if v < 0 else ""
        v = abs(v)
        return f"{sign}{int(v//60):02d}:{int(v%60):02d}"

    # Line labels
    if max_h > 0:
        y = max_h * 1.01
        ax.text(0, y, "On Time (00:00)", rotation=45, ha="left", color="#ce007f")
        ax.text(avg_line, y, f"Avg ({hhmm(avg_line)})", rotation=45, ha="left", color="#22a3b5")
        ax.text(med_line, y, f"Median ({hhmm(med_line)})", rotation=45, ha="left", color="#27a06b")

    # Bar labels
    for s, w, c in zip(starts, adj, counts):
        
        if c <= 0:
                continue

        # Three-tier placement
        if c > max_h * 0.1:
            # Tall bars
            ypos = c * 0.5
        elif c > max_h * 0.02:
            # Medium-small
            ypos = c * 1.25
        else:
            # Very small numbers → lift higher
            ypos = c + (max_h * 0.03)

        ax.text(
            s + w / 2,
            ypos,
            f"{int(c)}",
            ha="center",
            rotation=90,
            color="white"
        )


    # X-axis formatting
    ax.set_xlim(-200, 200)
    ticks = np.arange(-180, 181, 60)
    ax.set_xticks(ticks)
    ax.set_xticklabels([hhmm(t) for t in ticks], rotation=45, ha="right", color="white")

    # Minimal chrome
    ax.yaxis.set_visible(False)
    for spine in ax.spines.values():
        spine.set_visible(False)

    plt.savefig(output_path, dpi=200, transparent=True)
    plt.close()
