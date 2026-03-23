
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe





def plot_entry_exit_lines(dist_df: pd.DataFrame,
                          med_entry: float,
                          med_exit: float,
                          save_path: str | None = None):
    """
    Two-line Entry/Exit distribution with:
      • Early/Late shading UNDER THE CURVES ONLY, by side of 0 (no gap at 00:00).
      • Dashed 'On time' line with vertical label centered on the line.
      • Median arrows pointing up-right (~45°) BUT text is horizontal, larger, bold.
      • Legend under the chart, side-by-side, larger, bold.
      • X-axis ticks & label in white, larger, bold.
      • No figure title.
    """
    import numpy as np
    import matplotlib.pyplot as plt

    # --- Brand colours ---
    BRAND_PINK   = "#CE007F"   # Entry
    BRAND_BLUE   = "#17828F"   # Exit
    BRAND_PURPLE = "#4B0082"   # 0-line
    EARLY_SHADE  = "#FDE2E4"
    LATE_SHADE   = "#DFF7EA"

    # --- Data ---
    x  = dist_df["MinutesDiff"].to_numpy()
    yE = dist_df["EntryCount"].to_numpy()
    yX = dist_df["ExitCount"].to_numpy()

    # Ensure domain includes 0 exactly to avoid any hairline seam in shading
    if 0 < x.min() or 0 > x.max():
        # If your clamp ever excludes 0 (shouldn’t), extend to cover it
        x = np.append(x, 0.0)
        yE = np.append(yE, 0.0)
        yX = np.append(yX, 0.0)
        order = np.argsort(x)
        x, yE, yX = x[order], yE[order], yX[order]

    fig, ax = plt.subplots(figsize=(14, 6), dpi=200)
    fig.patch.set_alpha(0)         # transparent for dark slides
    ax.set_facecolor("none")

    # ---- Shading under curves by side of 0 (not full panel) ----
    early = x <= 0     # include 0 on early side to guarantee no gap
    late  = x >= 0     # include 0 on late side too (overlap at baseline is invisible)

    ax.fill_between(x[early], 0, yE[early], color=EARLY_SHADE, alpha=0.35, zorder=0)
    ax.fill_between(x[late],  0, yE[late],  color=LATE_SHADE,  alpha=0.35, zorder=0)

    ax.fill_between(x[early], 0, yX[early], color=EARLY_SHADE, alpha=0.35, zorder=0)
    ax.fill_between(x[late],  0, yX[late],  color=LATE_SHADE,  alpha=0.35, zorder=0)

    # ---- Lines ----
    ax.plot(x, yE, color=BRAND_PINK, linewidth=2.8, label="Entry", zorder=5)
    ax.plot(x, yX, color=BRAND_BLUE, linewidth=2.8, label="Exit",  zorder=5)

    # ---- On-time dashed line + centered vertical label ----
    ax.axvline(0, color=BRAND_PURPLE, linestyle=(0, (6, 6)), linewidth=2.2, zorder=6)

    ymax = max(float(yE.max()), float(yX.max()), 1.0)
    ax.annotate(
        "On time",
        xy=(0, ymax/2),  # middle of the data height
        xytext=(0, 0),
        textcoords="offset points",
        rotation=90,
        ha="center",
        va="center",
        fontsize=13,
        fontweight="bold",
        color="white",
        zorder=7
    )

    # ---- Median arrows up-right; text horizontal, bold ----
    def hhmm(v):
        sign = "-" if v < 0 else ""
        v = abs(v)
        return f"{sign}{int(v//60):02d}:{int(v%60):02d}"

    
    def interp_y(xp, X, Y):
        if xp <= X.min(): return float(Y[0])
        if xp >= X.max(): return float(Y[-1])
        return float(np.interp(xp, X, Y))

    y_entry_m = interp_y(med_entry, x, yE)
    y_exit_m  = interp_y(med_exit,  x, yX)

    dx  = (x.max() - x.min()) * 0.03
    dyE = max(yE.max(), 1.0) * 0.18
    dyX = max(yX.max(), 1.0) * 0.18

    # Entry median arrow only
    ax.annotate(
        "",
        xy=(med_entry, y_entry_m),
        xytext=(med_entry + dx, y_entry_m + dyE),
        arrowprops=dict(arrowstyle="->", color=BRAND_PINK, lw=2),
        zorder=8,
    )

    # Exit median arrow only
    ax.annotate(
        "",
        xy=(med_exit, y_exit_m),
        xytext=(med_exit + dx, y_exit_m + dyX),
        arrowprops=dict(arrowstyle="->", color=BRAND_BLUE, lw=2),
        zorder=8,
)


    # ---- X-axis: HH:MM, white, bigger, bold ----
    step  = 60
    start = int(np.floor(x.min()/step)*step)
    stop  = int(np.ceil (x.max()/step)*step)
    ticks = np.arange(start, stop+1, step)

    ax.set_xticks(ticks)
    ax.set_xticklabels([hhmm(t) for t in ticks], color="white", fontsize=12, fontweight="bold")
    ax.set_xlabel("Difference from booked (HH:MM)", color="white", fontsize=12, fontweight="bold", labelpad=8)

    # ---- Cleanup ----
    ax.yaxis.set_visible(False)
    for s in ("top", "left", "right"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color("white")

    # ---- Legend below: side-by-side, bigger, bold ----
    leg = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.12),  # below the axis
        ncol=2,
        frameon=False,
        handlelength=2.5,
        borderaxespad=0.0
    )
    for txt in leg.get_texts():
        txt.set_fontsize(12)
        txt.set_fontweight("bold")
        txt.set_color("white")

    # No title
    # ax.set_title(...)

    fig.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=200, bbox_inches="tight", transparent=True)
    return ax

# def plot_distribution(hist_df: pd.DataFrame, mode: str, output_path: str) -> None:
#     """
#         Plot the FastPark entry/exit distribution histogram with average and median overlays.

#         This function takes the histogram dataframe produced by `entry_exit_histogram`
#         and generates a clean vertical-bar plot with:
#             - Bars showing entry/exit counts per bin
#             - Vertical lines for: On-time (0 mins), average, and median
#             - HH:MM tick labels at -180, -120, -60, 0, 60, 120, 180 mins

#         Parameters
#         ----------
#         hist_df : pd.DataFrame
#             The histogram output including:
#                 ['Bin Start', 'Bin End', 'Bin Midpoint',
#                 'Entry Count', 'Exit Count',
#                 'Zero Line', 'Avg Entry Line', 'Median Entry Line',
#                 'Avg Exit Line',  'Median Exit Line']
#         mode : str
#             Either "entry" or "exit" — selects which counts and which overlay lines to plot.
#         output_path : str
#             File path (PNG) to save the generated chart.

#         Returns
#         -------
#         None
#             Saves a PNG file to the given path.
#         """

#     df = hist_df.copy()

#     # Series + colors per mode
#     counts_col = "Entry Count" if mode == "entry" else "Exit Count"
#     avg_line = df["Avg Entry Line"].iloc[0] if mode == "entry" else df["Avg Exit Line"].iloc[0]
#     med_line = df["Median Entry Line"].iloc[0] if mode == "entry" else df["Median Exit Line"].iloc[0]
#     bar_color = "#ce007f" if mode == "entry" else "#22a3b5"

#     # Bar geometry
#     starts = df["Bin Start"].to_numpy()
#     ends = df["Bin End"].to_numpy()
#     widths = ends - starts
#     counts = df[counts_col].to_numpy()

#     fig, ax = plt.subplots(figsize=(12, 9), dpi=200)
#     fig.patch.set_alpha(0)
#     ax.set_facecolor("none")

#     # Small gaps between bars
#     gap = 0.05
#     adj = widths * (1 - gap)

#     # Bars
#     ax.bar(starts + widths * gap / 2, counts, width=adj, align="edge",
#            color=bar_color, edgecolor="white")

#     # Reference lines
#     max_h = counts.max() if counts.size else 0
#     if max_h > 0:
#         ax.set_ylim(0, max_h * 1.05)
#         ax.axvline(0, color="white", linewidth=2)
#         ax.axvline(avg_line, color="#9A8B7D", linewidth=2)
#         ax.axvline(med_line, color="#27a06b", linewidth=2)

#     # Formatter
#     def hhmm(v):
#         sign = "-" if v < 0 else ""
#         v = abs(v)
#         return f"{sign}{int(v//60):02d}:{int(v%60):02d}"

#     # Line labels
#     if max_h > 0:
#         y = max_h * 1.01
#         ax.text(0, y, "On Time (00:00)", rotation=45, ha="left", color="white", fontsize=12)
#         ax.text(avg_line, y, f"Avg ({hhmm(avg_line)})", rotation=45, ha="left", color="#9A8B7D", fontsize=12)
#         ax.text(med_line, y, f"Median ({hhmm(med_line)})", rotation=45, ha="left", color="#27a06b", fontsize=12)

#     # Bar labels
#     for s, w, c in zip(starts, adj, counts):
        
#         if c <= 0:
#                 continue

#         # Three-tier placement
#         if c > max_h * 0.1:
#             # Tall bars
#             ypos = c * 0.5
#         elif c > max_h * 0.02:
#             # Medium-small
#             ypos = c * 1.25
#         else:
#             # Very small numbers → lift higher
#             ypos = c + (max_h * 0.03)

#         ax.text(
#             s + w / 2,
#             ypos,
#             f"{int(c)}",
#             ha="center",
#             color="white",
#             fontsize=11
#         )


#     # X-axis formatting
#     ax.set_xlim(-200, 200)
#     ticks = np.arange(-180, 181, 60)
#     ax.set_xticks(ticks)
#     ax.set_xticklabels([hhmm(t) for t in ticks], ha="right", color="#22a3b5", fontsize=12)

#     # Minimal chrome
#     ax.yaxis.set_visible(False)
#     for spine in ax.spines.values():
#         spine.set_visible(False)

#     plt.savefig(output_path, dpi=200, transparent=True)
#     plt.close()






# def plot_distribution_darkslide(hist_df: pd.DataFrame, mode: str, output_path: str) -> None:
#     """
#     Dark-slide version of plot_distribution using the original (working)
#     bar-label placement logic, with updated colours, bold fonts, and improved
#     reference-line labels (clean, vertical, non-overlapping).
#     """

#     df = hist_df.copy()

#     # --- Dark-slide colours (desaturated, readable) ---
#     bar_color = "#a10065" if mode == "entry" else "#17828f"

#     counts_col = "Entry Count" if mode == "entry" else "Exit Count"
#     avg_line  = df["Avg Entry Line"].iloc[0] if mode == "entry" else df["Avg Exit Line"].iloc[0]
#     med_line  = df["Median Entry Line"].iloc[0] if mode == "entry" else df["Median Exit Line"].iloc[0]

#     # --- Bar geometry ---
#     starts  = df["Bin Start"].to_numpy()
#     ends    = df["Bin End"].to_numpy()
#     widths  = ends - starts
#     counts  = df[counts_col].to_numpy()

#     fig, ax = plt.subplots(figsize=(12, 9), dpi=200)
#     fig.patch.set_alpha(0)
#     ax.set_facecolor("none")

#     # --- Bars ---
#     gap = 0.05
#     adj = widths * (1 - gap)

#     ax.bar(
#         starts + widths * gap / 2,
#         counts,
#         width=adj,
#         align="edge",
#         color=bar_color,
#         edgecolor="#dddddd",
#         linewidth=0.8,
#         zorder=10
#     )

#     max_h = counts.max() if counts.size else 0
#     ax.set_ylim(0, max_h * 1.40)

#     # --- Reference lines ---
#     ax.axvline(0,        color="white",   linewidth=4, zorder=20)
#     ax.axvline(avg_line, color="#c3b9b0", linewidth=4, zorder=20)
#     ax.axvline(med_line, color="#2bc68a", linewidth=4, zorder=20)

#     # --- Time formatter ---
#     def hhmm(v):
#         sign = "-" if v < 0 else ""
#         v = abs(v)
#         return f"{sign}{int(v//60):02d}:{int(v%60):02d}"

#     # --- Reference-line labels: vertical, pixel-offset, bottom→top ---
#     label_y = max_h * 1.32
#     pixel_offset = 16

#     def line_label(x, text, color, side="right"):
#         """
#         Places a vertical label next to the line at x, offset in *pixels*
#         (so it never touches the line regardless of data scale).
#         """
#         dx = pixel_offset if side == "right" else -pixel_offset

#         ax.annotate(
#             text,
#             xy=(x, label_y),
#             xycoords="data",
#             xytext=(dx, 0),
#             textcoords="offset points",
#             rotation=90,
#             rotation_mode="anchor",
#             va="bottom",
#             ha="left" if side == "right" else "right",
#             fontsize=14,
#             fontweight="bold",
#             color=color,
#             zorder=25,
#             clip_on=False,
#             path_effects=[pe.withStroke(linewidth=2, foreground="black", alpha=0.35)]
#         )

#     # --- Decide label sides (Avg & Median split; On-Time always left) ---
#     median_left = med_line < avg_line
#     med_side = "left" if median_left else "right"
#     avg_side = "right" if median_left else "left"

#     line_label(0,        "On-Time: 00:00",            "white",    side="left")
#     line_label(avg_line, f"Avg: {hhmm(avg_line)}",    "#c3b9b0",  side=avg_side)
#     line_label(med_line, f"Median: {hhmm(med_line)}", "#2bc68a",  side=med_side)

#     # --- ORIGINAL WORKING BAR-LABEL LOGIC (unchanged) ---
#     for s, w, c in zip(starts, adj, counts):

#         if c <= 0:
#             continue

#         # Three-tier placement (your original logic)
#         if c > max_h * 0.1:
#             ypos = c * 0.5
#         elif c > max_h * 0.02:
#             ypos = c * 1.25
#         else:
#             ypos = c + (max_h * 0.03)

#         ax.text(
#             s + w / 2,
#             ypos,
#             f"{int(c)}",
#             ha="center",
#             color="white",
#             fontsize=12,
#             fontweight="bold",
#             zorder=30,
#         )

#     # --- X-axis formatting ---
#     ticks = np.arange(-180, 181, 60)
#     ax.set_xlim(-200, 200)
#     ax.set_xticks(ticks)
#     ax.set_xticklabels([hhmm(t) for t in ticks], color="white", fontsize=14)

#     # --- Minimalist frame ---
#     ax.yaxis.set_visible(False)
#     for spine in ax.spines.values():
#         spine.set_visible(False)

#     plt.savefig(output_path, dpi=200, transparent=True)
#     plt.close()
