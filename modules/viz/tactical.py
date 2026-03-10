
# modules/viz/tactical.py
from __future__ import annotations

from typing import Optional, Sequence, Iterable, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import HourLocator, DateFormatter
import seaborn as sns

# For optional baselines in hourly arrivals
from modules.config import IA1_TPH, IA2_TPH, IA1_CAX, IA2_CAX


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------
def _maybe_save(fig: plt.Figure, save_path: Optional[str]) -> None:
    """
    Save the figure to disk if a path is provided (non-transparent background).

    Parameters
    ----------
    fig : matplotlib.figure.Figure
        Figure to save.
    save_path : str or None
        If provided, location to write (PNG/other).
    """
    if save_path:
        fig.savefig(save_path, dpi=200, bbox_inches="tight", pad_inches=0.05)


def _fmt_day(d: pd.Timestamp | pd.Series | np.datetime64 | str) -> str:
    """
    Return a 'DD Mon' label (no year).
    """
    return pd.to_datetime(d).strftime("%d %b")


def _fmt_hour(dt_like: pd.Timestamp | pd.Series | np.datetime64 | str) -> str:
    """
    Return an 'HH:MM' label.
    """
    return pd.to_datetime(dt_like).strftime("%H:%M")


def _fmt_ts(dt_like: pd.Timestamp | pd.Series | np.datetime64 | str) -> str:
    """
    Return a 'DD Mon HH:MM' timestamp label (no year).
    """
    return pd.to_datetime(dt_like).strftime("%d %b %H:%M")


def _apply_datetime_axis(ax: plt.Axes, formatter: str = "%d %b %H:%M") -> None:
    """
    Apply a readable datetime axis with specific strftime format.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        Axes to format.
    formatter : str
        strftime pattern, e.g., '%d %b %H:%M' for '05 Mar 07:00'.
    """
    locator = mdates.AutoDateLocator(minticks=6, maxticks=12)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.DateFormatter(formatter))


# ---------------------------------------------------------------------
# Daily stacked arrivals / departures
# ---------------------------------------------------------------------
def plot_daily_pax_summary(
    summary_df: pd.DataFrame,
    title: str = "Daily Arrivals / Departures",
    ax: Optional[plt.Axes] = None,
    save_path: Optional[str] = None,
) -> plt.Axes:
    """
    Plot a daily stacked bar chart of Arrivals vs Departures, with totals and A/B/C ranking.

    Parameters
    ----------
    summary_df : pandas.DataFrame
        Must include: 'Schedule Date', 'A', 'D', 'Total', 'Total_k', 'Ranking', 'Date_Label'.
        'Date_Label' should already be formatted 'DD Mon' in domain.
    title : str
        Chart title.
    ax : matplotlib.axes.Axes, optional
        Target axes (created if None).
    save_path : str, optional
        If provided, save the figure to this path.

    Returns
    -------
    matplotlib.axes.Axes
        Axes with the rendered chart.
    """
    df = summary_df.copy()

    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 6))
    else:
        fig = ax.figure

    max_total = df["Total"].max() if len(df) else 0
    baseline_annot_y = 0.02 * max_total

    for i, row in df.iterrows():
        alpha = 1.0 if row["Ranking"] == "A" else 0.8 if row["Ranking"] == "B" else 0.6
        ax.bar(i, row.get("D", 0), color="#7E0C6E", alpha=alpha, label="Departures" if i == 0 else None)
        ax.bar(i, row.get("A", 0), bottom=row.get("D", 0), color="#CE007F", alpha=alpha, label="Arrivals" if i == 0 else None)
        ax.text(i, row["Total"], row["Total_k"], ha="center", va="bottom", fontweight="bold")
        ax.text(i, baseline_annot_y, row["Ranking"], ha="center", va="bottom", color="#ef9a00", fontweight="bold")

    ax.set_ylabel("Passengers")
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(df["Date_Label"], rotation=45)

    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=2, frameon=False)

    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)

    ax.set_title(title)
    plt.tight_layout()
    _maybe_save(fig, save_path)
    return ax


# ---------------------------------------------------------------------
# Hourly arrivals (stacked by sector) with optional baselines
# ---------------------------------------------------------------------
def plot_hourly_pax(
    hourly_df: pd.DataFrame,
    sectors: Sequence[str],
    title: str = "Hourly Arrivals by Sector",
    international_only: bool = False,
    show_ia2_baseline: bool = False,
    show_total_imm_baseline: bool = False,
    ax: Optional[plt.Axes] = None,
    save_path: Optional[str] = None,
) -> plt.Axes:
    """
    Plot a stacked bar chart of hourly arrivals by sector across multiple days.

    Parameters
    ----------
    hourly_df : pandas.DataFrame
        Must include: 'Date' (date), 'Hour' (int:0..23), 'Hour_Label' ('HH:MM'), and sector columns.
    sectors : sequence[str]
        Sector columns to stack (ignored if international_only=True).
    title : str
        Chart title.
    international_only : bool
        If True, plot only 'International'.
    show_ia2_baseline : bool
        If True, draw a dashed line at IA2_CAX + IA2_TPH (immigration context).
    show_total_imm_baseline : bool
        If True, draw dashed line at (IA1_CAX+IA2_CAX) + (IA1_TPH+IA2_TPH).
    ax : matplotlib.axes.Axes, optional
        Target axes (created if None).
    save_path : str, optional
        Save figure path.

    Returns
    -------
    matplotlib.axes.Axes
        Axes with the rendered chart.
    """
    df = hourly_df.copy().sort_values(["Date", "Hour"])

    if international_only:
        sectors = ["International"]

    colors = {"CTA": "#15235a", "Domestic": "#22a3b5", "International": "#27a06b"}

    if ax is None:
        fig, ax = plt.subplots(figsize=(15, 6))
    else:
        fig = ax.figure

    bottom = np.zeros(len(df))
    for s in sectors:
        vals = df.get(s, 0).to_numpy(dtype=float)
        ax.bar(df.index, vals, bottom=bottom, label=s, color=colors.get(s, "#888888"))
        bottom += vals

    # Center one tick per day, label as 'DD Mon'
    day_positions = df.groupby("Date").apply(lambda x: x.index.to_numpy().mean())
    day_labels = [ _fmt_day(d) for d in day_positions.index ]
    ax.set_xticks(day_positions)
    ax.set_xticklabels(day_labels)

    if show_ia2_baseline:
        ax.axhline(IA2_CAX + IA2_TPH, color="#CE007F", linestyle="--", alpha=0.9, label="IA2 Capacity + TPH")
    if show_total_imm_baseline:
        ax.axhline((IA1_CAX + IA2_CAX) + (IA1_TPH + IA2_TPH), color="#15235a", linestyle="--", alpha=0.9, label="IA1+IA2 Capacity + TPH")

    ax.set_ylabel("Passengers")
    ax.set_xlabel("Date")
    ax.set_title(title)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    _maybe_save(fig, save_path)
    return ax


# ---------------------------------------------------------------------
# Weekly A/B/C heatmap
# ---------------------------------------------------------------------

def plot_weekly_abc(
    daily_summary_df: pd.DataFrame,
    a_threshold: float,
    b_threshold: float,
    window_label: str = "(2-week)",
    ax: Optional[plt.Axes] = None,
    save_path: Optional[str] = None,
) -> plt.Axes:
    """
    Plot a heatmap of weekly counts of A/B/C days (7-day chunks, in sequence).
    Auto-sizes figure height and fonts based on the number of weeks to keep numbers readable.

    Parameters
    ----------
    daily_summary_df : pandas.DataFrame
        Must include: 'Schedule Date' and 'Ranking' (A/B/C).
    a_threshold : float
        Threshold used for 'A' classification (printed in title).
    b_threshold : float
        Threshold used for 'B' classification (printed in title).
    window_label : str
        Text suffix to distinguish windows (e.g., "(2-week)").
    ax : matplotlib.axes.Axes, optional
        Target axes (created if None).
    save_path : str, optional
        Path to save the figure.

    Returns
    -------
    matplotlib.axes.Axes
        Axes with the rendered heatmap.
    """
    
    df = daily_summary_df.copy().sort_values("Schedule Date").reset_index(drop=True)
    df["Week_Num"] = (np.arange(len(df)) // 7) + 1

    # Week-start labels as 'DD Mon' (aligned to weeks)
    weekly_counts = (
        df.groupby(["Week_Num", "Ranking"])["Schedule Date"]
          .count()
          .unstack(fill_value=0)
          .reindex(columns=["A", "B", "C"], fill_value=0)
    )
    week_starts = df.groupby("Week_Num")["Schedule Date"].min()
    week_labels = [pd.to_datetime(d).strftime("%d %b") for d in week_starts]

    # Figure sizing / fonts scale with number of rows
    nrows = weekly_counts.shape[0]
    # Non-square cells so tall seasons remain readable
    fig_h = max(4.0, 0.45 * nrows + 1.6)  # 0.45" per row + header margin
    if ax is None:
        fig, ax = plt.subplots(figsize=(12, fig_h))
    else:
        fig = ax.figure

    annot_fs = 11 if nrows <= 14 else 10 if nrows <= 20 else 9 if nrows <= 28 else 8
    ylab_fs  = 11 if nrows <= 20 else 10
    xtick_fs = 11

    sns.heatmap(
        weekly_counts,
        ax=ax,
        cmap=sns.color_palette(["#D6F0F4", "#22a3b5", "#15235a"], as_cmap=True),
        cbar=False,
        linewidths=0.5,
        linecolor="white",
        square=False,                      # let height grow with rows
        annot=weekly_counts.values,
        fmt="d",
        annot_kws={"fontsize": annot_fs},
    )

    # Align y-ticks with cell centers and set readable labels
    ax.set_yticks(np.arange(nrows) + 0.5)
    ax.set_yticklabels(week_labels, rotation=0, fontsize=ylab_fs)

    # Top axis labels
    ax.xaxis.set_ticks_position("top")
    ax.xaxis.set_label_position("top")
    ax.set_xticklabels(weekly_counts.columns, rotation=0, fontsize=xtick_fs)
    ax.set_ylabel("Week Commencing")

    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(axis="both", length=0)

    ax.set_title(
        "A/B/C Days per Week\n"
        f"Top 10% / Following 40% / Bottom 50% of forecasted pax {window_label}\n"
        f"A: > {int(a_threshold):,} pax | "
        f"B: {int(b_threshold):,}–{int(a_threshold):,} pax | "
        f"C: < {int(b_threshold):,} pax",
        fontsize=12,
        pad=16,
    )

    plt.tight_layout()
    _maybe_save(fig, save_path)
    return ax



# ---------------------------------------------------------------------
# Peak-day — all sectors (hourly)
# ---------------------------------------------------------------------
def plot_peak_day_all_sectors(
    hourly_df: pd.DataFrame,
    sector_columns: Sequence[str],
    peak_day: object,
    title: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    save_path: Optional[str] = None,
) -> plt.Axes:
    """
    Plot a stacked hourly arrivals chart for a single peak day across sectors.

    Parameters
    ----------
    hourly_df : pandas.DataFrame
        Must include: 'Date', 'Hour', and numeric sector columns.
    sector_columns : sequence[str]
        Sector columns to include in the stack.
    peak_day : date-like
        Day to plot.
    title : str, optional
        Chart title; default is 'Peak Arrival Day: DD Mon YYYY'.
    ax : matplotlib.axes.Axes, optional
        Target axes (created if None).
    save_path : str, optional
        Path to save the figure.

    Returns
    -------
    matplotlib.axes.Axes
        Axes with the rendered chart.
    """
    match_date = pd.to_datetime(peak_day).date()
    df = hourly_df[hourly_df["Date"] == match_date].copy().sort_values("Hour")

    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 6))
    else:
        fig = ax.figure

    colors = {"CTA": "#15235a", "Domestic": "#22a3b5", "International": "#27a06b"}

    bottom = np.zeros(len(df))
    for s in sector_columns:
        vals = df.get(s, 0).to_numpy(dtype=float)
        ax.bar(df["Hour"], vals, bottom=bottom, label=s, color=colors.get(s, "#888888"))
        bottom += vals

    ax.set_ylabel("Passengers")
    ax.set_xticks(range(24))
    ax.set_xticklabels([_fmt_hour(f"1970-01-01 {h:02d}:00") for h in range(24)], rotation=45)

    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.legend()
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5)

    label = _fmt_day(match_date)
    ax.set_title(title or f"Peak Arrival Day: {label}")
    plt.tight_layout()
    _maybe_save(fig, save_path)
    return ax


# ---------------------------------------------------------------------
# Security rolling-hour (area/lines + capacity)
# ---------------------------------------------------------------------
def plot_security_forecast(
    security_df: pd.DataFrame,
    capacity_line: float,
    title: str = "Security Forecast",
    ax: Optional[plt.Axes] = None,
    save_path: Optional[str] = None,
) -> plt.Axes:
    """
    Plot rolling-hour passenger, staff and total series with a horizontal capacity line.

    Parameters
    ----------
    security_df : pandas.DataFrame
        Must include 'Forecast DateTime', 'Rolling Hour Pax', 'Rolling Hour Staff',
        'Rolling Hour Total', and 'Date' (for tick placement).
    capacity_line : float
        Horizontal capacity reference line (pax).
    title : str
        Chart title.
    ax : matplotlib.axes.Axes, optional
        Target axes (created if None).
    save_path : str, optional
        Path to save the figure.

    Returns
    -------
    matplotlib.axes.Axes
        Axes with the rendered chart.
    """
    df = security_df.copy().sort_values("Forecast DateTime")

    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 6))
    else:
        fig = ax.figure

    ax.fill_between(df["Forecast DateTime"], df["Rolling Hour Pax"], color="#7E0C6E", alpha=0.8, label="Passengers (RH)")
    ax.plot(df["Forecast DateTime"], df["Rolling Hour Staff"], color="#15235a", linewidth=2, label="Staff (RH)")
    ax.plot(df["Forecast DateTime"], df["Rolling Hour Total"], color="#22a3b5", linewidth=2, label="Total (RH)")
    ax.axhline(capacity_line, color="#CE007F", linestyle="--", label="Capacity")

    # Mid-day tick per day, labelled 'DD Mon'
    mid_ticks = df.groupby(df["Forecast DateTime"].dt.date)["Forecast DateTime"].apply(lambda x: x.iloc[len(x) // 2])
    ax.set_xticks(mid_ticks)
    ax.set_xticklabels([_fmt_day(d) for d in mid_ticks.dt.date], rotation=45, fontsize=9)

    ax.set_ylabel("Passengers / Staff")
    ax.set_xlabel("Date")
    ax.set_title(title)

    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.legend()
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    _maybe_save(fig, save_path)
    return ax


# ---------------------------------------------------------------------
# Peak Security (single-day) with shaded window and annotation
# ---------------------------------------------------------------------
def plot_peak_security(
    security_df: pd.DataFrame,
    peak_hour_info: dict,
    capacity_line: float,
    title_prefix: str = "Peak Security",
    ax: Optional[plt.Axes] = None,
    save_path: Optional[str] = None,
) -> plt.Axes:
    """
    Plot a single day's rolling-hour security series and highlight the peak 60-minute window.

    Parameters
    ----------
    security_df : pandas.DataFrame
        Must include 'Forecast DateTime', 'Date', and rolling-hour columns for the day of interest.
    peak_hour_info : dict
        Output of domain.peak_security_hour(...):
          {'Date', 'Window Start', 'Window End', 'Pax RH', 'Staff RH', 'Total RH'}
    capacity_line : float
        Horizontal capacity reference line (pax).
    title_prefix : str
        Prefix for the title (e.g., "Peak Security (2-week)").
    ax : matplotlib.axes.Axes, optional
        Target axes (created if None).
    save_path : str, optional
        Path to save the figure.

    Returns
    -------
    matplotlib.axes.Axes
        Axes with the rendered chart.
    """
    day = pd.to_datetime(peak_hour_info["Date"]).date()
    df = security_df[security_df["Date"] == day].copy().sort_values("Forecast DateTime")

    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 6))
    else:
        fig = ax.figure

    # Continuous hour for x
    df["__hour_float"] = df["Forecast DateTime"].dt.hour + df["Forecast DateTime"].dt.minute / 60.0

    ax.plot(df["__hour_float"], df["Rolling Hour Pax"], color="#7E0C6E", linewidth=2, label="Passengers (RH)")
    ax.plot(df["__hour_float"], df["Rolling Hour Staff"], color="#15235a", linewidth=2, label="Staff (RH)")
    ax.plot(df["__hour_float"], df["Rolling Hour Total"], color="#22a3b5", linewidth=2, label="Total (RH)")

    ax.axhline(capacity_line, color="#CE007F", linestyle="--", label="Capacity")

    # Shade the peak window
    wh_start = peak_hour_info["Window Start"]
    wh_end   = peak_hour_info["Window End"]
    start_h = int(wh_start.split(":")[0]) + int(wh_start.split(":")[1]) / 60.0
    end_h   = int(wh_end.split(":")[0])   + int(wh_end.split(":")[1])   / 60.0
    ax.axvspan(start_h, end_h, color="red", alpha=0.25)

    # Annotation
    mid_h = (start_h + end_h) / 2.0
    y_max = df["Rolling Hour Total"].max()
    annotation = (
        f"Peak Hour\n"
        f"{wh_start}–{wh_end}\n"
        f"Pax RH: {peak_hour_info['Pax RH']}\n"
        f"Staff RH: {peak_hour_info['Staff RH']}\n"
        f"Total RH: {peak_hour_info['Total RH']}"
    )
    ax.text(
        mid_h, y_max * 1.05,
        annotation,
        ha="center", va="bottom", fontsize=9, fontweight="bold", color="black",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7, edgecolor="none")
    )

    # X ticks as 'HH:MM'
    ax.set_xticks(range(0, 24))
    ax.set_xticklabels([_fmt_hour(f"1970-01-01 {h:02d}:00") for h in range(24)], rotation=45)

    ax.set_ylabel("Passengers / Staff")
    ax.set_xlabel("Hour")
    label = _fmt_day(day)
    ax.set_title(f"{title_prefix}: {label}")

    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.legend()
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    _maybe_save(fig, save_path)
    return ax


# ---------------------------------------------------------------------
# Immigration — 15‑minute queue (datetime axis + IA1 shading)
# ---------------------------------------------------------------------
def plot_peak_international_immigration(
    imm_df: pd.DataFrame,
    peak_day: object,
    time_col: str,
    window_label: str = "",
    ax: Optional[plt.Axes] = None,
    save_path: Optional[str] = None,
) -> plt.Axes:
    """
    Plot the immigration queue for a single day at any slot size using a dynamic timestamp column

    Parameters
    ----------
    imm_df : pandas.DataFrame
        Must include: 'Time_col', 'Overflow', 'Capacity', 'IA1_Open', 'Date'.
    peak_day : date-like
        Day being plotted (for title).
    time_col: str
        Timestamp column name (e.g., 'Time_5', 'Time_10', 'Time_15')
    window_label : str
        Title suffix (e.g., "(2-week)" or "(Summer)").
    ax : matplotlib.axes.Axes, optional
        Target axes (created if None).
    save_path : str, optional
        Path to save figure.

    Returns
    -------
    matplotlib.axes.Axes
        Axes with the rendered chart.
    """
    x = imm_df.copy().sort_values(time_col)
    x[time_col] = pd.to_datetime(x[time_col], errors="coerce")

    peak_day = pd.to_datetime(peak_day).date()
    x = x[x["Date"] == peak_day]

    if ax is None:
        fig, ax = plt.subplots(figsize=(14, 6))
    else:
        fig = ax.figure

    # Width in days from 15‑min delta
    deltas = x[time_col].diff().dropna()
    if not deltas.empty:
        width_days = deltas.median().total_seconds() / 86400.0
        slot = deltas.iloc[0]
    else:
        #fallback (slot_minutes unknown here, assume 15 min only if no deltas)
        width_days = 15/ 1440.0
        slot = pd.Timedelta(minutes=15)

    # Bars for queue
    ax.bar(
        x[time_col].values,
        x["Overflow"].values,
        width=width_days,
        align="center",
        color="#CE007F",
        edgecolor="white",
        linewidth=0.6,
        label="Queue",
        zorder=2,
    )

    
    # Capacity lines for IA1/IA2 aligned with opening hours
    left_edges = x[time_col].to_numpy()
    rightmost = left_edges[-1] + slot if len(left_edges) else pd.NaT
    edges = np.append(left_edges, rightmost)

    cap_vals = x["Capacity"].astype(float).to_numpy()
    cap_step = np.append(cap_vals, cap_vals[-1] if len(cap_vals) else np.nan)
    ax.step(
        edges,
        cap_step,
        where="post",
        linestyle="--",
        color="#15235a",
        linewidth=1.2,
        label="Immigration Capacity",
        zorder=3,
    )


    # Shade IA1 open blocks
    
    flags = x["IA1_Open"].astype(bool).to_numpy()
    times = x[time_col].to_numpy()
    shaded_any = False
    start_i = None

    for i in range(len(flags) + 1):
        current = flags[i] if i < len(flags) else False
        if current and start_i is None:
            start_i = i
        if (not current) and start_i is not None:
            start_edge = times[start_i]
            end_edge = times[i] if i < len(times) else times[-1] + slot
            ax.axvspan(
                start_edge,
                end_edge,
                color="#7E0C6E",
                alpha=0.15,
                label="IA1 open" if not shaded_any else None,
                zorder=1,
            )
            shaded_any = True
            start_i = None


    # Timestamps as 'DD Mon HH:MM'
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b %H:%M"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.set_ylabel("Passengers")
    ax.set_xlabel("Time")
    label = _fmt_day(peak_day)
    suffix = f" {window_label}".rstrip()
    ax.set_title(f"Peak International Immigration Day{suffix} - {label}")

    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.legend()
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    _maybe_save(fig, save_path)
    return ax



def render_table_png(
    df: pd.DataFrame,
    title: str,
    save_path: Optional[str] = None,
    max_rows: int = 30,
    col_widths: Optional[Sequence[float]] = None,
) -> plt.Axes:
    """
    Render a small table as a PNG image for slides.

    Parameters
    ----------
    df : pandas.DataFrame
        Table to render. Pre-format values as strings for best control.
    title : str
        Title shown above the table (e.g., "Immigration Overflow Windows (2-week)").
    save_path : str, optional
        If provided, save the figure to this path.
    max_rows : int, default 30
        Maximum number of rows to render (truncates beyond this).
    col_widths : sequence[float], optional
        Optional relative column widths for nicer layout (e.g., [0.9, 1.3, 1.3, 0.8, 0.8]).

    Returns
    -------
    matplotlib.axes.Axes
        Axes containing the rendered table.
    """
    data = df.copy()
    if data.empty:
        data = pd.DataFrame({"No data": ["(no rows)"]})

    truncated = False
    if len(data) > max_rows:
        data = data.iloc[:max_rows].copy()
        truncated = True

    nrows, ncols = data.shape
    fig_w = 8.0
    row_h = 0.35
    top_h = 0.9
    fig_h = top_h + row_h * (nrows + 1)  # +1 header
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")

    cell_text = data.values.tolist()
    col_labels = list(data.columns)

    if col_widths is None:
        col_widths = [1.0 / max(ncols, 1)] * ncols

    table = ax.table(
        cellText=cell_text,
        colLabels=col_labels,
        loc="center",
        cellLoc="left",
        colLoc="left",
        colWidths=col_widths,
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.2)

    ax.set_title(title, fontsize=12, pad=12)
    if truncated:
        ax.text(
            0.0, -0.05,
            f"(Showing first {max_rows} rows)",
            transform=ax.transAxes, fontsize=8, ha="left", va="top", color="#555"
        )

    plt.tight_layout()
    _maybe_save(fig, save_path)
    return ax
