"""Chart generation using plotly."""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

from .signals import Signal


# Color palette
COLORS = {
    "candlestick_up": "#26a69a",
    "candlestick_down": "#ef5350",
    "sma_5": "#2196F3",
    "sma_25": "#FF9800",
    "sma_75": "#9C27B0",
    "sma_200": "#795548",
    "bb_fill": "rgba(33, 150, 243, 0.1)",
    "bb_line": "rgba(33, 150, 243, 0.5)",
    "rsi_line": "#2196F3",
    "rsi_overbought": "rgba(239, 83, 80, 0.3)",
    "rsi_oversold": "rgba(38, 166, 154, 0.3)",
    "macd_line": "#2196F3",
    "macd_signal": "#FF9800",
    "macd_hist_positive": "#26a69a",
    "macd_hist_negative": "#ef5350",
    "golden_cross": "#26a69a",
    "dead_cross": "#ef5350",
}


def _get_sma_color(period: int) -> str:
    """Get color for SMA line based on period."""
    color_map = {
        5: COLORS["sma_5"],
        25: COLORS["sma_25"],
        75: COLORS["sma_75"],
        200: COLORS["sma_200"],
    }
    return color_map.get(period, "#607D8B")


def create_chart(
    df: pd.DataFrame,
    ticker: str,
    show_sma: list[int] | None = None,
    show_bb: bool = False,
    show_rsi: bool = False,
    show_macd: bool = False,
    signals: list[Signal] | None = None,
) -> go.Figure:
    """Create interactive candlestick chart with indicators.

    Args:
        df: DataFrame with OHLCV and optional indicator columns
        ticker: Stock ticker for chart title
        show_sma: List of SMA periods to display
        show_bb: Whether to show Bollinger Bands
        show_rsi: Whether to show RSI subplot
        show_macd: Whether to show MACD subplot
        signals: List of Signal objects to display as markers

    Returns:
        Plotly Figure object
    """
    # Calculate number of rows needed
    rows = 1
    if show_rsi:
        rows += 1
    if show_macd:
        rows += 1

    # Set row heights
    if rows == 1:
        row_heights = [1.0]
    elif rows == 2:
        row_heights = [0.7, 0.3]
    else:
        row_heights = [0.5, 0.25, 0.25]

    # Create subplots
    fig = make_subplots(
        rows=rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=_get_subplot_titles(show_rsi, show_macd),
    )

    # Add candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="Price",
            increasing_line_color=COLORS["candlestick_up"],
            decreasing_line_color=COLORS["candlestick_down"],
        ),
        row=1,
        col=1,
    )

    # Add SMA lines
    if show_sma:
        for period in show_sma:
            col_name = f"SMA_{period}"
            if col_name in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df[col_name],
                        name=f"SMA {period}",
                        line=dict(color=_get_sma_color(period), width=1),
                    ),
                    row=1,
                    col=1,
                )

    # Add Bollinger Bands
    if show_bb and all(c in df.columns for c in ["BB_Upper", "BB_Middle", "BB_Lower"]):
        # Upper band
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["BB_Upper"],
                name="BB Upper",
                line=dict(color=COLORS["bb_line"], width=1),
            ),
            row=1,
            col=1,
        )
        # Lower band with fill
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["BB_Lower"],
                name="BB Lower",
                line=dict(color=COLORS["bb_line"], width=1),
                fill="tonexty",
                fillcolor=COLORS["bb_fill"],
            ),
            row=1,
            col=1,
        )

    # Add signal markers
    if signals:
        _add_signal_markers(fig, signals, df)

    # Track current row for subplots
    current_row = 1

    # Add RSI subplot
    if show_rsi:
        current_row += 1
        rsi_col = [c for c in df.columns if c.startswith("RSI_")]
        if rsi_col:
            rsi_data = df[rsi_col[0]]
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=rsi_data,
                    name="RSI",
                    line=dict(color=COLORS["rsi_line"], width=1),
                ),
                row=current_row,
                col=1,
            )
            # Add overbought/oversold lines
            fig.add_hline(
                y=70,
                line_dash="dash",
                line_color="rgba(239, 83, 80, 0.5)",
                row=current_row,
                col=1,
            )
            fig.add_hline(
                y=30,
                line_dash="dash",
                line_color="rgba(38, 166, 154, 0.5)",
                row=current_row,
                col=1,
            )
            # Set RSI y-axis range
            fig.update_yaxes(range=[0, 100], row=current_row, col=1)

    # Add MACD subplot
    if show_macd:
        current_row += 1
        if all(c in df.columns for c in ["MACD", "MACD_Signal", "MACD_Hist"]):
            # MACD histogram
            colors = [
                COLORS["macd_hist_positive"] if v >= 0 else COLORS["macd_hist_negative"]
                for v in df["MACD_Hist"]
            ]
            fig.add_trace(
                go.Bar(
                    x=df.index,
                    y=df["MACD_Hist"],
                    name="MACD Hist",
                    marker_color=colors,
                ),
                row=current_row,
                col=1,
            )
            # MACD line
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df["MACD"],
                    name="MACD",
                    line=dict(color=COLORS["macd_line"], width=1),
                ),
                row=current_row,
                col=1,
            )
            # Signal line
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df["MACD_Signal"],
                    name="Signal",
                    line=dict(color=COLORS["macd_signal"], width=1),
                ),
                row=current_row,
                col=1,
            )

    # Update layout
    fig.update_layout(
        title=f"{ticker} Technical Analysis",
        xaxis_rangeslider_visible=False,
        height=600 if rows == 1 else 800,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )

    return fig


def _get_subplot_titles(show_rsi: bool, show_macd: bool) -> tuple[str, ...]:
    """Generate subplot titles based on enabled indicators."""
    titles: list[str] = [""]  # Main chart has no title
    if show_rsi:
        titles.append("RSI")
    if show_macd:
        titles.append("MACD")
    return tuple(titles)


def _add_signal_markers(
    fig: go.Figure,
    signals: list[Signal],
    df: pd.DataFrame,
) -> None:
    """Add signal markers to the chart."""
    for signal in signals:
        if signal.date not in df.index:
            continue

        marker_symbol = (
            "triangle-up" if signal.signal_type == "golden_cross" else "triangle-down"
        )
        marker_color = (
            COLORS["golden_cross"]
            if signal.signal_type == "golden_cross"
            else COLORS["dead_cross"]
        )
        label = (
            f"GC ({signal.short_period}/{signal.long_period})"
            if signal.signal_type == "golden_cross"
            else f"DC ({signal.short_period}/{signal.long_period})"
        )

        # Position marker slightly above/below the price
        offset = df["High"].max() * 0.02
        y_pos = (
            signal.price + offset
            if signal.signal_type == "golden_cross"
            else signal.price - offset
        )

        fig.add_trace(
            go.Scatter(
                x=[signal.date],
                y=[y_pos],
                mode="markers",
                name=label,
                marker=dict(
                    symbol=marker_symbol,
                    size=12,
                    color=marker_color,
                ),
                showlegend=False,
                hovertemplate=(
                    f"{signal.signal_type.replace('_', ' ').title()}<br>"
                    f"Date: %{{x}}<br>"
                    f"Price: {signal.price:.2f}<br>"
                    f"MA: {signal.short_period}/{signal.long_period}"
                    "<extra></extra>"
                ),
            ),
            row=1,
            col=1,
        )
