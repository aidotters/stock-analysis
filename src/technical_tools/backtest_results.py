"""BacktestResults class for storing and analyzing backtest results."""

from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


@dataclass
class Trade:
    """Represents a single trade in the backtest.

    Attributes:
        symbol: Stock symbol
        entry_date: Date when position was entered
        entry_price: Price at which position was entered
        exit_date: Date when position was exited
        exit_price: Price at which position was exited
        shares: Number of shares traded
        pnl: Profit/loss in currency
        return_pct: Return as percentage (0.10 = 10%)
        holding_days: Number of days position was held
        exit_reason: Reason for exit (e.g., "stop_loss", "take_profit")
    """

    symbol: str
    entry_date: datetime
    entry_price: float
    exit_date: datetime
    exit_price: float
    shares: int
    pnl: float
    return_pct: float
    holding_days: int
    exit_reason: str


class BacktestResults:
    """Container for backtest results with analysis methods.

    Provides methods for summarizing performance metrics, visualizing
    results, and accessing individual trade data.

    Example:
        >>> results = bt.run(symbols=["7203"], start="2023-01-01", end="2023-12-31")
        >>> print(results.summary())
        >>> results.plot().show()
        >>> trades_df = results.trades()
    """

    def __init__(
        self,
        trades: list[Trade],
        equity_curve: pd.Series,
        initial_cash: float,
    ) -> None:
        """Initialize BacktestResults.

        Args:
            trades: List of Trade objects
            equity_curve: Series with datetime index and equity values
            initial_cash: Initial cash amount
        """
        self._trades = trades
        self._equity_curve = equity_curve
        self._initial_cash = initial_cash

    @cached_property
    def _trade_returns(self) -> list[float]:
        """Get list of trade returns."""
        return [t.return_pct for t in self._trades]

    @cached_property
    def _winning_trades(self) -> list[Trade]:
        """Get list of winning trades."""
        return [t for t in self._trades if t.pnl > 0]

    @cached_property
    def _losing_trades(self) -> list[Trade]:
        """Get list of losing trades."""
        return [t for t in self._trades if t.pnl <= 0]

    def summary(self) -> dict:
        """Get summary of backtest performance metrics.

        Returns:
            Dictionary containing:
            - total_trades: Number of trades
            - win_rate: Percentage of winning trades (0-1)
            - avg_return: Average return per trade
            - max_return: Maximum return achieved
            - max_loss: Maximum loss incurred
            - profit_factor: Total profit / Total loss
            - max_drawdown: Maximum drawdown percentage
            - sharpe_ratio: Annualized Sharpe ratio
            - avg_holding_days: Average holding period in days
        """
        if not self._trades:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "avg_return": 0.0,
                "max_return": 0.0,
                "max_loss": 0.0,
                "profit_factor": 0.0,
                "max_drawdown": 0.0,
                "sharpe_ratio": 0.0,
                "avg_holding_days": 0.0,
            }

        total_trades = len(self._trades)
        win_rate = len(self._winning_trades) / total_trades if total_trades > 0 else 0.0
        avg_return = np.mean(self._trade_returns) if self._trade_returns else 0.0
        max_return = max(self._trade_returns) if self._trade_returns else 0.0
        max_loss = min(self._trade_returns) if self._trade_returns else 0.0

        # Profit factor (capped at 99.99 to avoid inf issues with JSON/visualization)
        total_profit = sum(t.pnl for t in self._winning_trades)
        total_loss = abs(sum(t.pnl for t in self._losing_trades))
        if total_loss > 0:
            profit_factor = min(total_profit / total_loss, 99.99)
        elif total_profit > 0:
            profit_factor = 99.99  # All winning trades
        else:
            profit_factor = 0.0  # No profit, no loss

        # Max drawdown
        max_drawdown = self._calculate_max_drawdown()

        # Sharpe ratio
        sharpe_ratio = self._calculate_sharpe_ratio()

        # Average holding period
        avg_holding = np.mean([t.holding_days for t in self._trades])

        return {
            "total_trades": total_trades,
            "win_rate": round(win_rate, 4),
            "avg_return": round(avg_return, 4),
            "max_return": round(max_return, 4),
            "max_loss": round(max_loss, 4),
            "profit_factor": round(profit_factor, 2),
            "max_drawdown": round(max_drawdown, 4),
            "sharpe_ratio": round(sharpe_ratio, 2),
            "avg_holding_days": round(avg_holding, 2),
        }

    def _calculate_max_drawdown(self) -> float:
        """Calculate maximum drawdown from equity curve."""
        if self._equity_curve.empty:
            return 0.0

        rolling_max = self._equity_curve.expanding().max()
        drawdown = (self._equity_curve - rolling_max) / rolling_max
        return abs(drawdown.min())

    def _calculate_sharpe_ratio(self, risk_free_rate: float = 0.0) -> float:
        """Calculate annualized Sharpe ratio.

        Args:
            risk_free_rate: Annual risk-free rate (default: 0)

        Returns:
            Annualized Sharpe ratio
        """
        if len(self._equity_curve) < 2:
            return 0.0

        daily_returns = self._equity_curve.pct_change().dropna()

        if daily_returns.empty or daily_returns.std() == 0:
            return 0.0

        # Annualize (assuming 252 trading days)
        annual_return = daily_returns.mean() * 252
        annual_std = daily_returns.std() * np.sqrt(252)

        return (annual_return - risk_free_rate) / annual_std

    def trades(self) -> pd.DataFrame:
        """Get DataFrame of all trades.

        Returns:
            DataFrame with columns: symbol, entry_date, entry_price,
            exit_date, exit_price, shares, pnl, return_pct, holding_days,
            exit_reason
        """
        if not self._trades:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "entry_date",
                    "entry_price",
                    "exit_date",
                    "exit_price",
                    "shares",
                    "pnl",
                    "return_pct",
                    "holding_days",
                    "exit_reason",
                ]
            )

        data = [
            {
                "symbol": t.symbol,
                "entry_date": t.entry_date,
                "entry_price": t.entry_price,
                "exit_date": t.exit_date,
                "exit_price": t.exit_price,
                "shares": t.shares,
                "pnl": t.pnl,
                "return_pct": t.return_pct,
                "holding_days": t.holding_days,
                "exit_reason": t.exit_reason,
            }
            for t in self._trades
        ]

        return pd.DataFrame(data)

    def plot(self) -> go.Figure:
        """Create interactive plotly chart of backtest results.

        Returns:
            Plotly Figure with equity curve and drawdown
        """
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            subplot_titles=("資産推移", "ドローダウン"),
            row_heights=[0.7, 0.3],
        )

        # Equity curve
        fig.add_trace(
            go.Scatter(
                x=self._equity_curve.index,
                y=self._equity_curve.values,
                mode="lines",
                name="資産",
                line=dict(color="blue", width=1.5),
            ),
            row=1,
            col=1,
        )

        # Drawdown
        rolling_max = self._equity_curve.expanding().max()
        drawdown = (self._equity_curve - rolling_max) / rolling_max * 100

        fig.add_trace(
            go.Scatter(
                x=drawdown.index,
                y=drawdown.values,
                mode="lines",
                name="ドローダウン",
                fill="tozeroy",
                line=dict(color="red", width=1),
            ),
            row=2,
            col=1,
        )

        # Add trade markers
        for trade in self._trades:
            # Entry marker
            entry_equity = self._equity_curve.get(trade.entry_date)
            if entry_equity is not None:
                fig.add_trace(
                    go.Scatter(
                        x=[trade.entry_date],
                        y=[entry_equity],
                        mode="markers",
                        marker=dict(color="green", size=8, symbol="triangle-up"),
                        name="買い",
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

            # Exit marker
            exit_equity = self._equity_curve.get(trade.exit_date)
            if exit_equity is not None:
                marker_color = "blue" if trade.pnl > 0 else "red"
                fig.add_trace(
                    go.Scatter(
                        x=[trade.exit_date],
                        y=[exit_equity],
                        mode="markers",
                        marker=dict(color=marker_color, size=8, symbol="triangle-down"),
                        name="売り",
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )

        fig.update_layout(
            title="バックテスト結果",
            xaxis_title="日付",
            yaxis_title="資産 (円)",
            yaxis2_title="ドローダウン (%)",
            height=600,
            showlegend=True,
        )

        return fig

    def export(
        self,
        path: str | Path,
        format: Literal["csv", "excel", "html"] | None = None,
    ) -> Path:
        """Export backtest results to file.

        Args:
            path: Output file path (extension determines format if not specified)
            format: Output format ("csv", "excel", "html"). If None, inferred from path.

        Returns:
            Path to the exported file

        Example:
            >>> results.export("report.csv")
            >>> results.export("report.xlsx", format="excel")
            >>> results.export("report.html")
        """
        path = Path(path)

        # Infer format from extension if not specified
        if format is None:
            suffix = path.suffix.lower()
            if suffix == ".csv":
                format = "csv"
            elif suffix in (".xlsx", ".xls"):
                format = "excel"
            elif suffix in (".html", ".htm"):
                format = "html"
            else:
                format = "csv"
                path = path.with_suffix(".csv")
        else:
            # Add extension if not present when format is explicitly specified
            if not path.suffix:
                if format == "csv":
                    path = path.with_suffix(".csv")
                elif format == "excel":
                    path = path.with_suffix(".xlsx")
                elif format == "html":
                    path = path.with_suffix(".html")

        trades_df = self.trades()
        summary = self.summary()

        if format == "csv":
            trades_df.to_csv(path, index=False)

        elif format == "excel":
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                # Summary sheet
                summary_df = pd.DataFrame([summary])
                summary_df.to_excel(writer, sheet_name="Summary", index=False)

                # Trades sheet
                trades_df.to_excel(writer, sheet_name="Trades", index=False)

                # By symbol sheet
                by_symbol = self.by_symbol()
                if not by_symbol.empty:
                    by_symbol.to_excel(writer, sheet_name="By Symbol", index=False)

                # Monthly returns sheet
                monthly = self.monthly_returns()
                if not monthly.empty:
                    monthly.to_excel(writer, sheet_name="Monthly Returns", index=False)

        elif format == "html":
            html_content = self._generate_html_report(trades_df, summary)
            path.write_text(html_content, encoding="utf-8")

        return path

    def _generate_html_report(
        self,
        trades_df: pd.DataFrame,
        summary: dict,
    ) -> str:
        """Generate HTML report content."""
        html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>バックテストレポート</title>
    <style>
        body { font-family: sans-serif; margin: 20px; }
        h1 { color: #333; }
        h2 { color: #666; border-bottom: 1px solid #ccc; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f5f5f5; }
        tr:nth-child(even) { background-color: #fafafa; }
        .metric { font-size: 1.2em; margin: 10px 0; }
        .positive { color: green; }
        .negative { color: red; }
    </style>
</head>
<body>
    <h1>バックテストレポート</h1>

    <h2>サマリー</h2>
    <table>
        <tr><th>指標</th><th>値</th></tr>
"""
        for key, value in summary.items():
            if isinstance(value, float):
                if "rate" in key or "return" in key or "drawdown" in key:
                    value_str = f"{value:.2%}"
                else:
                    value_str = f"{value:.2f}"
            else:
                value_str = str(value)
            html += f"        <tr><td>{key}</td><td>{value_str}</td></tr>\n"

        html += """    </table>

    <h2>取引一覧</h2>
"""
        html += trades_df.to_html(index=False, classes="trades-table")

        # By symbol analysis
        by_symbol = self.by_symbol()
        if not by_symbol.empty:
            html += """
    <h2>銘柄別パフォーマンス</h2>
"""
            html += by_symbol.to_html(index=False, classes="by-symbol-table")

        # Monthly returns
        monthly = self.monthly_returns()
        if not monthly.empty:
            html += """
    <h2>月次リターン</h2>
"""
            html += monthly.to_html(index=False, classes="monthly-table")

        html += """
</body>
</html>"""
        return html

    def by_symbol(self) -> pd.DataFrame:
        """Get performance breakdown by symbol.

        Returns:
            DataFrame with columns: symbol, trades, win_rate, avg_return, total_pnl
        """
        if not self._trades:
            return pd.DataFrame(
                columns=["symbol", "trades", "win_rate", "avg_return", "total_pnl"]
            )

        trades_df = self.trades()

        result = (
            trades_df.groupby("symbol")
            .agg(
                trades=("symbol", "count"),
                wins=("pnl", lambda x: (x > 0).sum()),
                avg_return=("return_pct", "mean"),
                total_pnl=("pnl", "sum"),
            )
            .reset_index()
        )

        result["win_rate"] = result["wins"] / result["trades"]
        result = result.drop(columns=["wins"])
        result = result.sort_values("total_pnl", ascending=False)

        return result

    def by_sector(self, sector_map: dict[str, str] | None = None) -> pd.DataFrame:
        """Get performance breakdown by sector.

        Args:
            sector_map: Optional dict mapping symbol to sector.
                If not provided, returns empty DataFrame.

        Returns:
            DataFrame with columns: sector, trades, win_rate, avg_return, total_pnl
        """
        if not self._trades or sector_map is None:
            return pd.DataFrame(
                columns=["sector", "trades", "win_rate", "avg_return", "total_pnl"]
            )

        trades_df = self.trades()
        trades_df["sector"] = trades_df["symbol"].map(sector_map)
        trades_df = trades_df.dropna(subset=["sector"])

        if trades_df.empty:
            return pd.DataFrame(
                columns=["sector", "trades", "win_rate", "avg_return", "total_pnl"]
            )

        result = (
            trades_df.groupby("sector")
            .agg(
                trades=("symbol", "count"),
                wins=("pnl", lambda x: (x > 0).sum()),
                avg_return=("return_pct", "mean"),
                total_pnl=("pnl", "sum"),
            )
            .reset_index()
        )

        result["win_rate"] = result["wins"] / result["trades"]
        result = result.drop(columns=["wins"])
        result = result.sort_values("total_pnl", ascending=False)

        return result

    def monthly_returns(self) -> pd.DataFrame:
        """Get monthly return analysis.

        Returns:
            DataFrame with columns: year_month, trades, avg_return, total_pnl
        """
        if not self._trades:
            return pd.DataFrame(
                columns=["year_month", "trades", "avg_return", "total_pnl"]
            )

        trades_df = self.trades()
        trades_df["year_month"] = pd.to_datetime(trades_df["exit_date"]).dt.to_period(
            "M"
        )

        result = (
            trades_df.groupby("year_month")
            .agg(
                trades=("symbol", "count"),
                avg_return=("return_pct", "mean"),
                total_pnl=("pnl", "sum"),
            )
            .reset_index()
        )

        result["year_month"] = result["year_month"].astype(str)
        return result

    def yearly_returns(self) -> pd.DataFrame:
        """Get yearly return analysis.

        Returns:
            DataFrame with columns: year, trades, win_rate, avg_return, total_pnl
        """
        if not self._trades:
            return pd.DataFrame(
                columns=["year", "trades", "win_rate", "avg_return", "total_pnl"]
            )

        trades_df = self.trades()
        trades_df["year"] = pd.to_datetime(trades_df["exit_date"]).dt.year

        result = (
            trades_df.groupby("year")
            .agg(
                trades=("symbol", "count"),
                wins=("pnl", lambda x: (x > 0).sum()),
                avg_return=("return_pct", "mean"),
                total_pnl=("pnl", "sum"),
            )
            .reset_index()
        )

        result["win_rate"] = result["wins"] / result["trades"]
        result = result.drop(columns=["wins"])

        return result

    def __repr__(self) -> str:
        summary = self.summary()
        return (
            f"BacktestResults(trades={summary['total_trades']}, "
            f"win_rate={summary['win_rate']:.1%}, "
            f"profit_factor={summary['profit_factor']:.2f})"
        )
