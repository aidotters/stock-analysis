"""VirtualPortfolio class for tracking simulated investments."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from market_reader import DataReader
from market_pipeline.config import get_settings

from .exceptions import PortfolioError
from .screener import ScreenerFilter, StockScreener

logger = logging.getLogger(__name__)


@dataclass
class Holding:
    """Represents a portfolio holding.

    Attributes:
        symbol: Stock symbol
        shares: Number of shares
        avg_price: Average purchase price
        purchased_at: Date of first purchase
    """

    symbol: str
    shares: int
    avg_price: float
    purchased_at: str  # ISO format string


@dataclass
class Transaction:
    """Represents a portfolio transaction.

    Attributes:
        symbol: Stock symbol
        transaction_type: "buy" or "sell"
        shares: Number of shares
        price: Price per share
        timestamp: Transaction timestamp
    """

    symbol: str
    transaction_type: Literal["buy", "sell"]
    shares: int
    price: float
    timestamp: str  # ISO format string


class VirtualPortfolio:
    """Virtual portfolio for tracking simulated investments.

    Allows creating hypothetical portfolios, tracking performance,
    and analyzing returns without actual trading.

    Example:
        >>> vp = VirtualPortfolio("my_strategy_2025")
        >>> vp.buy("7203", shares=100, price=2500)
        >>> print(vp.summary())
        >>> vp.plot().show()

    Attributes:
        name: Portfolio name
    """

    def __init__(
        self,
        name: str,
        portfolio_dir: Path | None = None,
    ) -> None:
        """Initialize VirtualPortfolio.

        Args:
            name: Portfolio name (used for file persistence)
            portfolio_dir: Directory for portfolio files
                (default: data/portfolios/)
        """
        self.name = name

        if portfolio_dir is None:
            settings = get_settings()
            self._portfolio_dir = Path(settings.paths.data_dir) / "portfolios"
        else:
            self._portfolio_dir = Path(portfolio_dir)

        self._portfolio_dir.mkdir(parents=True, exist_ok=True)
        self._portfolio_path = self._portfolio_dir / f"{name}.json"

        self._holdings: list[Holding] = []
        self._transactions: list[Transaction] = []
        self._created_at: str = datetime.now().isoformat()

        self._reader = DataReader()

        # Load existing portfolio if exists
        if self._portfolio_path.exists():
            self._load()

    def _load(self) -> None:
        """Load portfolio from JSON file."""
        try:
            data = json.loads(self._portfolio_path.read_text())
            self._created_at = data.get("created_at", datetime.now().isoformat())
            self._holdings = [Holding(**h) for h in data.get("holdings", [])]
            self._transactions = [
                Transaction(**t) for t in data.get("transactions", [])
            ]
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Error loading portfolio {self.name}: {e}")
            self._holdings = []
            self._transactions = []

    def _save(self) -> None:
        """Save portfolio to JSON file."""
        data = {
            "name": self.name,
            "created_at": self._created_at,
            "holdings": [asdict(h) for h in self._holdings],
            "transactions": [asdict(t) for t in self._transactions],
        }
        self._portfolio_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))

    def buy(
        self,
        symbol: str,
        shares: int | None = None,
        amount: float | None = None,
        price: float | None = None,
    ) -> "VirtualPortfolio":
        """Buy shares of a stock.

        Either shares or amount must be specified. If price is not provided,
        the current market price is used.

        Args:
            symbol: Stock symbol
            shares: Number of shares to buy (optional if amount is provided)
            amount: Amount in yen to invest (optional if shares is provided)
            price: Price per share (optional, uses market price if not provided)

        Returns:
            Self for method chaining

        Raises:
            PortfolioError: If neither shares nor amount is specified
        """
        if shares is None and amount is None:
            raise PortfolioError(
                "Either 'shares' or 'amount' must be specified for buy()"
            )

        # Get price if not provided
        if price is None:
            price = self._get_current_price(symbol)

        # Calculate shares from amount if needed
        if shares is None and amount is not None:
            shares = int(amount // price)

        # At this point, shares must be set
        if shares is None or shares <= 0:
            raise PortfolioError("Number of shares must be positive")

        # Update or add holding
        existing = self._find_holding(symbol)
        timestamp = datetime.now().isoformat()

        if existing:
            # Calculate new average price
            total_cost = existing.avg_price * existing.shares + price * int(shares)
            total_shares = existing.shares + int(shares)
            existing.avg_price = total_cost / total_shares
            existing.shares = total_shares
        else:
            self._holdings.append(
                Holding(
                    symbol=symbol,
                    shares=shares,
                    avg_price=price,
                    purchased_at=timestamp,
                )
            )

        # Record transaction
        self._transactions.append(
            Transaction(
                symbol=symbol,
                transaction_type="buy",
                shares=shares,
                price=price,
                timestamp=timestamp,
            )
        )

        self._save()
        return self

    def sell(
        self,
        symbol: str,
        shares: int,
        price: float | None = None,
    ) -> "VirtualPortfolio":
        """Sell shares of a stock.

        Args:
            symbol: Stock symbol
            shares: Number of shares to sell
            price: Price per share (optional, uses market price if not provided)

        Returns:
            Self for method chaining

        Raises:
            PortfolioError: If not enough shares owned or symbol not found
        """
        holding = self._find_holding(symbol)

        if holding is None:
            raise PortfolioError(f"No holding found for {symbol}")

        if holding.shares < shares:
            raise PortfolioError(
                f"Cannot sell {shares} shares of {symbol}, only {holding.shares} owned"
            )

        if price is None:
            price = self._get_current_price(symbol)

        # Update holding
        holding.shares -= shares

        # Remove if no shares left
        if holding.shares == 0:
            self._holdings.remove(holding)

        # Record transaction
        self._transactions.append(
            Transaction(
                symbol=symbol,
                transaction_type="sell",
                shares=shares,
                price=price,
                timestamp=datetime.now().isoformat(),
            )
        )

        self._save()
        return self

    def sell_all(self, symbol: str, price: float | None = None) -> "VirtualPortfolio":
        """Sell all shares of a stock.

        Args:
            symbol: Stock symbol
            price: Price per share (optional, uses market price if not provided)

        Returns:
            Self for method chaining
        """
        holding = self._find_holding(symbol)

        if holding is None:
            raise PortfolioError(f"No holding found for {symbol}")

        return self.sell(symbol, shares=holding.shares, price=price)

    def summary(self) -> dict:
        """Get portfolio summary.

        Returns:
            Dictionary containing:
            - total_investment: Total amount invested
            - current_value: Current market value
            - total_pnl: Total profit/loss
            - return_pct: Return percentage
        """
        if not self._holdings:
            return {
                "total_investment": 0.0,
                "current_value": 0.0,
                "total_pnl": 0.0,
                "return_pct": 0.0,
            }

        total_investment = 0.0
        current_value = 0.0

        for holding in self._holdings:
            investment = holding.avg_price * holding.shares
            total_investment += investment

            try:
                current_price = self._get_current_price(holding.symbol)
                current_value += current_price * holding.shares
            except Exception as e:
                logger.warning(f"Error getting price for {holding.symbol}: {e}")
                current_value += investment  # Use investment as fallback

        total_pnl = current_value - total_investment
        return_pct = total_pnl / total_investment if total_investment > 0 else 0.0

        return {
            "total_investment": round(total_investment, 0),
            "current_value": round(current_value, 0),
            "total_pnl": round(total_pnl, 0),
            "return_pct": round(return_pct, 4),
        }

    def holdings(self) -> pd.DataFrame:
        """Get DataFrame of current holdings.

        Returns:
            DataFrame with columns: symbol, shares, avg_price, current_price, pnl
        """
        if not self._holdings:
            return pd.DataFrame(
                columns=["symbol", "shares", "avg_price", "current_price", "pnl"]
            )

        data = []
        for holding in self._holdings:
            try:
                current_price = self._get_current_price(holding.symbol)
            except Exception:
                current_price = holding.avg_price

            pnl = (current_price - holding.avg_price) * holding.shares

            data.append(
                {
                    "symbol": holding.symbol,
                    "shares": holding.shares,
                    "avg_price": holding.avg_price,
                    "current_price": current_price,
                    "pnl": pnl,
                }
            )

        return pd.DataFrame(data)

    def performance(self, days: int = 30) -> pd.DataFrame:
        """Get portfolio performance over time.

        Args:
            days: Number of days to calculate performance for

        Returns:
            DataFrame with columns: date, portfolio_value
        """
        if not self._holdings:
            return pd.DataFrame(columns=["date", "portfolio_value"])

        # Get price history for all holdings
        end_date = datetime.now()
        start_date = end_date - pd.Timedelta(days=days)

        all_values = []

        for holding in self._holdings:
            try:
                prices = self._reader.get_prices(
                    holding.symbol,
                    start=start_date.strftime("%Y-%m-%d"),
                    end=end_date.strftime("%Y-%m-%d"),
                )
                if not prices.empty:
                    values = prices["Close"] * holding.shares
                    all_values.append(values)
            except Exception as e:
                logger.warning(f"Error getting prices for {holding.symbol}: {e}")

        if not all_values:
            return pd.DataFrame(columns=["date", "portfolio_value"])

        # Combine all values
        combined = pd.concat(all_values, axis=1).sum(axis=1)
        result = pd.DataFrame(
            {
                "date": combined.index,
                "portfolio_value": combined.values,
            }
        )

        return result

    def plot(self) -> go.Figure:
        """Create interactive plotly chart of portfolio.

        Returns:
            Plotly Figure with holdings breakdown and performance
        """
        holdings_df = self.holdings()

        if holdings_df.empty:
            fig = go.Figure()
            fig.add_annotation(
                text="ポートフォリオに銘柄がありません",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
            )
            fig.update_layout(title="ポートフォリオ")
            return fig

        fig = make_subplots(
            rows=2,
            cols=2,
            specs=[
                [{"type": "pie"}, {"type": "bar"}],
                [{"colspan": 2}, None],
            ],
            subplot_titles=("保有比率", "損益", "パフォーマンス推移"),
            vertical_spacing=0.15,
        )

        # Pie chart - Holdings by value
        values = holdings_df["current_price"] * holdings_df["shares"]
        fig.add_trace(
            go.Pie(
                labels=holdings_df["symbol"],
                values=values,
                hole=0.3,
                textposition="inside",
                textinfo="label+percent",
            ),
            row=1,
            col=1,
        )

        # Bar chart - PnL by symbol
        colors = ["green" if pnl > 0 else "red" for pnl in holdings_df["pnl"]]
        fig.add_trace(
            go.Bar(
                x=holdings_df["symbol"],
                y=holdings_df["pnl"],
                marker_color=colors,
                name="損益",
            ),
            row=1,
            col=2,
        )

        # Line chart - Performance over time
        perf = self.performance(days=30)
        if not perf.empty:
            fig.add_trace(
                go.Scatter(
                    x=perf["date"],
                    y=perf["portfolio_value"],
                    mode="lines",
                    name="ポートフォリオ価値",
                    line=dict(color="blue", width=2),
                ),
                row=2,
                col=1,
            )

        summary = self.summary()
        title = (
            f"ポートフォリオ: {self.name}<br>"
            f"<sup>投資額: ¥{summary['total_investment']:,.0f} | "
            f"評価額: ¥{summary['current_value']:,.0f} | "
            f"損益: ¥{summary['total_pnl']:,.0f} "
            f"({summary['return_pct']:.1%})</sup>"
        )

        fig.update_layout(
            title=title,
            height=700,
            showlegend=False,
        )

        return fig

    def buy_from_screener(
        self,
        screener_filter: ScreenerFilter | dict | None = None,
        amount_per_stock: float = 100000,
        max_stocks: int = 10,
        screener: StockScreener | None = None,
        **filter_kwargs,
    ) -> "VirtualPortfolio":
        """Buy stocks from screener results.

        Adds stocks that match the screener filter to the portfolio,
        investing a fixed amount in each stock.

        Args:
            screener_filter: ScreenerFilter object or dict with filter params.
                If None, filter_kwargs are used.
            amount_per_stock: Amount in yen to invest per stock (default: 100,000)
            max_stocks: Maximum number of stocks to add (default: 10)
            screener: Optional StockScreener instance (uses default if not provided)
            **filter_kwargs: Filter parameters if screener_filter is not provided

        Returns:
            Self for method chaining

        Example:
            >>> vp.buy_from_screener(
            ...     screener_filter={"composite_score_min": 80},
            ...     amount_per_stock=100000
            ... )

            >>> from technical_tools import ScreenerFilter
            >>> config = ScreenerFilter(composite_score_min=80, hl_ratio_min=75)
            >>> vp.buy_from_screener(screener_filter=config)
        """
        if screener is None:
            screener = StockScreener()

        # Build filter
        if isinstance(screener_filter, ScreenerFilter):
            # Use the ScreenerFilter object directly
            filter_obj = screener_filter
        elif isinstance(screener_filter, dict):
            # Convert dict to ScreenerFilter
            filter_obj = ScreenerFilter(**screener_filter)
        elif filter_kwargs:
            # Build from kwargs
            filter_obj = ScreenerFilter(**filter_kwargs)
        else:
            # Default filter with high composite score
            filter_obj = ScreenerFilter(composite_score_min=70.0)

        # Override limit to max_stocks
        filter_obj.limit = max_stocks

        # Get screener results
        results_df = screener.filter(filter_obj)

        if results_df.empty:
            logger.warning("No stocks matched the screener filter")
            return self

        # Add each stock to portfolio
        added_count = 0
        for _, row in results_df.iterrows():
            symbol = row["code"]

            try:
                self.buy(symbol, amount=amount_per_stock)
                added_count += 1
                logger.info(f"Added {symbol} to portfolio (amount={amount_per_stock})")
            except PortfolioError as e:
                logger.warning(f"Failed to add {symbol}: {e}")
                continue

        logger.info(f"Added {added_count} stocks from screener results")
        return self

    def _find_holding(self, symbol: str) -> Holding | None:
        """Find holding by symbol."""
        for holding in self._holdings:
            if holding.symbol == symbol:
                return holding
        return None

    def _get_current_price(self, symbol: str) -> float:
        """Get current price for a symbol."""
        df = self._reader.get_prices(symbol)
        if df.empty:
            raise PortfolioError(f"Cannot get price for {symbol}")
        return float(df["Close"].iloc[-1])

    def __repr__(self) -> str:
        return f"VirtualPortfolio(name='{self.name}', holdings={len(self._holdings)})"
