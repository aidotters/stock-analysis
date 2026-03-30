"""Tests for VirtualPortfolio class."""

import json
import pandas as pd
import pytest
from pathlib import Path
from datetime import datetime

from technical_tools.virtual_portfolio import VirtualPortfolio
from technical_tools.screener import ScreenerFilter
from technical_tools.exceptions import PortfolioError


@pytest.fixture
def temp_portfolio_dir(tmp_path: Path) -> Path:
    """Create temporary directory for portfolio files."""
    portfolio_dir = tmp_path / "portfolios"
    portfolio_dir.mkdir()
    return portfolio_dir


@pytest.fixture
def mock_price_data() -> pd.DataFrame:
    """Create mock price data."""
    dates = pd.date_range(start="2023-01-01", periods=100, freq="B")
    return pd.DataFrame(
        {
            "Open": [2500 + i * 10 for i in range(100)],
            "High": [2520 + i * 10 for i in range(100)],
            "Low": [2480 + i * 10 for i in range(100)],
            "Close": [2510 + i * 10 for i in range(100)],
            "Volume": [1000000] * 100,
        },
        index=dates,
    )


class TestVirtualPortfolioInit:
    """Test VirtualPortfolio initialization."""

    def test_init_creates_new_portfolio(self, temp_portfolio_dir: Path) -> None:
        """Can create a new portfolio with a name."""
        vp = VirtualPortfolio("test_strategy", portfolio_dir=temp_portfolio_dir)
        assert vp is not None
        assert vp.name == "test_strategy"

    def test_init_loads_existing_portfolio(self, temp_portfolio_dir: Path) -> None:
        """Can load an existing portfolio from JSON."""
        # Create a portfolio file
        portfolio_file = temp_portfolio_dir / "existing.json"
        portfolio_data = {
            "name": "existing",
            "created_at": "2023-01-01T00:00:00",
            "holdings": [
                {
                    "symbol": "7203",
                    "shares": 100,
                    "avg_price": 2500.0,
                    "purchased_at": "2023-01-10T00:00:00",
                }
            ],
            "transactions": [],
        }
        portfolio_file.write_text(json.dumps(portfolio_data))

        vp = VirtualPortfolio("existing", portfolio_dir=temp_portfolio_dir)
        holdings = vp.holdings()

        assert len(holdings) == 1
        assert holdings.iloc[0]["symbol"] == "7203"


class TestVirtualPortfolioBuy:
    """Test buy method."""

    def test_buy_with_shares(self, temp_portfolio_dir: Path, mocker) -> None:
        """Can buy a stock specifying number of shares."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [2500.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100)

        holdings = vp.holdings()
        assert len(holdings) == 1
        assert holdings.iloc[0]["shares"] == 100

    def test_buy_with_amount(self, temp_portfolio_dir: Path, mocker) -> None:
        """Can buy a stock specifying amount in yen."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [2500.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", amount=250000)  # 250000 / 2500 = 100 shares

        holdings = vp.holdings()
        assert len(holdings) == 1
        assert holdings.iloc[0]["shares"] == 100

    def test_buy_with_explicit_price(self, temp_portfolio_dir: Path) -> None:
        """Can buy a stock with explicit price."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        holdings = vp.holdings()
        assert len(holdings) == 1
        assert holdings.iloc[0]["avg_price"] == 2500.0

    def test_buy_additional_shares(self, temp_portfolio_dir: Path) -> None:
        """Buying more of existing stock updates average price."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)
        vp.buy("7203", shares=100, price=3000.0)

        holdings = vp.holdings()
        assert len(holdings) == 1
        assert holdings.iloc[0]["shares"] == 200
        # Average price: (100*2500 + 100*3000) / 200 = 2750
        assert holdings.iloc[0]["avg_price"] == 2750.0

    def test_buy_without_shares_or_amount_raises_error(
        self, temp_portfolio_dir: Path
    ) -> None:
        """Buying without shares or amount raises error."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        with pytest.raises(PortfolioError):
            vp.buy("7203")


class TestVirtualPortfolioSell:
    """Test sell and sell_all methods."""

    def test_sell_partial_shares(self, temp_portfolio_dir: Path) -> None:
        """Can sell partial shares."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)
        vp.sell("7203", shares=50, price=3000.0)

        holdings = vp.holdings()
        assert holdings.iloc[0]["shares"] == 50

    def test_sell_all_removes_holding(self, temp_portfolio_dir: Path) -> None:
        """sell_all removes the entire holding."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)
        vp.sell_all("7203", price=3000.0)

        holdings = vp.holdings()
        assert len(holdings) == 0

    def test_sell_more_than_owned_raises_error(self, temp_portfolio_dir: Path) -> None:
        """Selling more than owned raises error."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        with pytest.raises(PortfolioError):
            vp.sell("7203", shares=150)

    def test_sell_non_existent_stock_raises_error(
        self, temp_portfolio_dir: Path
    ) -> None:
        """Selling a stock not owned raises error."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)

        with pytest.raises(PortfolioError):
            vp.sell("7203", shares=100)


class TestVirtualPortfolioSummary:
    """Test summary method."""

    def test_summary_returns_dict(self, temp_portfolio_dir: Path, mocker) -> None:
        """summary() returns a dictionary."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [3000.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        summary = vp.summary()
        assert isinstance(summary, dict)

    def test_summary_contains_required_fields(
        self, temp_portfolio_dir: Path, mocker
    ) -> None:
        """summary() contains required fields."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [3000.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        summary = vp.summary()

        assert "total_investment" in summary
        assert "current_value" in summary
        assert "total_pnl" in summary
        assert "return_pct" in summary

    def test_summary_calculates_pnl_correctly(
        self, temp_portfolio_dir: Path, mocker
    ) -> None:
        """summary() calculates PnL correctly."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [3000.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        summary = vp.summary()

        # Investment: 100 * 2500 = 250000
        # Current value: 100 * 3000 = 300000
        # PnL: 50000
        # Return: 20%
        assert summary["total_investment"] == 250000
        assert summary["current_value"] == 300000
        assert summary["total_pnl"] == 50000
        assert abs(summary["return_pct"] - 0.20) < 0.01


class TestVirtualPortfolioHoldings:
    """Test holdings method."""

    def test_holdings_returns_dataframe(self, temp_portfolio_dir: Path, mocker) -> None:
        """holdings() returns a DataFrame."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [3000.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        holdings = vp.holdings()
        assert isinstance(holdings, pd.DataFrame)

    def test_holdings_contains_required_columns(
        self, temp_portfolio_dir: Path, mocker
    ) -> None:
        """holdings() DataFrame contains required columns."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [3000.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        holdings = vp.holdings()

        required_columns = [
            "symbol",
            "shares",
            "avg_price",
            "current_price",
            "pnl",
        ]
        for col in required_columns:
            assert col in holdings.columns


class TestVirtualPortfolioPerformance:
    """Test performance method."""

    def test_performance_returns_dataframe(
        self, temp_portfolio_dir: Path, mock_price_data: pd.DataFrame, mocker
    ) -> None:
        """performance() returns a DataFrame."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = mock_price_data

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        perf = vp.performance()
        assert isinstance(perf, pd.DataFrame)

    def test_performance_contains_daily_returns(
        self, temp_portfolio_dir: Path, mock_price_data: pd.DataFrame, mocker
    ) -> None:
        """performance() contains daily return data."""
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = mock_price_data

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        perf = vp.performance()
        assert "value" in perf.columns or "portfolio_value" in perf.columns


class TestVirtualPortfolioPlot:
    """Test plot method."""

    def test_plot_returns_figure(
        self, temp_portfolio_dir: Path, mock_price_data: pd.DataFrame, mocker
    ) -> None:
        """plot() returns a plotly Figure."""
        import plotly.graph_objects as go

        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = mock_price_data

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        fig = vp.plot()
        assert isinstance(fig, go.Figure)


class TestVirtualPortfolioPersistence:
    """Test JSON persistence."""

    def test_portfolio_is_saved_after_buy(self, temp_portfolio_dir: Path) -> None:
        """Portfolio is saved to JSON after buy."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)

        portfolio_file = temp_portfolio_dir / "test.json"
        assert portfolio_file.exists()

    def test_portfolio_is_saved_after_sell(self, temp_portfolio_dir: Path) -> None:
        """Portfolio is saved to JSON after sell."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)
        vp.sell("7203", shares=50, price=3000.0)

        portfolio_file = temp_portfolio_dir / "test.json"
        data = json.loads(portfolio_file.read_text())

        # Should have 1 holding with 50 shares
        assert len(data["holdings"]) == 1
        assert data["holdings"][0]["shares"] == 50

    def test_transactions_are_recorded(self, temp_portfolio_dir: Path) -> None:
        """All transactions are recorded in JSON."""
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy("7203", shares=100, price=2500.0)
        vp.sell("7203", shares=50, price=3000.0)

        portfolio_file = temp_portfolio_dir / "test.json"
        data = json.loads(portfolio_file.read_text())

        # Should have 2 transactions (buy + sell)
        assert len(data["transactions"]) == 2

    def test_portfolio_can_be_reloaded(self, temp_portfolio_dir: Path) -> None:
        """Portfolio can be reloaded after restart."""
        # Create and save
        vp1 = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp1.buy("7203", shares=100, price=2500.0)

        # Reload
        vp2 = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        holdings = vp2.holdings()

        assert len(holdings) == 1
        assert holdings.iloc[0]["shares"] == 100


class TestVirtualPortfolioBuyFromScreener:
    """Test buy_from_screener method."""

    def test_buy_from_screener_with_dict_filter(
        self, temp_portfolio_dir: Path, mocker
    ) -> None:
        """buy_from_screener() works with dict filter."""
        # Mock screener
        mock_screener = mocker.MagicMock()
        mock_screener.filter.return_value = pd.DataFrame(
            {
                "code": ["7203", "9984"],
                "composite_score": [85.0, 80.0],
            }
        )

        # Mock price reader
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [2500.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy_from_screener(
            screener_filter={"composite_score_min": 80},
            amount_per_stock=100000,
            screener=mock_screener,
        )

        holdings = vp.holdings()
        assert len(holdings) == 2

    def test_buy_from_screener_with_screener_filter_object(
        self, temp_portfolio_dir: Path, mocker
    ) -> None:
        """buy_from_screener() works with ScreenerFilter object."""
        # Mock screener
        mock_screener = mocker.MagicMock()
        mock_screener.filter.return_value = pd.DataFrame(
            {
                "code": ["7203"],
                "composite_score": [85.0],
            }
        )

        # Mock price reader
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [2500.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        config = ScreenerFilter(composite_score_min=80.0)
        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy_from_screener(
            screener_filter=config,
            amount_per_stock=100000,
            screener=mock_screener,
        )

        holdings = vp.holdings()
        assert len(holdings) == 1

    def test_buy_from_screener_with_max_stocks(
        self, temp_portfolio_dir: Path, mocker
    ) -> None:
        """buy_from_screener() respects max_stocks parameter."""
        # Mock screener
        mock_screener = mocker.MagicMock()
        mock_screener.filter.return_value = pd.DataFrame(
            {
                "code": ["7203", "9984", "6758"],
                "composite_score": [85.0, 80.0, 75.0],
            }
        )

        # Mock price reader
        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [2500.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        vp.buy_from_screener(
            screener_filter={"composite_score_min": 70},
            amount_per_stock=100000,
            max_stocks=2,
            screener=mock_screener,
        )

        # The filter's limit should be set to max_stocks
        call_args = mock_screener.filter.call_args
        assert call_args[0][0].limit == 2

    def test_buy_from_screener_with_empty_results(
        self, temp_portfolio_dir: Path, mocker
    ) -> None:
        """buy_from_screener() handles empty screener results."""
        # Mock screener returning empty
        mock_screener = mocker.MagicMock()
        mock_screener.filter.return_value = pd.DataFrame()

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        result = vp.buy_from_screener(
            screener_filter={"composite_score_min": 99},
            screener=mock_screener,
        )

        assert result is vp  # Returns self for chaining
        assert len(vp.holdings()) == 0

    def test_buy_from_screener_returns_self(
        self, temp_portfolio_dir: Path, mocker
    ) -> None:
        """buy_from_screener() returns self for method chaining."""
        mock_screener = mocker.MagicMock()
        mock_screener.filter.return_value = pd.DataFrame(
            {
                "code": ["7203"],
                "composite_score": [85.0],
            }
        )

        mock_reader = mocker.patch("technical_tools.virtual_portfolio.DataReader")
        mock_reader.return_value.get_prices.return_value = pd.DataFrame(
            {
                "Close": [2500.0],
            },
            index=pd.DatetimeIndex([datetime.now()]),
        )

        vp = VirtualPortfolio("test", portfolio_dir=temp_portfolio_dir)
        result = vp.buy_from_screener(
            screener_filter={"composite_score_min": 80},
            screener=mock_screener,
        )

        assert result is vp
