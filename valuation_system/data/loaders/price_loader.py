"""
Price Loader - Fetch current and historical prices and multiples.
Primary source: combined_monthly_prices.csv (updated daily)
Fallback: Yahoo Finance
"""

import os
import logging
from typing import Optional
from datetime import date

import numpy as np
import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'config', '.env'))


class PriceLoader:
    """
    Price and multiple data from two sources:
    1. combined_monthly_prices.csv (PRIMARY - updated daily, has P/E, P/B, EV/EBITDA)
    2. Yahoo Finance (FALLBACK - for intraday or if local file not updated)

    Columns in monthly prices file:
    - Company Name, daily_date, open, high, low, close, mcap, vol
    - pe, pb, evebidta, ps (valuation multiples)
    - nse_symbol, accode, exchange, bse_code, year_month
    - sector, industry, house, trading_status, listing_status, isin
    """

    def __init__(self, prices_path: str = None):
        self.prices_path = prices_path or os.getenv('MONTHLY_PRICES_PATH')
        if not self.prices_path:
            raise ValueError("MONTHLY_PRICES_PATH not set in .env")

        self._df = None
        logger.info(f"PriceLoader initialized with: {self.prices_path}")

    @property
    def df(self) -> pd.DataFrame:
        """Lazy load the prices file."""
        if self._df is None:
            logger.info(f"Loading monthly prices: {self.prices_path}")
            self._df = pd.read_csv(self.prices_path, low_memory=False)
            self._df['daily_date'] = pd.to_datetime(self._df['daily_date'])
            logger.info(f"Loaded {len(self._df)} price records, date range: "
                        f"{self._df['daily_date'].min()} to {self._df['daily_date'].max()}")
        return self._df

    def reload(self):
        """Force reload of prices file (call after daily update)."""
        self._df = None
        _ = self.df

    def get_latest_data(self, symbol: str, bse_code=None, company_name=None) -> dict:
        """
        Get latest price + multiples for a symbol.
        Returns CMP, P/E, P/B, EV/EBITDA, P/S, MCap all in one call.
        Lookup order: nse_symbol -> bse_code -> company_name -> Yahoo fallback.
        """
        # Sanitize: pandas nan comes through as string 'nan' from core CSV
        import math
        if not symbol or symbol == 'nan' or (isinstance(symbol, float) and math.isnan(symbol)):
            symbol = ''

        # Use per-symbol latest date, not global max (some records have future dates)
        symbol_rows = self.df[self.df['nse_symbol'] == symbol] if symbol else pd.DataFrame()

        if symbol_rows.empty and bse_code:
            # Try with explicit BSE code (numeric only — skip non-numeric scrip_codes)
            try:
                bse_code_num = float(bse_code)
                symbol_rows = self.df[self.df['bse_code'] == bse_code_num]
                if not symbol_rows.empty:
                    logger.info(f"Found {symbol} via BSE code {bse_code}")
            except (ValueError, TypeError):
                logger.debug(f"Non-numeric BSE code '{bse_code}' for {symbol}, skipping BSE lookup")

        if symbol_rows.empty and company_name:
            # Try by company name (exact match first, then contains)
            symbol_rows = self.df[self.df['Company Name'] == company_name]
            if symbol_rows.empty:
                symbol_rows = self.df[self.df['Company Name'].str.contains(company_name.split(' ')[0], case=False, na=False)]
            if not symbol_rows.empty:
                logger.info(f"Found {symbol} via company name lookup")

        if symbol_rows.empty:
            display_name = symbol or company_name or 'unknown'
            logger.warning(f"No local price data for {display_name} (bse_code={bse_code}), falling back to Yahoo")
            return self._get_from_yahoo(symbol if symbol else display_name)

        # Get the most recent row for this symbol
        row = symbol_rows.sort_values('daily_date', ascending=False).iloc[0]

        return {
            'company_name': row.get('Company Name', ''),
            'cmp': self._safe_float(row.get('close')),
            'pe': self._safe_float(row.get('pe')),
            'pb': self._safe_float(row.get('pb')),
            'ev_ebitda': self._safe_float(row.get('evebidta')),
            'ps': self._safe_float(row.get('ps')),
            'mcap_cr': self._safe_float(row.get('mcap')),
            'volume': self._safe_float(row.get('vol')),
            'date': row['daily_date'].date() if pd.notna(row['daily_date']) else None,
            'sector': row.get('sector', ''),
            'industry': row.get('industry', ''),
            'source': 'local_monthly_prices'
        }

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get latest closing price."""
        data = self.get_latest_data(symbol)
        return data.get('cmp')

    def get_peer_multiples(self, sector: str, top_n: int = 15,
                           as_of_date: str = None) -> dict:
        """
        Get peer multiples for a sector.
        Auto-selects top N by market cap.
        Uses latest daily_date or specified date.
        """
        if as_of_date:
            target_date = pd.to_datetime(as_of_date)
        else:
            target_date = self.df['daily_date'].max()

        sector_df = self.df[
            (self.df['sector'] == sector) &
            (self.df['daily_date'] == target_date)
        ].copy()

        if sector_df.empty:
            logger.warning(f"No data for sector '{sector}' on {target_date}")
            return {}

        # Top N by market cap
        sector_df = sector_df.dropna(subset=['mcap'])
        peers = sector_df.nlargest(top_n, 'mcap')

        def safe_stats(series):
            clean = series.dropna()
            clean = clean[clean > 0]  # Remove negative/zero multiples
            if clean.empty:
                return {'median': None, 'mean': None, 'p25': None, 'p75': None}
            return {
                'median': float(clean.median()),
                'mean': float(clean.mean()),
                'p25': float(clean.quantile(0.25)),
                'p75': float(clean.quantile(0.75)),
                'count': len(clean)
            }

        return {
            'as_of_date': str(target_date.date()),
            'sector': sector,
            'peer_count': len(peers),
            'pe': safe_stats(peers['pe']),
            'pb': safe_stats(peers['pb']),
            'ev_ebitda': safe_stats(peers['evebidta']),
            'ps': safe_stats(peers['ps']),
            'mcap': safe_stats(peers['mcap']),
            'peer_list': peers[['Company Name', 'nse_symbol', 'mcap', 'pe', 'pb', 'evebidta', 'ps']].to_dict('records')
        }

    def get_peer_multiples_by_symbols(self, symbols: list,
                                       weights: dict = None,
                                       as_of_date: str = None) -> dict:
        """
        Get aggregated peer multiples for specific symbols (mssdb-selected peers).

        Args:
            symbols: List of NSE symbols for peer companies
            weights: Optional {symbol: weight} for weighted stats
                     (tight peers get 2x, broad peers get 1x)
            as_of_date: Optional date, defaults to latest

        Returns:
            Same structure as get_peer_multiples() for compatibility.
        """
        if not symbols:
            return {}

        if as_of_date:
            target_date = pd.to_datetime(as_of_date)
        else:
            target_date = self.df['daily_date'].max()

        # Filter for given symbols on target date
        peers_df = self.df[
            (self.df['nse_symbol'].isin(symbols)) &
            (self.df['daily_date'] == target_date)
        ].copy()

        if peers_df.empty:
            logger.warning(f"No price data for peer symbols on {target_date}: {symbols}")
            return {}

        # Filter out companies with no mcap
        peers_df = peers_df.dropna(subset=['mcap'])

        def weighted_stats(series, symbol_col=None):
            """Compute stats, optionally weighted."""
            clean = series.dropna()
            clean = clean[clean > 0]
            if clean.empty:
                return {'median': None, 'mean': None, 'p25': None, 'p75': None, 'count': 0}

            if weights and symbol_col is not None:
                # Get corresponding symbols for the clean values
                clean_symbols = symbol_col.loc[clean.index]
                w = np.array([weights.get(s, 1.0) for s in clean_symbols])
                w = w / w.sum()  # Normalize
                sorted_idx = clean.values.argsort()
                sorted_vals = clean.values[sorted_idx]
                sorted_w = w[sorted_idx]
                cumw = np.cumsum(sorted_w)
                # Weighted median: value where cumulative weight crosses 0.5
                median_val = float(sorted_vals[np.searchsorted(cumw, 0.5)])
                mean_val = float(np.average(clean.values, weights=w))
                p25_val = float(sorted_vals[np.searchsorted(cumw, 0.25)])
                p75_val = float(sorted_vals[np.searchsorted(cumw, 0.75)])
            else:
                median_val = float(clean.median())
                mean_val = float(clean.mean())
                p25_val = float(clean.quantile(0.25))
                p75_val = float(clean.quantile(0.75))

            return {
                'median': round(median_val, 2),
                'mean': round(mean_val, 2),
                'p25': round(p25_val, 2),
                'p75': round(p75_val, 2),
                'count': len(clean)
            }

        sym_col = peers_df['nse_symbol']
        result = {
            'as_of_date': str(target_date.date()),
            'peer_count': len(peers_df),
            'pe': weighted_stats(peers_df['pe'], sym_col),
            'pb': weighted_stats(peers_df['pb'], sym_col),
            'ev_ebitda': weighted_stats(peers_df['evebidta'], sym_col),
            'ps': weighted_stats(peers_df['ps'], sym_col),
            'mcap': weighted_stats(peers_df['mcap'], sym_col),
            'peer_list': peers_df[
                ['Company Name', 'nse_symbol', 'mcap', 'pe', 'pb', 'evebidta', 'ps']
            ].to_dict('records'),
        }

        logger.info(f"Peer multiples from {len(peers_df)} symbols: "
                     f"PE median={result['pe'].get('median')}, "
                     f"PB median={result['pb'].get('median')}, "
                     f"EV/EBITDA median={result['ev_ebitda'].get('median')}")
        return result

    def get_mcap_for_symbols(self, symbols: list, as_of_date: str = None) -> dict:
        """
        Get latest MCap for a list of symbols from prices CSV.
        Returns: {symbol: mcap_cr, ...}
        Used for size filtering in peer selection.
        """
        if not symbols:
            return {}

        if as_of_date:
            target_date = pd.to_datetime(as_of_date)
        else:
            target_date = self.df['daily_date'].max()

        data = self.df[
            (self.df['nse_symbol'].isin(symbols)) &
            (self.df['daily_date'] == target_date)
        ]

        result = {}
        for _, row in data.iterrows():
            sym = row.get('nse_symbol')
            mcap = self._safe_float(row.get('mcap'))
            if sym and mcap and mcap > 0:
                result[sym] = mcap
        return result

    def get_historical_multiples(self, symbol: str, periods: int = 60) -> pd.DataFrame:
        """
        Get historical P/E, P/B, EV/EBITDA for trend analysis.
        Useful for mean-reversion based relative valuation.
        """
        company_df = self.df[
            self.df['nse_symbol'] == symbol
        ].sort_values('daily_date', ascending=False)

        if company_df.empty:
            logger.warning(f"No historical data for {symbol}")
            return pd.DataFrame()

        cols = ['daily_date', 'year_month', 'close', 'pe', 'pb', 'evebidta', 'ps', 'mcap']
        available_cols = [c for c in cols if c in company_df.columns]

        return company_df[available_cols].head(periods).reset_index(drop=True)

    def get_price_history_for_beta(self, symbol: str, period: str = '5y') -> pd.DataFrame:
        """
        Get daily price history for beta calculation.
        Falls back to Yahoo Finance for daily granularity.
        """
        try:
            import yfinance
            ticker = yfinance.Ticker(f"{symbol}.NS")
            return ticker.history(period=period)
        except Exception as e:
            logger.error(f"Yahoo Finance error for {symbol}: {e}")
            return pd.DataFrame()

    def get_market_index_returns(self, index: str = '^NSEI', period: str = '5y') -> pd.DataFrame:
        """Get Nifty 50 returns for beta calculation."""
        try:
            import yfinance
            ticker = yfinance.Ticker(index)
            return ticker.history(period=period)
        except Exception as e:
            logger.error(f"Yahoo Finance error for {index}: {e}")
            return pd.DataFrame()

    def _get_from_yahoo(self, symbol: str) -> dict:
        """Fallback to Yahoo Finance."""
        try:
            import yfinance
            ticker = yfinance.Ticker(f"{symbol}.NS")
            hist = ticker.history(period='5d')
            info = ticker.info

            if hist.empty:
                logger.error(f"No Yahoo Finance data for {symbol}")
                return {'cmp': None, 'source': 'yahoo_finance_failed'}

            return {
                'company_name': info.get('longName', symbol),
                'cmp': float(hist['Close'].iloc[-1]),
                'pe': info.get('trailingPE'),
                'pb': info.get('priceToBook'),
                'ev_ebitda': info.get('enterpriseToEbitda'),
                'ps': info.get('priceToSalesTrailing12Months'),
                'mcap_cr': info.get('marketCap', 0) / 1e7,
                'date': date.today(),
                'source': 'yahoo_finance'
            }
        except Exception as e:
            logger.error(f"Yahoo Finance fallback failed for {symbol}: {e}")
            return {'cmp': None, 'source': 'failed'}

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        """Safely convert to float, handling NaN and None."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
