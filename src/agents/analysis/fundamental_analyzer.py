"""
Fundamental Analyzer

Compares USDA WASDE estimates vs yield model predictions vs user spreadsheet
estimates, with historical accuracy metrics from the forecast tracker.

Provides side-by-side comparison for corn, soybeans, and wheat with
stocks-to-use ratios, yield deviations, and demand trend analysis.

Usage:
    analyzer = FundamentalAnalyzer()
    result = analyzer.analyze('corn', '2025/26')
    print(result['comparison_table'])
"""

import logging
from datetime import date
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def _get_connection():
    from src.services.database.db_config import get_connection
    return get_connection()


class FundamentalAnalyzer:
    """Analyzes supply and demand fundamentals with multi-source comparison."""

    def analyze(self, commodity: str, marketing_year: str = None,
                country: str = 'US') -> Dict:
        """
        Run a comprehensive fundamental analysis.

        Args:
            commodity: corn, soybeans, wheat
            marketing_year: e.g. '2025/26' (default: latest)
            country: Country code

        Returns:
            Dict with balance_sheet, model_comparison, accuracy, outlook
        """
        # Get WASDE balance sheet
        wasde = self._get_wasde_data(commodity, country, marketing_year)

        # Get model yield forecast
        model_yield = self._get_model_yield(commodity)

        # Get user estimates (from silver.user_sd_estimate)
        user_est = self._get_user_estimates(commodity, country, marketing_year)

        # Get forecast accuracy
        accuracy = self._get_forecast_accuracy(commodity)

        # Build comparison
        comparison = self._build_comparison(commodity, wasde, model_yield, user_est)

        # Key metrics
        metrics = self._compute_key_metrics(wasde)

        return {
            'commodity': commodity,
            'country': country,
            'marketing_year': marketing_year or (wasde.get('marketing_year') if wasde else 'Unknown'),
            'balance_sheet': wasde,
            'model_yield': model_yield,
            'user_estimates': user_est,
            'comparison_table': comparison,
            'key_metrics': metrics,
            'forecast_accuracy': accuracy,
        }

    def analyze_all(self, commodities: List[str] = None,
                    country: str = 'US') -> Dict:
        """Analyze all major commodities."""
        commodities = commodities or ['corn', 'soybeans', 'wheat']
        return {c: self.analyze(c, country=country) for c in commodities}

    def _get_wasde_data(self, commodity: str, country: str,
                         marketing_year: str = None) -> Optional[Dict]:
        """Get latest WASDE balance sheet."""
        try:
            with _get_connection() as conn:
                cur = conn.cursor()
                if marketing_year:
                    my_int = int(marketing_year.split('/')[0])
                    cur.execute("""
                        SELECT marketing_year, report_date,
                               area_harvested, yield, production,
                               beginning_stocks, imports, total_supply,
                               feed_dom_consumption, fsi_consumption, crush,
                               domestic_consumption, exports, ending_stocks,
                               total_distribution
                        FROM bronze.fas_psd
                        WHERE commodity = %s AND country_code = %s
                          AND marketing_year = %s AND ending_stocks IS NOT NULL
                        ORDER BY report_date DESC LIMIT 1
                    """, (commodity, country, my_int))
                else:
                    cur.execute("""
                        SELECT marketing_year, report_date,
                               area_harvested, yield, production,
                               beginning_stocks, imports, total_supply,
                               feed_dom_consumption, fsi_consumption, crush,
                               domestic_consumption, exports, ending_stocks,
                               total_distribution
                        FROM bronze.fas_psd
                        WHERE commodity = %s AND country_code = %s
                          AND ending_stocks IS NOT NULL
                        ORDER BY marketing_year DESC, report_date DESC LIMIT 1
                    """, (commodity, country))

                row = cur.fetchone()
                if not row:
                    return None

                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))
        except Exception as e:
            logger.warning(f"WASDE data fetch failed: {e}")
            return None

    def _get_model_yield(self, commodity: str) -> Optional[Dict]:
        """Get the latest yield model prediction."""
        try:
            with _get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT AVG(yield_forecast) AS avg_yield,
                           AVG(confidence) AS avg_confidence,
                           MAX(forecast_week) AS latest_week,
                           MAX(year) AS year
                    FROM gold.yield_forecast
                    WHERE commodity = %s AND model_type = 'ensemble'
                      AND year = EXTRACT(YEAR FROM CURRENT_DATE)
                """, (commodity.upper(),))
                row = cur.fetchone()
                if row and row[0] is not None:
                    return {
                        'yield_forecast': round(float(row[0]), 1),
                        'confidence': round(float(row[1]), 2) if row[1] else None,
                        'week': int(row[2]) if row[2] else None,
                        'year': int(row[3]) if row[3] else None,
                    }
        except Exception as e:
            logger.debug(f"Model yield fetch failed: {e}")
        return None

    def _get_user_estimates(self, commodity: str, country: str,
                            marketing_year: str = None) -> Optional[Dict]:
        """Get user S&D estimates from silver.user_sd_estimate."""
        try:
            with _get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT attribute, estimate_value
                    FROM silver.user_sd_estimate
                    WHERE commodity = %s AND country = %s
                    ORDER BY updated_at DESC
                """, (commodity, country))
                rows = cur.fetchall()
                if rows:
                    return {row[0]: float(row[1]) for row in rows}
        except Exception as e:
            logger.debug(f"User estimates fetch failed: {e}")
        return None

    def _get_forecast_accuracy(self, commodity: str) -> Optional[Dict]:
        """Get forecast accuracy from tracker."""
        try:
            with _get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT forecast_type, n_forecasts, mape,
                           directional_accuracy, mpe AS bias_pct
                    FROM core.accuracy_metrics
                    WHERE commodity = %s
                    ORDER BY computed_date DESC
                    LIMIT 5
                """, (commodity,))
                rows = cur.fetchall()
                if rows:
                    cols = [d[0] for d in cur.description]
                    return [dict(zip(cols, r)) for r in rows]
        except Exception as e:
            logger.debug(f"Accuracy fetch failed: {e}")
        return None

    def _build_comparison(self, commodity: str, wasde: Dict,
                           model_yield: Dict, user_est: Dict) -> str:
        """Build a markdown comparison table."""
        lines = [
            f"## {commodity.title()} S&D Comparison",
            "",
            "| Source | Yield | Production | Ending Stocks | S/U % |",
            "|--------|-------|------------|---------------|-------|",
        ]

        if wasde:
            stu = self._stocks_to_use(wasde)
            lines.append(
                f"| USDA WASDE | {wasde.get('yield', 'N/A')} | "
                f"{wasde.get('production', 'N/A')} | "
                f"{wasde.get('ending_stocks', 'N/A')} | {stu} |"
            )

        if model_yield:
            lines.append(
                f"| RLC Model | {model_yield['yield_forecast']} bu/ac | "
                f"— | — | — |"
            )

        if user_est:
            u_yield = user_est.get('yield', 'N/A')
            u_prod = user_est.get('production', 'N/A')
            u_es = user_est.get('ending_stocks', 'N/A')
            lines.append(
                f"| User Estimate | {u_yield} | {u_prod} | {u_es} | — |"
            )

        return "\n".join(lines)

    def _compute_key_metrics(self, wasde: Dict) -> Dict:
        """Compute key supply/demand metrics."""
        if not wasde:
            return {}

        stu = None
        total_use = (wasde.get('domestic_consumption') or 0) + (wasde.get('exports') or 0)
        if total_use > 0 and wasde.get('ending_stocks'):
            stu = round(float(wasde['ending_stocks']) / total_use * 100, 1)

        return {
            'stocks_to_use_pct': stu,
            'total_supply': wasde.get('total_supply'),
            'total_use': total_use,
            'ending_stocks': wasde.get('ending_stocks'),
        }

    def _stocks_to_use(self, wasde: Dict) -> str:
        total_use = (wasde.get('domestic_consumption') or 0) + (wasde.get('exports') or 0)
        if total_use > 0 and wasde.get('ending_stocks'):
            return f"{float(wasde['ending_stocks']) / total_use * 100:.1f}%"
        return "N/A"
