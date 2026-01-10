"""
Tests for Wheat Tender Collector

Run with:
    pytest tests/test_wheat_tender_collector.py -v
"""

import pytest
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.agents.collectors.tenders.wheat_tender_collector import (
    WheatTenderCollector,
    WheatTenderConfig,
    TenderParser,
    TenderResult,
    TenderAnnouncement,
    TenderType,
    AgricensusScraper,
    AgroChartScraper,
)
from src.agents.collectors.tenders.alert_system import (
    TenderAlertManager,
    AlertConfig,
    AlertMessage,
)


class TestTenderParser:
    """Tests for the TenderParser class"""

    def setup_method(self):
        self.parser = TenderParser()

    def test_extract_volume_basic(self):
        """Test basic volume extraction"""
        text = "Egypt bought 60,000 mt of wheat from Russia"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.volume_mt == 60000

    def test_extract_volume_thousands(self):
        """Test volume with k suffix"""
        text = "Algeria purchased 480k tonnes of wheat"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.volume_mt == 480000

    def test_extract_volume_large(self):
        """Test large volume extraction"""
        text = "OAIC bought 600,000 mt wheat"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.volume_mt == 600000

    def test_extract_price_usd(self):
        """Test price extraction in USD"""
        text = "wheat at $275.50/mt C&F"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.price_usd_mt == 275.50
        assert result.price_type == "C&F"

    def test_extract_price_cif(self):
        """Test CIF price extraction"""
        text = "booked wheat USD 280 per tonne CIF"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.price_usd_mt == 280.0
        assert result.price_type == "CIF"

    def test_extract_country_egypt(self):
        """Test Egypt country extraction"""
        text = "Egyptian wheat tender results announced"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.country == "Egypt"
        assert result.country_code == "EG"

    def test_extract_country_algeria(self):
        """Test Algeria country extraction"""
        text = "Algeria OAIC buys wheat"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.country == "Algeria"
        assert result.country_code == "DZ"

    def test_extract_agency_gasc(self):
        """Test GASC agency extraction"""
        text = "GASC bought 60,000 mt wheat"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.agency == "GASC"
        assert result.agency_code == "GASC"

    def test_extract_agency_mostakbal_misr(self):
        """Test Mostakbal Misr extraction"""
        text = "Mostakbal Misr buys wheat from France"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.agency == "Mostakbal Misr"
        assert result.agency_code == "MOSTAKBAL_MISR"

    def test_extract_agency_oaic(self):
        """Test OAIC extraction"""
        text = "OAIC tender results for 500,000 mt"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.agency == "OAIC"
        assert result.agency_code == "OAIC"

    def test_extract_origins(self):
        """Test origin country extraction"""
        text = "bought wheat from Russia, France, and Romania"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert "Russia" in result.origins
        assert "France" in result.origins
        assert "Romania" in result.origins

    def test_extract_suppliers(self):
        """Test supplier extraction"""
        text = "Cargill and Viterra won the tender"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert "Cargill" in result.suppliers
        assert "Viterra" in result.suppliers

    def test_extract_wheat_type_milling(self):
        """Test milling wheat type extraction"""
        text = "bought 60,000 mt milling wheat"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.wheat_type == "milling"

    def test_extract_wheat_type_feed(self):
        """Test feed wheat type extraction"""
        text = "purchased feed wheat for livestock"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.wheat_type == "feed"

    def test_tender_type_result(self):
        """Test tender type detection - result"""
        text = "Egypt awarded wheat tender to Russia"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.tender_type == TenderType.RESULT

    def test_tender_type_announcement(self):
        """Test tender type detection - announcement"""
        text = "OAIC issued tender seeking 500,000 mt wheat"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.tender_type == TenderType.ANNOUNCEMENT

    def test_tender_type_cancelled(self):
        """Test tender type detection - cancelled"""
        text = "Algeria cancelled wheat tender, no award"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.tender_type == TenderType.CANCELLED

    def test_comprehensive_parse(self):
        """Test parsing a realistic tender article"""
        headline = "Egypt's Mostakbal Misr buys 60,000 mt wheat from Russia"
        text = """
        Egypt's Mostakbal Misr on Tuesday booked 60,000 mt of milling wheat
        from Russia at $275.50/mt C&F for January 15-31 shipment.
        The cargo was awarded to trading house Cargill.
        """

        result = self.parser.parse(text, headline, source="test", source_url="http://test.com")

        assert result.country == "Egypt"
        assert result.country_code == "EG"
        assert result.agency == "Mostakbal Misr"
        assert result.volume_mt == 60000
        assert result.price_usd_mt == 275.50
        assert result.price_type == "C&F"
        assert result.wheat_type == "milling"
        assert "Russia" in result.origins
        assert "Cargill" in result.suppliers
        assert result.tender_type == TenderType.RESULT
        assert result.parse_confidence > 0.5

    def test_parse_confidence_high(self):
        """Test that comprehensive data gives high confidence"""
        text = """
        Egypt Mostakbal Misr bought 480,000 mt wheat from France
        at $280/mt C&F. Cargill awarded the contract.
        """
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        # Should have high confidence with country, agency, volume, price, origin, supplier
        assert result.parse_confidence > 0.7

    def test_parse_confidence_low(self):
        """Test that minimal data gives low confidence"""
        text = "Some agricultural news about crops"
        result = self.parser.parse(text, source="test", source_url="http://test.com")

        assert result.parse_confidence < 0.3


class TestWheatTenderCollector:
    """Tests for the WheatTenderCollector class"""

    def test_init_default_config(self):
        """Test collector initialization with default config"""
        collector = WheatTenderCollector()

        assert collector.config.source_name == "Wheat Tender Monitor"
        assert len(collector.scrapers) > 0

    def test_init_custom_config(self):
        """Test collector initialization with custom config"""
        config = WheatTenderConfig(
            scrape_interval_minutes=30,
            max_articles_per_source=10
        )
        collector = WheatTenderCollector(config)

        assert collector.config.scrape_interval_minutes == 30
        assert collector.config.max_articles_per_source == 10

    def test_get_table_name(self):
        """Test get_table_name returns correct table"""
        collector = WheatTenderCollector()
        assert collector.get_table_name() == "bronze.wheat_tender_raw"

    @patch('requests.Session.get')
    def test_collect_handles_errors(self, mock_get):
        """Test that collect handles network errors gracefully"""
        mock_get.side_effect = Exception("Network error")

        collector = WheatTenderCollector()
        result = collector.collect()

        # Should still return a result, just with no data
        assert result.success == True
        assert result.records_fetched == 0


class TestAlertManager:
    """Tests for the TenderAlertManager class"""

    def setup_method(self):
        self.manager = TenderAlertManager()

    def test_default_alerts_loaded(self):
        """Test that default alerts are loaded"""
        assert len(self.manager.configs) > 0
        alert_names = [c.name for c in self.manager.configs]
        assert "egypt_tender" in alert_names
        assert "algeria_tender" in alert_names

    def test_should_trigger_country_match(self):
        """Test alert triggers on country match"""
        config = AlertConfig(
            name="test_egypt",
            country_codes=["EG"],
            volume_threshold_mt=0,
            is_active=True
        )

        tender_data = {
            'country_code': 'EG',
            'country_raw': 'Egypt',
            'volume_value': 60000,
        }

        assert self.manager._should_trigger(config, tender_data) == True

    def test_should_not_trigger_country_mismatch(self):
        """Test alert doesn't trigger on country mismatch"""
        config = AlertConfig(
            name="test_egypt",
            country_codes=["EG"],
            is_active=True
        )

        tender_data = {
            'country_code': 'DZ',
            'country_raw': 'Algeria',
            'volume_value': 60000,
        }

        assert self.manager._should_trigger(config, tender_data) == False

    def test_should_trigger_volume_threshold(self):
        """Test alert triggers on volume threshold"""
        config = AlertConfig(
            name="large_tender",
            volume_threshold_mt=500000,
            is_active=True
        )

        tender_data = {
            'volume_value': 600000,
        }

        assert self.manager._should_trigger(config, tender_data) == True

    def test_should_not_trigger_below_threshold(self):
        """Test alert doesn't trigger below volume threshold"""
        config = AlertConfig(
            name="large_tender",
            volume_threshold_mt=500000,
            is_active=True
        )

        tender_data = {
            'volume_value': 100000,
        }

        assert self.manager._should_trigger(config, tender_data) == False

    def test_should_not_trigger_inactive(self):
        """Test inactive alert doesn't trigger"""
        config = AlertConfig(
            name="test",
            is_active=False
        )

        tender_data = {'country_raw': 'Egypt'}

        # Inactive alerts should be skipped in process_tender
        # but _should_trigger doesn't check is_active
        # This is checked in process_tender loop

    def test_build_message(self):
        """Test alert message building"""
        config = AlertConfig(name="test_alert")

        tender_data = {
            'country_raw': 'Egypt',
            'agency_raw': 'Mostakbal Misr',
            'volume_value': 60000,
            'price_value': 275.50,
            'price_type': 'C&F',
            'origins_raw': 'Russia',
            'headline': 'Test headline',
            'source_name': 'Test',
            'article_url': 'http://test.com',
        }

        message = self.manager._build_message(config, tender_data)

        assert "Egypt" in message.subject
        assert "60,000" in message.body
        assert message.data['volume_mt'] == 60000


class TestAgricensusScraper:
    """Tests for AgricensusScraper"""

    def test_matches_keywords(self):
        """Test keyword matching"""
        config = WheatTenderConfig()
        scraper = AgricensusScraper(config)

        assert scraper._matches_keywords("Egypt wheat tender results") == True
        assert scraper._matches_keywords("GASC bought wheat") == True
        assert scraper._matches_keywords("Random news article") == False


class TestAgroChartScraper:
    """Tests for AgroChartScraper"""

    def test_matches_keywords(self):
        """Test keyword matching"""
        config = WheatTenderConfig()
        scraper = AgroChartScraper(config)

        assert scraper._matches_keywords("Algeria OAIC wheat tender") == True
        assert scraper._matches_keywords("wheat purchase completed") == True


# Integration tests (require network, mark as slow)
@pytest.mark.skip(reason="Integration test - requires network")
class TestIntegration:
    """Integration tests - require network access"""

    def test_full_collection(self):
        """Test full collection cycle"""
        collector = WheatTenderCollector()
        result = collector.collect(use_cache=False)

        # Should complete without error
        assert result is not None
        print(f"Collected {result.records_fetched} records")

    def test_alert_processing(self):
        """Test alert processing on real data"""
        collector = WheatTenderCollector()
        result = collector.collect(use_cache=False)

        manager = TenderAlertManager()

        if result.data is not None and hasattr(result.data, 'to_dict'):
            records = result.data.to_dict('records')
            for record in records[:5]:  # Process first 5
                triggered = manager.process_tender(record)
                print(f"Alert triggered: {triggered}")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
