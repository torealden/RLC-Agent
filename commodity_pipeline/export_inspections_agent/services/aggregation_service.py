"""
Data Aggregation Service
Creates summary tables from raw inspection records
Handles weekly, country, region, and port-level aggregations
"""

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from sqlalchemy import func, and_, or_, distinct, case
from sqlalchemy.orm import Session

from database.models import (
    InspectionRecord, WeeklyCommoditySummary, WeeklyCountryExports,
    WeeklyRegionExports, WeeklyPortExports, WheatClassExports,
    WeeklyQualityStats, Commodity
)

logger = logging.getLogger(__name__)


class DataAggregationService:
    """
    Aggregates raw inspection records into summary tables
    """
    
    def __init__(self, session: Session, config=None):
        self.session = session
        if config is None:
            from config.settings import default_config
            config = default_config
        self.config = config
    
    def aggregate_week(self, week_ending_date: date, 
                       commodities: List[str] = None) -> Dict[str, int]:
        """
        Run all aggregations for a specific week
        
        Args:
            week_ending_date: The Thursday week-ending date
            commodities: Optional list of commodities to aggregate (default: all)
            
        Returns:
            Dict with counts of records created
        """
        logger.info(f"Running aggregations for week ending {week_ending_date}")
        
        results = {
            'commodity_summary': 0,
            'country_exports': 0,
            'region_exports': 0,
            'port_exports': 0,
            'wheat_class': 0,
            'quality_stats': 0
        }
        
        # Get commodities for this week if not specified
        if commodities is None:
            commodities = self._get_commodities_for_week(week_ending_date)
        
        for commodity in commodities:
            # Weekly commodity summary
            summary = self.aggregate_weekly_commodity(week_ending_date, commodity)
            if summary:
                results['commodity_summary'] += 1
            
            # Country-level exports
            country_count = self.aggregate_weekly_countries(week_ending_date, commodity)
            results['country_exports'] += country_count
            
            # Region-level exports
            region_count = self.aggregate_weekly_regions(week_ending_date, commodity)
            results['region_exports'] += region_count
            
            # Port-level exports
            port_count = self.aggregate_weekly_ports(week_ending_date, commodity)
            results['port_exports'] += port_count
            
            # Quality stats
            quality = self.aggregate_weekly_quality(week_ending_date, commodity)
            if quality:
                results['quality_stats'] += 1
            
            # Wheat-specific class tracking
            if 'WHEAT' in commodity.upper():
                wheat_count = self.aggregate_wheat_classes(week_ending_date)
                results['wheat_class'] += wheat_count
        
        self.session.commit()
        logger.info(f"Aggregation complete for {week_ending_date}: {results}")
        
        return results
    
    def _get_commodities_for_week(self, week_ending_date: date) -> List[str]:
        """Get list of commodities with data for a specific week"""
        result = self.session.query(
            distinct(InspectionRecord.grain)
        ).filter(
            InspectionRecord.week_ending_date == week_ending_date
        ).all()
        
        return [r[0] for r in result if r[0]]
    
    def _get_marketing_year(self, commodity: str, week_date: date) -> int:
        """Calculate marketing year for commodity and date"""
        return self.config.commodities.get_marketing_year(commodity, week_date)
    
    def _get_bushel_weight(self, commodity: str) -> Decimal:
        """Get bushel weight for commodity"""
        return Decimal(str(self.config.commodities.get_bushel_weight(commodity)))
    
    def aggregate_weekly_commodity(self, week_ending_date: date, 
                                   commodity: str) -> Optional[WeeklyCommoditySummary]:
        """
        Create weekly summary for a single commodity
        """
        # Query aggregates
        result = self.session.query(
            func.sum(InspectionRecord.pounds).label('total_pounds'),
            func.sum(InspectionRecord.metric_tons).label('total_mt'),
            func.count(InspectionRecord.id).label('cert_count'),
            func.sum(InspectionRecord.sublot_carriers).label('sublot_count')
        ).filter(
            InspectionRecord.week_ending_date == week_ending_date,
            func.upper(InspectionRecord.grain) == commodity.upper()
        ).first()
        
        if not result or not result.total_pounds:
            return None
        
        total_pounds = int(result.total_pounds)
        marketing_year = self._get_marketing_year(commodity, week_ending_date)
        
        # Calculate bushels
        bushel_weight = self._get_bushel_weight(commodity)
        total_bushels = Decimal(str(total_pounds)) / bushel_weight
        
        # Get prior week data
        prior_week = week_ending_date - timedelta(days=7)
        prior_data = self.session.query(
            func.sum(InspectionRecord.pounds)
        ).filter(
            InspectionRecord.week_ending_date == prior_week,
            func.upper(InspectionRecord.grain) == commodity.upper()
        ).scalar()
        
        # Get year-ago data
        year_ago = week_ending_date - timedelta(days=364)  # Approximate
        year_ago_data = self.session.query(
            func.sum(InspectionRecord.pounds)
        ).filter(
            InspectionRecord.week_ending_date == year_ago,
            func.upper(InspectionRecord.grain) == commodity.upper()
        ).scalar()
        
        # Get marketing year to date
        my_start = self._get_my_start_date(commodity, marketing_year)
        my_to_date = self.session.query(
            func.sum(InspectionRecord.pounds)
        ).filter(
            InspectionRecord.week_ending_date >= my_start,
            InspectionRecord.week_ending_date <= week_ending_date,
            func.upper(InspectionRecord.grain) == commodity.upper()
        ).scalar()
        
        # Create or update summary
        summary = self.session.query(WeeklyCommoditySummary).filter(
            WeeklyCommoditySummary.week_ending_date == week_ending_date,
            WeeklyCommoditySummary.commodity == commodity.upper()
        ).first()
        
        if not summary:
            summary = WeeklyCommoditySummary(
                week_ending_date=week_ending_date,
                commodity=commodity.upper()
            )
            self.session.add(summary)
        
        summary.calendar_year = week_ending_date.year
        summary.marketing_year = marketing_year
        summary.week_number = week_ending_date.isocalendar()[1]
        summary.total_pounds = total_pounds
        summary.total_metric_tons = result.total_mt
        summary.total_bushels = total_bushels
        summary.certificate_count = result.cert_count
        summary.sublot_count = result.sublot_count or 0
        summary.prior_week_pounds = prior_data
        summary.year_ago_pounds = year_ago_data
        summary.my_to_date_pounds = my_to_date
        summary.my_to_date_metric_tons = Decimal(str(my_to_date or 0)) / Decimal('2204.62')
        summary.calculated_at = datetime.utcnow()
        
        return summary
    
    def _get_my_start_date(self, commodity: str, marketing_year: int) -> date:
        """Get marketing year start date for commodity"""
        start_month = self.config.commodities.marketing_year_starts.get(
            commodity.upper(), 9
        )
        return date(marketing_year, start_month, 1)
    
    def aggregate_weekly_countries(self, week_ending_date: date,
                                   commodity: str) -> int:
        """
        Aggregate exports by destination country for a week
        """
        marketing_year = self._get_marketing_year(commodity, week_ending_date)
        bushel_weight = self._get_bushel_weight(commodity)
        
        # Query country-level data
        results = self.session.query(
            InspectionRecord.destination,
            InspectionRecord.destination_region,
            func.sum(InspectionRecord.pounds).label('total_pounds'),
            func.sum(InspectionRecord.metric_tons).label('total_mt'),
            func.count(InspectionRecord.id).label('cert_count')
        ).filter(
            InspectionRecord.week_ending_date == week_ending_date,
            func.upper(InspectionRecord.grain) == commodity.upper()
        ).group_by(
            InspectionRecord.destination,
            InspectionRecord.destination_region
        ).all()
        
        count = 0
        for row in results:
            if not row.destination or not row.total_pounds:
                continue
            
            # Calculate region if not present
            region = row.destination_region or self.config.regions.get_destination_region(
                row.destination
            )
            
            # Get or create record
            export = self.session.query(WeeklyCountryExports).filter(
                WeeklyCountryExports.week_ending_date == week_ending_date,
                WeeklyCountryExports.commodity == commodity.upper(),
                WeeklyCountryExports.destination_country == row.destination
            ).first()
            
            if not export:
                export = WeeklyCountryExports(
                    week_ending_date=week_ending_date,
                    commodity=commodity.upper(),
                    destination_country=row.destination
                )
                self.session.add(export)
            
            export.marketing_year = marketing_year
            export.destination_region = region
            export.total_pounds = int(row.total_pounds)
            export.total_metric_tons = row.total_mt
            export.total_bushels = Decimal(str(row.total_pounds)) / bushel_weight
            export.certificate_count = row.cert_count
            export.calculated_at = datetime.utcnow()
            
            count += 1
        
        return count
    
    def aggregate_weekly_regions(self, week_ending_date: date,
                                 commodity: str) -> int:
        """
        Aggregate exports by destination region for a week
        """
        marketing_year = self._get_marketing_year(commodity, week_ending_date)
        bushel_weight = self._get_bushel_weight(commodity)
        
        # Query region-level data
        results = self.session.query(
            InspectionRecord.destination_region,
            func.sum(InspectionRecord.pounds).label('total_pounds'),
            func.sum(InspectionRecord.metric_tons).label('total_mt'),
            func.count(InspectionRecord.id).label('cert_count'),
            func.count(distinct(InspectionRecord.destination)).label('country_count')
        ).filter(
            InspectionRecord.week_ending_date == week_ending_date,
            func.upper(InspectionRecord.grain) == commodity.upper()
        ).group_by(
            InspectionRecord.destination_region
        ).all()
        
        count = 0
        for row in results:
            if not row.destination_region or not row.total_pounds:
                continue
            
            # Get or create record
            export = self.session.query(WeeklyRegionExports).filter(
                WeeklyRegionExports.week_ending_date == week_ending_date,
                WeeklyRegionExports.commodity == commodity.upper(),
                WeeklyRegionExports.destination_region == row.destination_region
            ).first()
            
            if not export:
                export = WeeklyRegionExports(
                    week_ending_date=week_ending_date,
                    commodity=commodity.upper(),
                    destination_region=row.destination_region
                )
                self.session.add(export)
            
            export.marketing_year = marketing_year
            export.total_pounds = int(row.total_pounds)
            export.total_metric_tons = row.total_mt
            export.total_bushels = Decimal(str(row.total_pounds)) / bushel_weight
            export.certificate_count = row.cert_count
            export.country_count = row.country_count
            export.calculated_at = datetime.utcnow()
            
            count += 1
        
        return count
    
    def aggregate_weekly_ports(self, week_ending_date: date,
                               commodity: str) -> int:
        """
        Aggregate exports by US port region for a week
        """
        marketing_year = self._get_marketing_year(commodity, week_ending_date)
        bushel_weight = self._get_bushel_weight(commodity)
        
        # Query with port region mapping
        # First, get all records and map ports to regions
        records = self.session.query(
            InspectionRecord.port,
            func.sum(InspectionRecord.pounds).label('total_pounds'),
            func.sum(InspectionRecord.metric_tons).label('total_mt'),
            func.count(InspectionRecord.id).label('cert_count')
        ).filter(
            InspectionRecord.week_ending_date == week_ending_date,
            func.upper(InspectionRecord.grain) == commodity.upper()
        ).group_by(
            InspectionRecord.port
        ).all()
        
        # Group by port region
        region_totals = defaultdict(lambda: {
            'total_pounds': 0,
            'total_mt': Decimal('0'),
            'cert_count': 0
        })
        
        for row in records:
            if not row.port:
                continue
            
            region = self.config.regions.get_port_region(row.port)
            region_totals[region]['total_pounds'] += int(row.total_pounds or 0)
            region_totals[region]['total_mt'] += row.total_mt or Decimal('0')
            region_totals[region]['cert_count'] += row.cert_count or 0
        
        count = 0
        for region, totals in region_totals.items():
            if totals['total_pounds'] == 0:
                continue
            
            # Get or create record
            export = self.session.query(WeeklyPortExports).filter(
                WeeklyPortExports.week_ending_date == week_ending_date,
                WeeklyPortExports.commodity == commodity.upper(),
                WeeklyPortExports.port_region == region
            ).first()
            
            if not export:
                export = WeeklyPortExports(
                    week_ending_date=week_ending_date,
                    commodity=commodity.upper(),
                    port_region=region
                )
                self.session.add(export)
            
            export.marketing_year = marketing_year
            export.total_pounds = totals['total_pounds']
            export.total_metric_tons = totals['total_mt']
            export.total_bushels = Decimal(str(totals['total_pounds'])) / bushel_weight
            export.certificate_count = totals['cert_count']
            export.calculated_at = datetime.utcnow()
            
            count += 1
        
        return count
    
    def aggregate_wheat_classes(self, week_ending_date: date) -> int:
        """
        Aggregate wheat exports by class (HRW, HRS, SRW, etc.)
        """
        marketing_year = self._get_marketing_year('WHEAT', week_ending_date)
        bushel_weight = self._get_bushel_weight('WHEAT')
        
        # Query by wheat class
        results = self.session.query(
            InspectionRecord.commodity_class,
            InspectionRecord.destination_region,
            func.sum(InspectionRecord.pounds).label('total_pounds'),
            func.sum(InspectionRecord.metric_tons).label('total_mt'),
            func.count(InspectionRecord.id).label('cert_count'),
            func.avg(InspectionRecord.test_weight).label('avg_tw'),
            func.avg(InspectionRecord.protein_avg).label('avg_protein'),
            func.avg(InspectionRecord.moisture_avg).label('avg_moisture'),
            func.avg(InspectionRecord.dockage_avg).label('avg_dockage'),
            func.avg(InspectionRecord.falling_number).label('avg_fn')
        ).filter(
            InspectionRecord.week_ending_date == week_ending_date,
            func.upper(InspectionRecord.grain).like('%WHEAT%')
        ).group_by(
            InspectionRecord.commodity_class,
            InspectionRecord.destination_region
        ).all()
        
        count = 0
        for row in results:
            if not row.commodity_class or not row.total_pounds:
                continue
            
            # Get or create record
            export = self.session.query(WheatClassExports).filter(
                WheatClassExports.week_ending_date == week_ending_date,
                WheatClassExports.wheat_class == row.commodity_class,
                WheatClassExports.destination_region == row.destination_region
            ).first()
            
            if not export:
                export = WheatClassExports(
                    week_ending_date=week_ending_date,
                    wheat_class=row.commodity_class,
                    destination_region=row.destination_region
                )
                self.session.add(export)
            
            export.marketing_year = marketing_year
            export.total_pounds = int(row.total_pounds)
            export.total_metric_tons = row.total_mt
            export.total_bushels = Decimal(str(row.total_pounds)) / bushel_weight
            export.certificate_count = row.cert_count
            export.avg_test_weight = row.avg_tw
            export.avg_protein = row.avg_protein
            export.avg_moisture = row.avg_moisture
            export.avg_dockage = row.avg_dockage
            export.avg_falling_number = row.avg_fn
            export.calculated_at = datetime.utcnow()
            
            count += 1
        
        return count
    
    def aggregate_weekly_quality(self, week_ending_date: date,
                                 commodity: str) -> Optional[WeeklyQualityStats]:
        """
        Aggregate quality statistics for a commodity
        """
        marketing_year = self._get_marketing_year(commodity, week_ending_date)
        
        # Query quality statistics
        result = self.session.query(
            func.count(InspectionRecord.id).label('cert_count'),
            func.sum(InspectionRecord.pounds).label('total_pounds'),
            func.avg(InspectionRecord.test_weight).label('tw_avg'),
            func.min(InspectionRecord.test_weight).label('tw_min'),
            func.max(InspectionRecord.test_weight).label('tw_max'),
            func.avg(InspectionRecord.moisture_avg).label('m_avg'),
            func.min(InspectionRecord.moisture_low).label('m_min'),
            func.max(InspectionRecord.moisture_high).label('m_max'),
            func.avg(InspectionRecord.total_damage_avg).label('dm_avg'),
            func.avg(InspectionRecord.heat_damage_avg).label('hd_avg'),
            func.avg(InspectionRecord.foreign_material_avg).label('fm_avg'),
            func.avg(InspectionRecord.dockage_avg).label('dkg_avg'),
            func.avg(InspectionRecord.protein_avg).label('pro_avg'),
            func.min(InspectionRecord.protein_low).label('pro_min'),
            func.max(InspectionRecord.protein_high).label('pro_max'),
            func.avg(InspectionRecord.oil_avg).label('oil_avg'),
            func.sum(case((InspectionRecord.aflatoxin_required == True, 1), else_=0)).label('afla_tested'),
            func.avg(InspectionRecord.aflatoxin_avg_ppb).label('afla_avg'),
            func.sum(case((InspectionRecord.aflatoxin_rejected == True, 1), else_=0)).label('afla_rej'),
            func.sum(case((InspectionRecord.don_required == True, 1), else_=0)).label('don_tested'),
            func.avg(InspectionRecord.don_avg_ppm).label('don_avg'),
            func.sum(case((InspectionRecord.don_rejected == True, 1), else_=0)).label('don_rej')
        ).filter(
            InspectionRecord.week_ending_date == week_ending_date,
            func.upper(InspectionRecord.grain) == commodity.upper()
        ).first()
        
        if not result or not result.cert_count:
            return None
        
        # Get or create record
        stats = self.session.query(WeeklyQualityStats).filter(
            WeeklyQualityStats.week_ending_date == week_ending_date,
            WeeklyQualityStats.commodity == commodity.upper(),
            WeeklyQualityStats.destination_region.is_(None)
        ).first()
        
        if not stats:
            stats = WeeklyQualityStats(
                week_ending_date=week_ending_date,
                commodity=commodity.upper(),
                destination_region=None  # Overall stats
            )
            self.session.add(stats)
        
        stats.marketing_year = marketing_year
        stats.certificate_count = result.cert_count
        stats.total_pounds = result.total_pounds
        stats.test_weight_avg = result.tw_avg
        stats.test_weight_min = result.tw_min
        stats.test_weight_max = result.tw_max
        stats.moisture_avg = result.m_avg
        stats.moisture_min = result.m_min
        stats.moisture_max = result.m_max
        stats.total_damage_avg = result.dm_avg
        stats.heat_damage_avg = result.hd_avg
        stats.foreign_material_avg = result.fm_avg
        stats.dockage_avg = result.dkg_avg
        stats.protein_avg = result.pro_avg
        stats.protein_min = result.pro_min
        stats.protein_max = result.pro_max
        stats.oil_avg = result.oil_avg
        stats.aflatoxin_tested_count = result.afla_tested
        stats.aflatoxin_avg_ppb = result.afla_avg
        stats.aflatoxin_reject_count = result.afla_rej
        stats.don_tested_count = result.don_tested
        stats.don_avg_ppm = result.don_avg
        stats.don_reject_count = result.don_rej
        stats.calculated_at = datetime.utcnow()
        
        return stats
    
    def aggregate_all_weeks(self, start_date: date = None, 
                           end_date: date = None) -> Dict[str, int]:
        """
        Run aggregations for all weeks in date range
        """
        if start_date is None:
            # Get earliest week in database
            result = self.session.query(
                func.min(InspectionRecord.week_ending_date)
            ).scalar()
            start_date = result or date.today()
        
        if end_date is None:
            result = self.session.query(
                func.max(InspectionRecord.week_ending_date)
            ).scalar()
            end_date = result or date.today()
        
        # Get distinct weeks
        weeks = self.session.query(
            distinct(InspectionRecord.week_ending_date)
        ).filter(
            InspectionRecord.week_ending_date >= start_date,
            InspectionRecord.week_ending_date <= end_date
        ).order_by(
            InspectionRecord.week_ending_date
        ).all()
        
        total_results = defaultdict(int)
        
        for (week_date,) in weeks:
            results = self.aggregate_week(week_date)
            for key, value in results.items():
                total_results[key] += value
        
        logger.info(f"Aggregated {len(weeks)} weeks: {dict(total_results)}")
        return dict(total_results)
    
    def recalculate_marketing_year_totals(self, marketing_year: int,
                                          commodity: str) -> int:
        """
        Recalculate all MY-to-date totals for a marketing year
        """
        my_start = self._get_my_start_date(commodity, marketing_year)
        
        # Get all weeks in this marketing year
        weeks = self.session.query(
            WeeklyCommoditySummary
        ).filter(
            WeeklyCommoditySummary.marketing_year == marketing_year,
            WeeklyCommoditySummary.commodity == commodity.upper()
        ).order_by(
            WeeklyCommoditySummary.week_ending_date
        ).all()
        
        running_total = 0
        for summary in weeks:
            running_total += summary.total_pounds or 0
            summary.my_to_date_pounds = running_total
            summary.my_to_date_metric_tons = Decimal(str(running_total)) / Decimal('2204.62')
        
        self.session.commit()
        return len(weeks)
