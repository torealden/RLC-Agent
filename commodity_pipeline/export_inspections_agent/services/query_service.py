"""
Query Service
Provides methods for querying export inspection data and generating reports
"""

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union

from sqlalchemy import func, and_, or_, desc, asc
from sqlalchemy.orm import Session

from database.models import (
    InspectionRecord, WeeklyCommoditySummary, WeeklyCountryExports,
    WeeklyRegionExports, WeeklyPortExports, WheatClassExports,
    WeeklyQualityStats, DataLoadLog
)

logger = logging.getLogger(__name__)


class QueryService:
    """
    Service for querying export inspection data
    """
    
    def __init__(self, session: Session, config=None):
        self.session = session
        if config is None:
            from config.settings import default_config
            config = default_config
        self.config = config
    
    # =========================================================================
    # WEEKLY SUMMARY QUERIES
    # =========================================================================
    
    def get_weekly_summary(self, week_ending_date: date, 
                          commodity: str = None) -> List[WeeklyCommoditySummary]:
        """
        Get weekly summary for one or all commodities
        """
        query = self.session.query(WeeklyCommoditySummary).filter(
            WeeklyCommoditySummary.week_ending_date == week_ending_date
        )
        
        if commodity:
            query = query.filter(
                WeeklyCommoditySummary.commodity == commodity.upper()
            )
        
        return query.all()
    
    def get_commodity_trend(self, commodity: str, 
                           weeks: int = 10) -> List[WeeklyCommoditySummary]:
        """
        Get recent weekly trend for a commodity
        """
        return self.session.query(WeeklyCommoditySummary).filter(
            WeeklyCommoditySummary.commodity == commodity.upper()
        ).order_by(
            desc(WeeklyCommoditySummary.week_ending_date)
        ).limit(weeks).all()
    
    def get_marketing_year_progress(self, commodity: str, 
                                    marketing_year: int) -> Dict:
        """
        Get marketing year to date progress for a commodity
        """
        summaries = self.session.query(WeeklyCommoditySummary).filter(
            WeeklyCommoditySummary.commodity == commodity.upper(),
            WeeklyCommoditySummary.marketing_year == marketing_year
        ).order_by(
            WeeklyCommoditySummary.week_ending_date
        ).all()
        
        if not summaries:
            return {}
        
        latest = summaries[-1]
        first = summaries[0]
        
        # Get prior year for comparison
        prior_my = self.session.query(WeeklyCommoditySummary).filter(
            WeeklyCommoditySummary.commodity == commodity.upper(),
            WeeklyCommoditySummary.marketing_year == marketing_year - 1
        ).order_by(
            desc(WeeklyCommoditySummary.week_ending_date)
        ).first()
        
        return {
            'commodity': commodity,
            'marketing_year': marketing_year,
            'weeks_reported': len(summaries),
            'first_week': first.week_ending_date,
            'latest_week': latest.week_ending_date,
            'my_to_date_pounds': latest.my_to_date_pounds,
            'my_to_date_metric_tons': float(latest.my_to_date_metric_tons or 0),
            'my_to_date_bushels': float(latest.total_bushels or 0),
            'latest_week_pounds': latest.total_pounds,
            'prior_year_total': prior_my.my_to_date_pounds if prior_my else None,
            'year_over_year_pct': (
                ((latest.my_to_date_pounds / prior_my.my_to_date_pounds) - 1) * 100
                if prior_my and prior_my.my_to_date_pounds else None
            )
        }
    
    # =========================================================================
    # COUNTRY/REGION QUERIES
    # =========================================================================
    
    def get_top_destinations(self, commodity: str, 
                            week_ending_date: date = None,
                            marketing_year: int = None,
                            limit: int = 10) -> List[Dict]:
        """
        Get top destination countries for a commodity
        """
        query = self.session.query(
            WeeklyCountryExports.destination_country,
            WeeklyCountryExports.destination_region,
            func.sum(WeeklyCountryExports.total_pounds).label('total_pounds'),
            func.sum(WeeklyCountryExports.total_metric_tons).label('total_mt'),
            func.count(WeeklyCountryExports.id).label('weeks_active')
        ).filter(
            WeeklyCountryExports.commodity == commodity.upper()
        )
        
        if week_ending_date:
            query = query.filter(
                WeeklyCountryExports.week_ending_date == week_ending_date
            )
        
        if marketing_year:
            query = query.filter(
                WeeklyCountryExports.marketing_year == marketing_year
            )
        
        results = query.group_by(
            WeeklyCountryExports.destination_country,
            WeeklyCountryExports.destination_region
        ).order_by(
            desc('total_pounds')
        ).limit(limit).all()
        
        return [
            {
                'country': r.destination_country,
                'region': r.destination_region,
                'total_pounds': int(r.total_pounds),
                'total_metric_tons': float(r.total_mt or 0),
                'weeks_active': r.weeks_active
            }
            for r in results
        ]
    
    def get_region_breakdown(self, commodity: str,
                            week_ending_date: date = None,
                            marketing_year: int = None) -> List[Dict]:
        """
        Get exports by destination region
        """
        query = self.session.query(
            WeeklyRegionExports.destination_region,
            func.sum(WeeklyRegionExports.total_pounds).label('total_pounds'),
            func.sum(WeeklyRegionExports.total_metric_tons).label('total_mt'),
            func.sum(WeeklyRegionExports.country_count).label('country_count')
        ).filter(
            WeeklyRegionExports.commodity == commodity.upper()
        )
        
        if week_ending_date:
            query = query.filter(
                WeeklyRegionExports.week_ending_date == week_ending_date
            )
        
        if marketing_year:
            query = query.filter(
                WeeklyRegionExports.marketing_year == marketing_year
            )
        
        results = query.group_by(
            WeeklyRegionExports.destination_region
        ).order_by(
            desc('total_pounds')
        ).all()
        
        total = sum(r.total_pounds for r in results)
        
        return [
            {
                'region': r.destination_region,
                'total_pounds': int(r.total_pounds),
                'total_metric_tons': float(r.total_mt or 0),
                'country_count': r.country_count,
                'percentage': round(r.total_pounds / total * 100, 1) if total else 0
            }
            for r in results
        ]
    
    def get_country_detail(self, country: str, 
                          commodity: str,
                          marketing_year: int = None,
                          weeks: int = None) -> Dict:
        """
        Get detailed export data for a specific country
        """
        query = self.session.query(WeeklyCountryExports).filter(
            WeeklyCountryExports.destination_country.ilike(f'%{country}%'),
            WeeklyCountryExports.commodity == commodity.upper()
        )
        
        if marketing_year:
            query = query.filter(
                WeeklyCountryExports.marketing_year == marketing_year
            )
        
        query = query.order_by(desc(WeeklyCountryExports.week_ending_date))
        
        if weeks:
            query = query.limit(weeks)
        
        results = query.all()
        
        if not results:
            return {}
        
        return {
            'country': results[0].destination_country,
            'region': results[0].destination_region,
            'commodity': commodity,
            'total_pounds': sum(r.total_pounds for r in results),
            'total_metric_tons': sum(float(r.total_metric_tons or 0) for r in results),
            'weeks_count': len(results),
            'weekly_data': [
                {
                    'week': r.week_ending_date,
                    'pounds': r.total_pounds,
                    'metric_tons': float(r.total_metric_tons or 0)
                }
                for r in results
            ]
        }
    
    # =========================================================================
    # PORT QUERIES
    # =========================================================================
    
    def get_port_breakdown(self, commodity: str,
                          week_ending_date: date = None,
                          marketing_year: int = None) -> List[Dict]:
        """
        Get exports by US port region
        """
        query = self.session.query(
            WeeklyPortExports.port_region,
            func.sum(WeeklyPortExports.total_pounds).label('total_pounds'),
            func.sum(WeeklyPortExports.total_metric_tons).label('total_mt'),
            func.count(WeeklyPortExports.id).label('weeks_active')
        ).filter(
            WeeklyPortExports.commodity == commodity.upper()
        )
        
        if week_ending_date:
            query = query.filter(
                WeeklyPortExports.week_ending_date == week_ending_date
            )
        
        if marketing_year:
            query = query.filter(
                WeeklyPortExports.marketing_year == marketing_year
            )
        
        results = query.group_by(
            WeeklyPortExports.port_region
        ).order_by(
            desc('total_pounds')
        ).all()
        
        total = sum(r.total_pounds for r in results)
        
        return [
            {
                'port_region': r.port_region,
                'total_pounds': int(r.total_pounds),
                'total_metric_tons': float(r.total_mt or 0),
                'weeks_active': r.weeks_active,
                'percentage': round(r.total_pounds / total * 100, 1) if total else 0
            }
            for r in results
        ]
    
    # =========================================================================
    # WHEAT CLASS QUERIES
    # =========================================================================
    
    def get_wheat_class_breakdown(self, week_ending_date: date = None,
                                  marketing_year: int = None) -> List[Dict]:
        """
        Get wheat exports by class
        """
        query = self.session.query(
            WheatClassExports.wheat_class,
            func.sum(WheatClassExports.total_pounds).label('total_pounds'),
            func.sum(WheatClassExports.total_metric_tons).label('total_mt'),
            func.avg(WheatClassExports.avg_protein).label('avg_protein'),
            func.avg(WheatClassExports.avg_test_weight).label('avg_tw')
        )
        
        if week_ending_date:
            query = query.filter(
                WheatClassExports.week_ending_date == week_ending_date
            )
        
        if marketing_year:
            query = query.filter(
                WheatClassExports.marketing_year == marketing_year
            )
        
        results = query.group_by(
            WheatClassExports.wheat_class
        ).order_by(
            desc('total_pounds')
        ).all()
        
        total = sum(r.total_pounds for r in results) if results else 0
        
        return [
            {
                'wheat_class': r.wheat_class,
                'total_pounds': int(r.total_pounds),
                'total_metric_tons': float(r.total_mt or 0),
                'avg_protein': float(r.avg_protein or 0),
                'avg_test_weight': float(r.avg_tw or 0),
                'percentage': round(r.total_pounds / total * 100, 1) if total else 0
            }
            for r in results
        ]
    
    # =========================================================================
    # QUALITY QUERIES
    # =========================================================================
    
    def get_quality_stats(self, commodity: str,
                         week_ending_date: date = None,
                         marketing_year: int = None) -> Dict:
        """
        Get quality statistics for a commodity
        """
        query = self.session.query(WeeklyQualityStats).filter(
            WeeklyQualityStats.commodity == commodity.upper(),
            WeeklyQualityStats.destination_region.is_(None)  # Overall stats
        )
        
        if week_ending_date:
            query = query.filter(
                WeeklyQualityStats.week_ending_date == week_ending_date
            )
            stats = query.first()
        elif marketing_year:
            query = query.filter(
                WeeklyQualityStats.marketing_year == marketing_year
            )
            stats_list = query.all()
            
            if not stats_list:
                return {}
            
            # Aggregate quality stats across weeks
            return {
                'commodity': commodity,
                'marketing_year': marketing_year,
                'weeks_count': len(stats_list),
                'total_certificates': sum(s.certificate_count or 0 for s in stats_list),
                'avg_test_weight': sum(
                    (s.test_weight_avg or 0) * (s.certificate_count or 0) 
                    for s in stats_list
                ) / sum(s.certificate_count or 1 for s in stats_list),
                'avg_moisture': sum(
                    (s.moisture_avg or 0) * (s.certificate_count or 0)
                    for s in stats_list
                ) / sum(s.certificate_count or 1 for s in stats_list),
                'avg_protein': sum(
                    (s.protein_avg or 0) * (s.certificate_count or 0)
                    for s in stats_list if s.protein_avg
                ) / sum(s.certificate_count or 1 for s in stats_list if s.protein_avg) if any(s.protein_avg for s in stats_list) else None,
            }
        else:
            stats = query.order_by(
                desc(WeeklyQualityStats.week_ending_date)
            ).first()
        
        if not stats:
            return {}
        
        return {
            'commodity': commodity,
            'week_ending_date': stats.week_ending_date,
            'certificate_count': stats.certificate_count,
            'test_weight': {
                'avg': float(stats.test_weight_avg or 0),
                'min': float(stats.test_weight_min or 0),
                'max': float(stats.test_weight_max or 0)
            },
            'moisture': {
                'avg': float(stats.moisture_avg or 0),
                'min': float(stats.moisture_min or 0),
                'max': float(stats.moisture_max or 0)
            },
            'protein': {
                'avg': float(stats.protein_avg or 0),
                'min': float(stats.protein_min or 0),
                'max': float(stats.protein_max or 0)
            } if stats.protein_avg else None,
            'oil': float(stats.oil_avg or 0) if stats.oil_avg else None,
            'damage': {
                'total': float(stats.total_damage_avg or 0),
                'heat': float(stats.heat_damage_avg or 0)
            },
            'aflatoxin': {
                'tested': stats.aflatoxin_tested_count,
                'avg_ppb': float(stats.aflatoxin_avg_ppb or 0),
                'rejected': stats.aflatoxin_reject_count
            } if stats.aflatoxin_tested_count else None,
            'don': {
                'tested': stats.don_tested_count,
                'avg_ppm': float(stats.don_avg_ppm or 0),
                'rejected': stats.don_reject_count
            } if stats.don_tested_count else None
        }
    
    # =========================================================================
    # RAW DATA QUERIES
    # =========================================================================
    
    def get_raw_inspections(self, 
                           commodity: str = None,
                           week_ending_date: date = None,
                           destination: str = None,
                           port: str = None,
                           limit: int = 100) -> List[InspectionRecord]:
        """
        Query raw inspection records with filters
        """
        query = self.session.query(InspectionRecord)
        
        if commodity:
            query = query.filter(
                func.upper(InspectionRecord.grain) == commodity.upper()
            )
        
        if week_ending_date:
            query = query.filter(
                InspectionRecord.week_ending_date == week_ending_date
            )
        
        if destination:
            query = query.filter(
                InspectionRecord.destination.ilike(f'%{destination}%')
            )
        
        if port:
            query = query.filter(
                InspectionRecord.port.ilike(f'%{port}%')
            )
        
        return query.order_by(
            desc(InspectionRecord.week_ending_date)
        ).limit(limit).all()
    
    # =========================================================================
    # COMPARISON QUERIES
    # =========================================================================
    
    def compare_weeks(self, commodity: str,
                     week1: date, week2: date) -> Dict:
        """
        Compare exports between two weeks
        """
        summary1 = self.session.query(WeeklyCommoditySummary).filter(
            WeeklyCommoditySummary.commodity == commodity.upper(),
            WeeklyCommoditySummary.week_ending_date == week1
        ).first()
        
        summary2 = self.session.query(WeeklyCommoditySummary).filter(
            WeeklyCommoditySummary.commodity == commodity.upper(),
            WeeklyCommoditySummary.week_ending_date == week2
        ).first()
        
        return {
            'commodity': commodity,
            'week1': {
                'date': week1,
                'total_pounds': summary1.total_pounds if summary1 else None,
                'total_metric_tons': float(summary1.total_metric_tons or 0) if summary1 else None
            },
            'week2': {
                'date': week2,
                'total_pounds': summary2.total_pounds if summary2 else None,
                'total_metric_tons': float(summary2.total_metric_tons or 0) if summary2 else None
            },
            'change': {
                'pounds': (summary1.total_pounds - summary2.total_pounds) if (summary1 and summary2) else None,
                'percent': (
                    ((summary1.total_pounds / summary2.total_pounds) - 1) * 100
                    if (summary1 and summary2 and summary2.total_pounds)
                    else None
                )
            }
        }
    
    def compare_marketing_years(self, commodity: str,
                               my1: int, my2: int) -> Dict:
        """
        Compare marketing year to date totals
        """
        progress1 = self.get_marketing_year_progress(commodity, my1)
        progress2 = self.get_marketing_year_progress(commodity, my2)
        
        return {
            'commodity': commodity,
            f'MY{my1}': progress1,
            f'MY{my2}': progress2,
            'yoy_change_percent': (
                ((progress1.get('my_to_date_pounds', 0) / progress2.get('my_to_date_pounds', 1)) - 1) * 100
                if progress1 and progress2 and progress2.get('my_to_date_pounds')
                else None
            )
        }
    
    # =========================================================================
    # UTILITY QUERIES
    # =========================================================================
    
    def get_available_weeks(self, commodity: str = None) -> List[date]:
        """
        Get list of weeks with data
        """
        query = self.session.query(
            WeeklyCommoditySummary.week_ending_date
        ).distinct()
        
        if commodity:
            query = query.filter(
                WeeklyCommoditySummary.commodity == commodity.upper()
            )
        
        results = query.order_by(
            desc(WeeklyCommoditySummary.week_ending_date)
        ).all()
        
        return [r[0] for r in results]
    
    def get_available_commodities(self) -> List[str]:
        """
        Get list of commodities with data
        """
        results = self.session.query(
            WeeklyCommoditySummary.commodity
        ).distinct().all()
        
        return [r[0] for r in results]
    
    def get_latest_week(self) -> Optional[date]:
        """
        Get most recent week with data
        """
        result = self.session.query(
            func.max(WeeklyCommoditySummary.week_ending_date)
        ).scalar()
        return result
