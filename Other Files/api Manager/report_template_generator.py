class ReportTemplates:
    """
    Manages different report formats for various audiences
    """
    
    def get_template(self, audience_type):
        templates = {
            'executive': {
                'sections': ['summary', 'key_metrics', 'outlook'],
                'length': 'brief',
                'technical_detail': 'low',
                'focus': 'strategic'
            },
            'trader': {
                'sections': ['spreads', 'technicals', 'positions', 'signals'],
                'length': 'detailed',
                'technical_detail': 'high',
                'focus': 'tactical'
            },
            'risk_manager': {
                'sections': ['exposures', 'var', 'scenarios', 'hedges'],
                'length': 'moderate',
                'technical_detail': 'moderate',
                'focus': 'risk'
            },
            'fundamental': {
                'sections': ['supply_demand', 'wasde', 'weather', 'trade'],
                'length': 'comprehensive',
                'technical_detail': 'high',
                'focus': 'fundamental'
            }
        }
        return templates.get(audience_type, templates['executive'])