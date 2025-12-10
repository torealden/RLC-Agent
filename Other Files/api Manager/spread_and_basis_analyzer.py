class SpreadAnalyzer:
    """
    Analyzes calendar spreads, crush margins, and basis patterns
    """
    
    def analyze_spreads(self, commodity):
        spreads = {}
        
        # Calendar spreads
        spreads['calendar'] = self.calculate_calendar_spreads(commodity)
        
        # Inter-commodity spreads
        if commodity == 'soybeans':
            spreads['crush'] = self.calculate_crush_margin()
            spreads['bean_corn'] = self.calculate_bean_corn_ratio()
        
        # Basis analysis
        spreads['basis'] = self.analyze_basis_patterns(commodity)
        
        # Identify trading opportunities
        opportunities = self.identify_opportunities(spreads)
        
        return {
            'spreads': spreads,
            'opportunities': opportunities,
            'historical_percentile': self.calculate_percentiles(spreads)
        }