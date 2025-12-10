class FundamentalAnalyzer:
    """
    Analyzes supply and demand fundamentals
    """
    
    def analyze_balance_sheet(self, commodity, season):
        # Pull WASDE data
        wasde_data = self.get_wasde_data(commodity, season)
        
        # Calculate key metrics
        metrics = {
            'stocks_to_use': wasde_data['ending_stocks'] / wasde_data['total_use'],
            'yield_trend': self.calculate_yield_trend(commodity),
            'demand_growth': self.calculate_demand_growth(commodity),
            'export_pace': self.analyze_export_pace(commodity),
            'production_risk': self.assess_production_risk(commodity)
        }
        
        # Historical comparison
        historical_context = self.compare_to_history(metrics, commodity)
        
        # Generate outlook
        outlook = self.llm_generate_outlook(metrics, historical_context)
        
        return {
            'metrics': metrics,
            'historical_context': historical_context,
            'outlook': outlook,
            'confidence': self.calculate_confidence(metrics)
        }