class IntelligentValidator:
    """
    Uses historical patterns and ML to validate incoming data
    """
    
    def validate_price_data(self, commodity, price, date):
        # Check against historical ranges
        historical_stats = self.get_historical_stats(commodity, date)
        
        # Calculate z-score
        z_score = abs(price - historical_stats['mean']) / historical_stats['std']
        
        if z_score > 3:
            # Flag for manual review
            return ValidationResult(
                valid=False,
                confidence=0.2,
                reason=f"Price {price} is {z_score:.1f} std devs from mean",
                suggested_action="manual_review"
            )
        
        # Check against correlated commodities
        correlations = self.check_correlations(commodity, price, date)
        if correlations['anomaly_score'] > 0.8:
            return ValidationResult(
                valid=False,
                confidence=0.5,
                reason="Price diverges from correlated commodities",
                suggested_action="verify_source"
            )
        
        return ValidationResult(valid=True, confidence=0.95)