class MLPricePredictor:
    """
    ML models for price prediction and pattern recognition
    """
    
    def __init__(self):
        self.models = {
            'lstm': self.build_lstm_model(),
            'random_forest': self.build_rf_model(),
            'xgboost': self.build_xgb_model(),
            'prophet': Prophet()
        }
        self.ensemble = EnsemblePredictor(self.models)
    
    def predict_prices(self, commodity, horizon_days=30):
        # Prepare features
        features = self.prepare_features(commodity)
        
        predictions = {}
        for name, model in self.models.items():
            predictions[name] = model.predict(features, horizon_days)
        
        # Ensemble prediction
        ensemble_pred = self.ensemble.predict(features, horizon_days)
        
        # Calculate confidence intervals
        confidence_intervals = self.calculate_confidence_intervals(predictions)
        
        return {
            'point_forecast': ensemble_pred,
            'confidence_intervals': confidence_intervals,
            'model_predictions': predictions,
            'feature_importance': self.get_feature_importance()
        }