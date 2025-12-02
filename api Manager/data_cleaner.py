class DataCleaningPipeline:
    """
    Comprehensive cleaning pipeline with multiple validation stages
    """
    
    def __init__(self):
        self.stages = [
            SchemaValidator(),      # Ensure data structure
            TypeValidator(),        # Check data types
            RangeValidator(),       # Validate reasonable ranges
            ConsistencyChecker(),   # Cross-reference checks
            DuplicateRemover(),     # Handle duplicates
            OutlierDetector(),      # Flag anomalies
            ImputationEngine()      # Handle missing values
        ]
    
    def process(self, raw_data):
        """
        Process data through all cleaning stages
        """
        cleaned_data = raw_data
        quality_report = {}
        
        for stage in self.stages:
            cleaned_data, stage_report = stage.process(cleaned_data)
            quality_report[stage.name] = stage_report
            
            # Log issues for manual review if needed
            if stage_report['issues_found'] > 0:
                self.log_for_review(stage.name, stage_report)
        
        return cleaned_data, quality_report