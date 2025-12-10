class ReportGenerator:
    """
    AI-powered report generation system
    """
    
    def __init__(self):
        self.llm = OllamaClient(model='llama2')  # Can switch to GPT-4
        self.templates = ReportTemplates()
        self.formatter = ReportFormatter()
    
    def generate_daily_report(self):
        """
        Generate comprehensive daily market report
        """
        # Gather all analyses
        analyses = self.gather_analyses()
        
        # Generate sections
        sections = {
            'executive_summary': self.generate_executive_summary(analyses),
            'market_movers': self.identify_market_movers(analyses),
            'fundamental_update': self.summarize_fundamentals(analyses),
            'technical_outlook': self.generate_technical_outlook(analyses),
            'trade_flows': self.analyze_trade_patterns(analyses),
            'weather_impact': self.assess_weather_impact(analyses),
            'forward_outlook': self.generate_outlook(analyses),
            'risks_opportunities': self.identify_risks_opportunities(analyses)
        }
        
        # LLM synthesis
        narrative = self.llm_synthesize_narrative(sections)
        
        # Format report
        formatted_report = self.formatter.format_report(
            narrative,
            include_charts=True,
            format='pdf'
        )
        
        return formatted_report
    
    def llm_synthesize_narrative(self, sections):
        """
        Use LLM to create coherent narrative from analysis sections
        """
        prompt = f"""
        You are a senior commodity analyst. Create a professional market report
        synthesizing the following analyses into a coherent narrative:
        
        {json.dumps(sections, indent=2)}
        
        Focus on:
        1. Key market drivers
        2. Notable changes from yesterday
        3. Implications for traders/hedgers
        4. Risk factors to monitor
        
        Write in a clear, professional tone suitable for institutional clients.
        """
        
        narrative = self.llm.generate(prompt)
        return narrative