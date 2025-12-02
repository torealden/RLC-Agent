class DashboardGenerator:
    """
    Creates interactive dashboards for data exploration
    """
    
    def create_commodity_dashboard(self, commodity):
        # Initialize dashboard
        dashboard = Dashboard(title=f"{commodity.upper()} Market Dashboard")
        
        # Add components
        dashboard.add_component(
            PriceChart(commodity, period='1Y', indicators=['sma', 'bollinger'])
        )
        
        dashboard.add_component(
            SpreadMatrix(commodity, spreads=['calendar', 'basis', 'inter_commodity'])
        )
        
        dashboard.add_component(
            FundamentalTable(commodity, metrics=['stocks_use', 'exports', 'crush'])
        )
        
        dashboard.add_component(
            HeatMap(title='Correlation Matrix', data=self.get_correlations())
        )
        
        # Add interactivity
        dashboard.add_filter('date_range', type='slider')
        dashboard.add_filter('comparison', options=['YoY', 'MoM', '5Y_avg'])
        
        # Export options
        dashboard.enable_export(['pdf', 'excel', 'powerpoint'])
        return dashboard
        