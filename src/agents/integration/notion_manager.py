class NotionIntegration:
    """
    Maintains living documentation in Notion
    """
    
    def __init__(self, token, database_ids):
        self.client = NotionClient(token)
        self.databases = database_ids
    
    def document_process(self, process_name, steps, outcomes):
        """
        Create or update process documentation
        """
        page = {
            'title': process_name,
            'properties': {
                'Last Updated': datetime.now().isoformat(),
                'Status': 'Active',
                'Automation Level': self.get_automation_level(process_name)
            },
            'content': self.format_process_steps(steps, outcomes)
        }
        
        # Check if page exists
        existing = self.find_page(process_name)
        if existing:
            self.client.update_page(existing['id'], page)
        else:
            self.client.create_page(self.databases['processes'], page)
    
    def log_analysis_insight(self, insight):
        """
        Add new market insight to knowledge base
        """
        self.client.create_page(
            self.databases['insights'],
            {
                'title': insight['title'],
                'commodity': insight['commodity'],
                'date': insight['date'],
                'content': insight['analysis'],
                'confidence': insight['confidence'],
                'tags': insight['tags']
            }
        )