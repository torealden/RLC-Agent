# core/orchestrator.py
import json
import threading
import schedule
import time
from datetime import datetime
from plugin_manager import PluginManager

class DataOrchestrator:
    def __init__(self, config_file: str = "config/api_sources.json", 
                 db_config: dict = None):
        self.config_file = config_file
        self.db_config = db_config
        self.plugin_manager = PluginManager()
        self.running = False
        
        # Load configuration
        with open(config_file, 'r') as f:
            self.api_configs = json.load(f)
    
    def initialize(self):
        """Initialize all plugins"""
        self.plugin_manager.load_all_plugins(self.api_configs, self.db_config)
        self.setup_schedules()
    
    def setup_schedules(self):
        """Set up scheduled execution for each plugin"""
        for name, plugin in self.plugin_manager.loaded_plugins.items():
            config = self.api_configs[name]
            schedule_type = config.get('schedule', 'manual')
            
            if schedule_type == 'daily':
                schedule.every().day.at("02:00").do(self.run_plugin, name)
            elif schedule_type == 'weekly':
                schedule.every().week.do(self.run_plugin, name)
            elif schedule_type == 'monthly':
                schedule.every().month.do(self.run_plugin, name)
    
    def run_plugin(self, plugin_name: str) -> bool:
        """Run a specific plugin"""
        if plugin_name in self.plugin_manager.loaded_plugins:
            plugin = self.plugin_manager.loaded_plugins[plugin_name]
            print(f"Running {plugin_name} at {datetime.now()}")
            return plugin.run()
        return False
    
    def run_all_plugins(self):
        """Run all enabled plugins"""
        results = {}
        for name, plugin in self.plugin_manager.loaded_plugins.items():
            results[name] = self.run_plugin(name)
        return results
    
    def start_scheduler(self):
        """Start the background scheduler"""
        self.running = True
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        self.running = False