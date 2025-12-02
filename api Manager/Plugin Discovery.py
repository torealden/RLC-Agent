# core/plugin_manager.py
import importlib
import os
from typing import Dict, List
from base_api import BaseAPISource

class PluginManager:
    def __init__(self, plugin_directory: str = "plugins"):
        self.plugin_directory = plugin_directory
        self.loaded_plugins: Dict[str, BaseAPISource] = {}
    
    def discover_plugins(self) -> List[str]:
        """Discover all plugin files in the plugin directory"""
        plugins = []
        for file in os.listdir(self.plugin_directory):
            if file.endswith("_source.py") and not file.startswith("__"):
                plugin_name = file.replace("_source.py", "")
                plugins.append(plugin_name)
        return plugins
    
    def load_plugin(self, plugin_name: str, config: dict, db_config: dict) -> BaseAPISource:
        """Dynamically load a plugin class"""
        try:
            module_name = f"{self.plugin_directory}.{plugin_name}_source"
            module = importlib.import_module(module_name)
            
            # Convention: class name is PluginNameSource
            class_name = f"{plugin_name.upper()}Source"
            plugin_class = getattr(module, class_name)
            
            return plugin_class(config, db_config)
            
        except Exception as e:
            print(f"Failed to load plugin {plugin_name}: {e}")
            return None
    
    def load_all_plugins(self, api_configs: dict, db_config: dict):
        """Load all enabled plugins"""
        for plugin_name, config in api_configs.items():
            if config.get('enabled', False):
                plugin = self.load_plugin(plugin_name, config, db_config)
                if plugin:
                    self.loaded_plugins[plugin_name] = plugin