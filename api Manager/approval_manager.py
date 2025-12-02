class ApprovalManager:
    """
    Manages gradual transition to full automation
    """
    
    def __init__(self):
        self.automation_levels = {
            'manual': 0,        # Everything needs approval
            'assisted': 1,      # Suggestions provided
            'supervised': 2,    # Auto-execute with review
            'autonomous': 3     # Fully automated
        }
        self.current_levels = self.load_automation_settings()
    
    def should_auto_execute(self, task_type, confidence):
        """
        Determine if task should run automatically
        """
        level = self.current_levels.get(task_type, 0)
        
        if level == 0:  # Manual
            return False, "Manual approval required"
        
        elif level == 1:  # Assisted
            if confidence > 0.95:
                return False, f"Suggested action (confidence: {confidence:.0%})"
            return False, "Low confidence, manual review needed"
        
        elif level == 2:  # Supervised
            if confidence > 0.8:
                self.log_for_review(task_type, "Auto-executed, please review")
                return True, "Auto-executed with review"
            return False, "Confidence too low for auto-execution"
        
        else:  # Autonomous
            if confidence > 0.6:
                return True, "Fully automated execution"
            return False, "Unusual condition detected, manual review"
    
    def graduate_automation(self, task_type, success_rate):
        """
        Gradually increase automation level based on performance
        """
        if success_rate > 0.95 and self.get_run_count(task_type) > 100:
            current = self.current_levels.get(task_type, 0)
            if current < 3:
                self.current_levels[task_type] = current + 1
                self.notify(f"Task {task_type} promoted to level {current + 1}")