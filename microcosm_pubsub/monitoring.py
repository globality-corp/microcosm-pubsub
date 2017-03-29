from microcosm_monitoring.monitoring import MonitoringInfo

# Metric used to track execution time per message
MONITORING_METRIC_MESSAGE = "microcosm.pubsub.messages"

class MessageMonitoringInfo(MonitoringInfo):

    """
    Used to collect specific information regarding actions on routes

    """

    def __init__(self, media_type):
        super().__init__()
        
        super().update_information('media_type', media_type);
        super().update_information('result', 'unknown');
        self.skipped = False
        
    def on_before(self):
        pass
    
    def on_after(self, response):
        super().update_information('result', 'skipped' if self.skipped else 'success');
    
    def on_error(self, error):
        super().update_information('result', 'failure');
        
    def was_skipped(self):
        self.skipped = True