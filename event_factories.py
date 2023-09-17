from event_processors import *

# Factory
class EventProcessorFactory:
    def get_processor(self, machine_type):
        if machine_type == 'car':
            return CarProcessorFactory()
        elif machine_type == 'motorcycle':
            return MotorcycleProcessorFactory()
        elif machine_type == 'ev':
            return EvProcessorFactory()
        elif machine_type == 'fault':
            return FaultProcessorFactory()
        else:
            return None
    
    def create_processor(self):
        pass

# Concrete Factory
class CarProcessorFactory(EventProcessorFactory):
    def create_processor(self):
        return CarEventProcessor()
    
# Concrete Factory
class MotorcycleProcessorFactory(EventProcessorFactory):
    def create_processor(self):
        return MotorcycleEventProcessor()
    
# Concrete Factory
class EvProcessorFactory(EventProcessorFactory):
    def create_processor(self):
        return EvEventProcessor()

# Concrete Factory
class FaultProcessorFactory(EventProcessorFactory):
    def create_processor(self):
        return FaultEventProcessor()