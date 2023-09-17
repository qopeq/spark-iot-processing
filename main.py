from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json

from event_factories import *

class MainProcessor:
    def __init__(self):
        
        carEventProcessorFactory = EventProcessorFactory().get_factory("car")
        self.carEventProcessor = carEventProcessorFactory.create_processor()

        motorcycleEventProcessorFactory = EventProcessorFactory().get_factory("motorcycle")
        self.motorcycleEventProcessor = motorcycleEventProcessorFactory.create_processor()
        
        evEventProcessorFactory = EventProcessorFactory().get_factory("ev")
        self.evEventProcessor = evEventProcessorFactory.create_processor()
        
        faultEventProcessorFactory = EventProcessorFactory().get_factory("fault")
        self.faultEventProcessor = faultEventProcessorFactory.create_processor()

    def process_events(self, event_df, batch_id):
        event_df.show()
        print(event_df.isEmpty())
        if not event_df.isEmpty():
           self.carEventProcessor.process_event(event_df)
           self.motorcycleEventProcessor.process_event(event_df)
           self.evEventProcessor.process_event(event_df)
           self.faultEventProcessor.process_event(event_df)

spark = SparkSession.builder.appName("EventProcessing").getOrCreate()
ssc = StreamingContext(spark.sparkContext, batchDuration=2)
mainProcessor = MainProcessor()

# Using socketTextStream for testing
# Connect to stream using nc -lk 9999
event_file_stream = spark.readStream \
                        .format("socket") \
                        .option("host", "localhost") \
                        .option("port", 9999) \
                        .load()

# Process the events
stream = event_file_stream.writeStream.foreachBatch(mainProcessor.process_events).start()

stream.awaitTermination()
