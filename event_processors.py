from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col,from_json

# Abstract processor
class EventProcessor:
    def process_event(self, event_df):
        pass

    def validate_event(self, event_df):
        pass

    def json_parser(self, event_df):
        pass

# Concrete processor
class CarEventProcessor(EventProcessor):
    
    def validate_event(self, event_df):
        return event_df.filter(col("fuel_level") > 0) \
                        .filter(col("fuel_level") < 100) \
                        .filter(col("fuel_consumption") > 0) \
                        .filter(col("engine_hours") > 0) \
                        .filter(col("mileage") > 0)

    def json_parser(self, event_df):

        schema = StructType([
            StructField("machine_type", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("asset_id", IntegerType(), True),
            StructField("event_id", StringType(), True),
            StructField("ts", TimestampType(), True),
            StructField("mileage", IntegerType(), True),
            StructField("fuel_consumption", DecimalType(10, 2), True),
            StructField("engine_hours", DecimalType(10, 2), True),
            StructField("engine_temperature", DecimalType(10, 2), True),
            StructField("oil_pressure", DecimalType(10, 2), True),
            StructField("last_service", TimestampType(), True),
            StructField("current_speed", IntegerType(), True),
            StructField("coolant_level", DecimalType(10, 2), True),
            StructField("fuel_level", DecimalType(10, 2), True),
            StructField("engine_load", DecimalType(10, 2), True)
        ])

        return event_df.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")

    def process_event(self, event_df):
        event_df = self.json_parser(event_df)
        if not self.validate_event(event_df).isEmpty():
            event_df.show()
            event_df.filter(col("machine_type") == "car").write.mode("append").csv("car_event")

# Concrete processor
class MotorcycleEventProcessor(EventProcessor):
    
    def validate_event(self, event_df):
        return event_df.filter(col("fuel_level") > 0) \
                        .filter(col("fuel_level") < 100) \
                        .filter(col("fuel_consumption") > 0) \
                        .filter(col("engine_hours") > 0) \
                        .filter(col("mileage") > 0)

    def json_parser(self, event_df):

        schema = StructType([
            StructField("machine_type", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("asset_id", IntegerType(), True),
            StructField("event_id", StringType(), True),
            StructField("ts", TimestampType(), True),
            StructField("mileage", IntegerType(), True),
            StructField("fuel_consumption", DecimalType(10, 2), True),
            StructField("engine_hours", DecimalType(10, 2), True),
            StructField("engine_temperature", DecimalType(10, 2), True),
            StructField("oil_pressure", DecimalType(10, 2), True),
            StructField("last_service", TimestampType(), True),
            StructField("current_speed", IntegerType(), True),
            StructField("coolant_level", DecimalType(10, 2), True),
            StructField("fuel_level", DecimalType(10, 2), True),
            StructField("engine_load", DecimalType(10, 2), True)
        ])

        return event_df.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")

    def process_event(self, event_df):
        event_df = self.json_parser(event_df)
        if not self.validate_event(event_df).isEmpty():
            event_df.show()
            event_df.filter(col("machine_type") == "motorcycle").write.mode("append").csv("motorcycle_event")

# Concrete processor
class EvEventProcessor(EventProcessor):
    
    def validate_event(self, event_df):
        return event_df.filter(col("battery_level") > 0) \
                        .filter(col("battery_level") < 100) \
                        .filter(col("energy_consumption") > 0) \
                        .filter(col("engine_hours") > 0) \
                        .filter(col("mileage") > 0)

    def json_parser(self, event_df):

        schema = StructType([
            StructField("machine_type", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("asset_id", IntegerType(), True),
            StructField("event_id", StringType(), True),
            StructField("ts", TimestampType(), True),
            StructField("mileage", IntegerType(), True),
            StructField("energy_consumption", DecimalType(10, 2), True),
            StructField("engine_hours", DecimalType(10, 2), True),
            StructField("engine_temperature", DecimalType(10, 2), True),
            StructField("battery_heat", DecimalType(10, 2), True),
            StructField("last_service", TimestampType(), True),
            StructField("current_speed", IntegerType(), True),
            StructField("battery_level", DecimalType(10, 2), True),
            StructField("engine_load", DecimalType(10, 2), True)
        ])

        return event_df.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")

    def process_event(self, event_df):
        event_df = self.json_parser(event_df)
        if not self.validate_event(event_df).isEmpty():
            event_df.show()
            event_df.filter(col("machine_type") == "ev").write.mode("append").csv("ev_event")

# Concrete processor
class FaultEventProcessor(EventProcessor):
    
    def json_parser(self, event_df):

        schema = StructType([
            StructField("machine_type", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("asset_id", IntegerType(), True),
            StructField("event_id", StringType(), True),
            StructField("ts", TimestampType(), True)
            ])

        return event_df.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")

    def process_event(self, event_df):
        event_df = self.json_parser(event_df)
        event_df.show()
        event_df.filter(col("event_type").like("fault%")) \
                .select("machine_type", "asset_id", "event_type", "event_id", "ts") \
                .write.mode("append").csv("fault_notification")
