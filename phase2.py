import os
import traceback
import uuid
from json import loads, dumps
from kafka import KafkaConsumer, KafkaProducer
from pandas import DataFrame
from opentelemetry.trace import SpanKind, Link
from opentelemetry import trace
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from multiprocessing import Process
import threading
from queue import Queue
from utils import config, utils

# Define column names for different types of inventory
table_columns = utils.table_columns()

# Set up OpenTelemetry tracing
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer("test-pipeline")
# Configure OTLP exporter for tracing
otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318")
otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))


def validate_record(df, prefix):
	try:
		# Extract the prefix from the filename (e.g., "product" from "product_0.csv")
		if prefix not in table_columns:
			print("prefix not found in table column - " + prefix)
			return False

		# Check if the DataFrame has the required columns
		required_columns = table_columns[prefix]
		missing_columns = set(required_columns) - set(df.columns)
		if missing_columns:
			print("Error: Missing columns in DataFrame:")
			return False

		# Validate data types and values for each row in the DataFrame.
		for _, row in df.iterrows():

			if prefix == "supplier":
				supplier_id = uuid.UUID(row["supplier_id"])
				product_id = uuid.UUID(row["product_id"])
				supplier_name = str(row["supplier_name"])
				supplies_available = int(row["supplies_available"])

				# Check if the data meets the validation criteria.
				if not (supplier_id and supplier_name and product_id and supplies_available >= 0):
					return False

			elif prefix == "product":
				product_id = uuid.UUID(row["product_id"])
				product_name = str(row["product_name"])
				quantity = int(row["quantity"])

				# Check if the data meets the validation criteria.
				if not (product_id and product_name and quantity >= 0):
					return False

			elif prefix == "warehouse":
				warehouse_id = uuid.UUID(row["warehouse_id"])
				warehouse_name = str(row["warehouse_name"])
				product_id = uuid.UUID(row["product_id"])
				supplier_id = uuid.UUID(row["supplier_id"])
				total_availability = int(row["total_availability"])

				# Check if the data meets the validation criteria.
				if not (warehouse_id and warehouse_name and product_id and supplier_id and total_availability >= 0):
					return False

		# All rows passed validation.
		return True

	except (ValueError, KeyError):
		return False



def transform_data(csv_consumer, consumer_queue, etl_producer):
	"""Process CSV data from Kafka and send it as Parquet to another Kafka topic."""
	try:
		messageQ = consumer_queue.get(timeout=60)
		message = messageQ.value
		
		extract_context = trace.get_current_span().get_span_context()
		ctx = utils.get_parent_context(message["traceId"], message["spanId"])
		with tracer.start_as_current_span(
			'transform_phase',  
			context=ctx,
			kind=SpanKind.SERVER,
			links=[Link(extract_context)]
		) as span:
			try:
				content = message["content"]
				df = DataFrame(content).transpose()
				df.columns = message['headers']
				
				is_data_valid = validate_record(df, message['filePrefix'])

				if not is_data_valid:
					span.set_status(Status(StatusCode.ERROR, description="Data is not valid"))
					print("Data is invalid")

				kafka_data = {
					"filePrefix": message['filePrefix'],
					"data": df.to_dict(),
					"traceId": message["traceId"],	
					"spanId": message["spanId"],	
				}
				etl_producer.send(config.ETL_KAFKA_TOPIC,
								value=dumps(kafka_data))
			except Exception as e:
				span.set_status(Status(StatusCode.ERROR, description="Error when converting the data to Dataframe"))
				print(f"Error when converting the data to Dataframe: {e}")
			finally:
				consumer_queue.task_done()
				csv_consumer.commit()
	except:
		span.set_status(Status(StatusCode.ERROR, description="Error parsing the message from extract"))
		print(f"Error parsing the message from extract: {e}")

def _consume(settings):
	"""Kafka consumer process for processing CSV data."""
	csv_consumer = KafkaConsumer(
		config.CSV_KAFKA_TOPIC,
		bootstrap_servers=[config.KAFKA_SERVER],
		auto_offset_reset="earliest",
		enable_auto_commit=False,
		group_id="transform-group",
		value_deserializer=lambda x: loads(x.decode("utf-8")))
	
	consumer_queue = Queue(maxsize=settings['num_threads'])

	etl_producer = KafkaProducer(
		bootstrap_servers=[config.KAFKA_SERVER],
		value_serializer=lambda x: dumps(x).encode("utf-8"),
		batch_size=20,
		linger_ms=2500
	)

	while True:
		try:
			for message in csv_consumer:
				if message is None:
					continue
				else:
					consumer_queue.put(message)
					t = threading.Thread(target=transform_data, args=(csv_consumer, consumer_queue, etl_producer))
					t.start()
		except Exception as e:
			print("Error occurred in phase 2:", str(e))
			print(traceback.print_exc())

def main(settings):
	"""Main function to start consumer workers."""
	workers = []
	while True:
		num_alive = len([w for w in workers if w.is_alive()])
		if settings['num_workers'] == num_alive:
			continue
		for _ in range(settings['num_workers'] - num_alive):
			p = Process(target=_consume, daemon=True, args=(settings,))
			p.start()
			workers.append(p)
			print("Starting worker: " + str(p.pid))

if __name__ == "__main__":
	print("Starting up the transform stage")

	main(settings={
		'num_workers': 1,
		'num_threads': 1,
	})