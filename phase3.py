"""
ETL Service: The current version of ETL service
reads the parquet file from the Kafka stream
and insert its data to the Amazon database.

Pre-req:
a) MySQL connection parameters must be set as environment variables.
b) Configurations must be set in the config.py file.
"""

import os
import sys

sys.path.append(os.getcwd())
import traceback
from datetime import datetime
from json import loads

from opentelemetry.trace import SpanKind, Link
from opentelemetry import trace, propagate
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

import pandas as pd
import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer

from utils import config, utils

table_columns = utils.table_columns()

MYSQL_USER = os.getenv("MYSQL_USER", None)
if not MYSQL_USER:
	raise Exception("MySQL user required!")

MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", None)
if not MYSQL_PASSWORD:
	raise Exception("MySQL password required!")

MYSQL_HOST = os.getenv("MYSQL_HOST", None)
if not MYSQL_HOST:
	raise Exception("MySQL host required!")

MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", None)
if not MYSQL_DATABASE:
	raise Exception("MySQL database required!")

trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer("test-pipeline")

otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318")
otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)

# The BatchSpanProcessor is responsible for batching and exporting spans to the
# OTLP collector endpoint. It sends spans in batches to reduce network overhead.
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))

def consume_and_load(db_config, message):
	"""
    Continuously reads the message from the Kafka stream, extracts the data, and
    stores it in the database.
    :param db_config: MySQL database configuration.
    :param message: Kafka message to process.
    """
	extract_context = trace.get_current_span().get_span_context()
	message = loads(message.value)

	# write output to file at persistent volume 
	with open("/dummy/output.txt", "a") as f:
		f.write(f"Message received: {message}\n")
	f.close()

	ctx = utils.get_parent_context(message["traceId"], message["spanId"])
	with tracer.start_as_current_span(
		'load_phase',               
		context=ctx,
		kind=SpanKind.SERVER,      
		links=[Link(extract_context)]
	) as span:
		try:
			df = pd.DataFrame.from_dict(message["data"])
			# Extract the prefix from the filename (e.g., "product" from "product_0.csv")
			prefix = message['filePrefix']
			
			# Define the required columns for each table based on prefixes
			try:
				# Connect to the MySQL database using the provided configuration
				with mysql.connector.connect(**db_config) as connection:
					with connection.cursor() as cursor:
						if prefix == "product":
							# For Product table, if product_id exists, increment quantity
							for index, row in df.iterrows():
								try:
									product_id = row["product_id"]
									quantity = row["quantity"]
									cursor.execute("SELECT quantity FROM product WHERE product_id = %s", (product_id,))
									existing_quantity = cursor.fetchone()
									if existing_quantity:
										new_quantity = existing_quantity[0] + int(quantity)
										cursor.execute("UPDATE product SET quantity = %s WHERE product_id = %s",
													(new_quantity, product_id))
									else:
										cursor.execute("INSERT INTO product (product_id, product_name, quantity) VALUES (%s, %s, %s)",
													(product_id, row["product_name"], int(quantity)))
								except Error as e:
									span.set_status(Status(StatusCode.ERROR, description="Error in adding/updating product data"))
									print(f"Error in adding/updating product data: {e}")
						elif prefix == "supplier":
							# For Supplier table, if product_id exists, increment supplies_available
							for index, row in df.iterrows():
								try:
									supplier_id = row["supplier_id"]
									product_id = row["product_id"]
									supplies_available = row["supplies_available"]
									cursor.execute("SELECT supplies_available FROM supplier WHERE supplier_id = %s AND product_id = %s",
												(supplier_id, product_id))
									existing_supplies = cursor.fetchone()
									if existing_supplies:
										new_supplies = existing_supplies[0] + int(supplies_available)
										cursor.execute("UPDATE supplier SET supplies_available = %s WHERE supplier_id = %s AND product_id = %s",
													(new_supplies, supplier_id, product_id))
									else:
										cursor.execute("INSERT INTO supplier (supplier_id, supplier_name, product_id, supplies_available) VALUES (%s, %s, %s, %s)",
													(supplier_id, row["supplier_name"], product_id, int(supplies_available)))
								except Error as e:
									span.set_status(Status(StatusCode.ERROR, description="Error in adding/updating supplier data"))
									print(f"Error in adding/updating supplier data: {e}")
						elif prefix == "warehouse":
							# For Warehouse table, if supplier_id and product_id match, increment total_availability
							for index, row in df.iterrows():
								try:
									supplier_id = row["supplier_id"]
									product_id = row["product_id"]
									total_availability = row["total_availability"]
									cursor.execute("SELECT total_availability FROM warehouse WHERE supplier_id = %s AND product_id = %s",
												(supplier_id, product_id))
									existing_availability = cursor.fetchone()
									if existing_availability:
										new_availability = existing_availability[0] + int(total_availability)
										cursor.execute("UPDATE warehouse SET total_availability = %s WHERE supplier_id = %s AND product_id = %s",
													(int(new_availability), supplier_id, product_id))
									else:
										cursor.execute("INSERT INTO warehouse (warehouse_id, warehouse_name, supplier_id, product_id, total_availability) VALUES (%s, %s, %s, %s, %s)",
													(row["warehouse_id"], row["warehouse_name"], supplier_id, product_id, int(total_availability)))
								except Error as e:
									span.set_status(Status(StatusCode.ERROR, description="Error in adding/updating warehouse data"))
									print(f"Error in adding/updating warehouse data: {e}")
						
						connection.commit()
						print(f"Data inserted/updated into {prefix}_table successfully.")

			except Error as e:
				span.set_status(Status(StatusCode.ERROR, description="Error: Unable to connect and add to the database"))
				print(f"Error: {e}")

		except Exception as e:
			span.set_status(Status(StatusCode.ERROR, description="Exception occured when handling data"))
			print(f"Exception occured when handling data: {e}")


def _consume():
	def des(x):
		return loads(x.decode("utf-8"))

	parquet_consumer = KafkaConsumer(
		config.ETL_KAFKA_TOPIC,
		bootstrap_servers=[config.KAFKA_SERVER],
		auto_offset_reset="earliest",
		enable_auto_commit=True,
		group_id="etl-group",
		value_deserializer=des)

	db_config = {
		"host": MYSQL_HOST,
		"user": MYSQL_USER,
		"password": MYSQL_PASSWORD,
		"database": MYSQL_DATABASE,
	}

	print("Starting parquet consumer!")
	print("SQL connection made!")
	while True:
		try:
			for message in parquet_consumer:
				if message is None:
					continue
				else:
					consume_and_load(db_config, message)
		except Exception as e:
			print("Error occurred in ETL service:", str(e))
			print(traceback.print_exc())


# Create appropriate directory structure when the
# script starts.
if __name__ == "__main__":
	print("Start!")
	_consume()