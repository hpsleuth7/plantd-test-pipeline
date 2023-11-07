from pathlib import Path
import zipfile
import io
import csv
import sys
import os
import time
from fastapi import FastAPI, UploadFile, File
from json import dumps
from utils import config
from kafka import KafkaProducer
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.status import Status, StatusCode


# Create a FastAPI application instance
app = FastAPI()

# Create a Kafka producer instance for publishing data
producer = KafkaProducer(
	bootstrap_servers=[config.KAFKA_SERVER],
	value_serializer=lambda x: dumps(x).encode("utf-8")
)

# Configure OpenTelemetry tracing for distributed tracing
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer("test-pipeline")

# Configure OTLP exporter for sending trace data
otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))

def process_csv_data(csv_data, context, csv_file_name,):
	"""
	Process CSV data and publish it to Kafka.

	:param csv_data: Binary CSV data to be processed.
	:param context: Current span context for tracing.
	:return: A dictionary indicating the processing status.
	"""
	
	try:
		# Parse CSV data
		csvreader = csv.reader(csv_data.decode('utf-8').splitlines())
		headers = next(csvreader) # we exclude the header since we need to send multiple entries from a single file

		prefix = csv_file_name.split("_")[0].lower()
		# Publish each row of CSV data to Kafka. Trace ID and Span Id help in tracing the record
		# end to end across all the threee phases
		for row in csvreader:
			kafka_data = {
				"filePrefix": prefix,
                "headers": headers,
				"content": row,
				"traceId": context.trace_id,	
				"spanId": context.span_id,
			}
			producer.send(config.CSV_KAFKA_TOPIC, value=kafka_data)
		
		print("CSV data posted to Kafka!")
		return {"status": "Success!", "code": 200}
	except Exception as e:
		# Handle errors and indicate processing failure
		context.span.set_status(Status(StatusCode.ERROR, description="Error processing CSV"))
		print(e, file=sys.stderr)
		return {"status": "Failed to process CSV!", "code": 500}

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
	"""
	Upload endpoint for processing zip files containing CSV data.

	:param file: The uploaded zip file containing CSV data.
	:return: A dictionary indicating the upload status.
	"""
	with tracer.start_as_current_span("extract_phase", kind=SpanKind.SERVER) as span:
		try:
			
			# get current time in seconds
			skip_time = 60 # number of seconds in skip interval
			if (int(time.time()) // skip_time) % 2 == 0:
				print("Skipping time")
				return {"status": "Success: time skip.", "code": 201}
			print("Not skipping time") 
			# do dummy work
			for i in range(10000):
				for j in range(2, i):
					if i % j == 0:
						break

			# Read the binary content of the uploaded zip file
			zip_data = await file.read()

			# Extract and process CSV files from the zip archive
			with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
				for file_info in zip_ref.infolist():
					if file_info.filename.endswith(".csv"):
						context = trace.get_current_span().get_span_context()
						csv_data = zip_ref.read(file_info.filename)
						csv_file_name = file_info.filename
						process_csv_data(csv_data, context, csv_file_name)
			
			# Indicate successful processing
			return {"status": "Success!", "code": 200}
		except Exception as e:
			# Handle errors and indicate upload failure
			span.set_status(Status(StatusCode.ERROR, description="Error during processing of zip file"))
			print(e, file=sys.stderr)
			return {"status": "Failed to upload!", "code": 500}

if __name__ == "__main__":
	# Start the FastAPI application with a single worker
	uvicorn.run(f"{Path(__file__).stem}:app", host="0.0.0.0", port=3000, workers=1)
