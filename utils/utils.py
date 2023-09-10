import datetime

from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry import trace


def table_columns():
	return  {
		"product": ["product_id", "product_name", "quantity"],
		"supplier": ["supplier_id", "supplier_name", "product_id", "supplies_available"],
		"warehouse": ["warehouse_id", "warehouse_name", "supplier_id", "product_id", "total_availability"],
	}
def get_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def get_parent_context(trace_id, span_id):
	"""Create a parent context for tracing spans."""

	
	parent_context = SpanContext(
		trace_id=trace_id,
		span_id=span_id,
		is_remote=True,
		trace_flags=TraceFlags(0x01)
	)
	return trace.set_span_in_context(NonRecordingSpan(parent_context))