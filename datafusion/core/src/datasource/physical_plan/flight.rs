use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_flight::{FlightClient, FlightInfo};
use arrow_flight::error::FlightError;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use tonic::transport::Channel;

use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;

#[derive(Debug)]
pub struct FlightExec {
    info: FlightInfo,
    origin: String,
    props: PlanProperties,
}

impl FlightExec {
    pub fn new(info: FlightInfo, origin: String) -> datafusion_common::Result<Self> {
        let schema: SchemaRef = Arc::new(info.clone().try_decode_schema()?);
        let partitions = info.endpoint.len();
        Ok(Self {
            info,
            origin,
            props: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::RoundRobinBatch(partitions),
                ExecutionMode::Bounded,
            ),
        })
    }
}

async fn flight_stream(info: FlightInfo, fallback_location: String, schema: SchemaRef, partition: usize) -> Result<SendableRecordBatchStream> {
    let endpoint = &info.endpoint[partition];
    let locations = if endpoint.location.is_empty() {
        vec![fallback_location]
    } else {
        endpoint.location.iter().map(|loc|
        if loc.uri.starts_with("arrow-flight-reuse-connection://") {
            fallback_location.clone()
        } else {
            loc.uri.clone()
        }
        ).collect()
    };
    for loc in locations {
        let Ok(dest) = Channel::from_shared(loc) else { continue };
        let Ok(mut client) = dest.connect().await.map(FlightClient::new) else { continue };
        let Ok(future_stream) = endpoint.ticket.clone()
            .ok_or(DataFusionError::Execution("no flight ticket".to_string()))
            .map(|ticket| client.do_get(ticket)) else { continue };
        let Ok(stream) = future_stream.await else { continue };

        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream.map_err(|e| DataFusionError::External(Box::new(e))),
        )));
    }
    Err(DataFusionError::External(Box::new(FlightError::ProtocolError(
        format!("No available endpoint for partition {}: {:?}", partition, &endpoint.location)
    ))))
}

impl DisplayAs for FlightExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => f.write_str("FlightExec"),
            DisplayFormatType::Verbose => f.write_str("FlightExec with lots of details :)")
        }
    }
}

impl ExecutionPlan for FlightExec {
    fn name(&self) -> &str {
        "flight"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
        let future_stream = flight_stream(self.info.clone(), self.origin.clone(), self.schema(), partition);
        let stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream)))
    }
}
