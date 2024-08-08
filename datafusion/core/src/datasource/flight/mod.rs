use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_flight::FlightInfo;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tonic::transport::Channel;

use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_expr::{CreateExternalTable, Expr, TableType};
use datafusion_expr::TableType::Base;
use datafusion_physical_plan::ExecutionPlan;

use crate::datasource::physical_plan::flight::FlightExec;
use crate::datasource::provider::TableProviderFactory;
use crate::datasource::TableProvider;
use crate::execution::context::SessionState;

pub struct FlightDataSource {
    pub driver: Arc<dyn FlightDriver>,
}

#[async_trait]
pub trait FlightDriver: Sync + Send {
    async fn flight_info(&self, channel: Channel, opts: &HashMap<String, String>) -> Result<FlightInfo>;
}

#[async_trait]
impl TableProviderFactory for FlightDataSource {
    async fn create(&self, _state: &SessionState, cmd: &CreateExternalTable) -> Result<Arc<dyn TableProvider>> {
        let channel = Channel::from_shared(cmd.location.clone())
            .unwrap()
            .connect()
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let info = self.driver.flight_info(channel, &cmd.options).await?;
        Ok(Arc::new(FlightTable {
            info,
            origin: cmd.location.clone(),
        }))
    }
}

pub struct FlightTable {
    info: FlightInfo,
    origin: String,
}

#[async_trait]
impl TableProvider for FlightTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(self.info.clone().try_decode_schema().unwrap())
    }

    fn table_type(&self) -> TableType {
        Base
    }

    async fn scan(&self, state: &SessionState, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FlightExec::new(self.info.clone(), self.origin.clone())?))
    }

}
