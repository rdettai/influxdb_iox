//! Tooling to delay certain async actions.

use datafusion::{
    arrow::{
        datatypes::SchemaRef,
        error::{ArrowError, Result as ArrowResult},
        record_batch::RecordBatch,
    },
    physical_plan::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::Stream;
use pin_project::pin_project;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A [`RecordBatchStream`] that is produced by a future.
///
/// This can be used to delay some IO or other operations until the stream is polled.
#[pin_project]
pub struct DelayedRecordBatchStream<F>
where
    F: Future<Output = ArrowResult<SendableRecordBatchStream>>,
{
    schema: SchemaRef,
    done: bool,
    #[pin]
    fut: F,
    #[pin]
    stream: Option<SendableRecordBatchStream>,
}

impl<F> DelayedRecordBatchStream<F>
where
    F: Future<Output = ArrowResult<SendableRecordBatchStream>>,
{
    /// Create new stream from given future and schema.
    ///
    /// The [`RecordBatchStream`] returned by the future must have the same schema as provided here.
    pub fn new(fut: F, schema: SchemaRef) -> Self {
        Self {
            schema,
            done: false,
            fut,
            stream: None,
        }
    }
}

impl<F> Stream for DelayedRecordBatchStream<F>
where
    F: Future<Output = ArrowResult<SendableRecordBatchStream>>,
{
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let this = self.as_mut().project();

            if *this.done {
                return Poll::Ready(None);
            }

            if let Some(stream) = this.stream.as_pin_mut() {
                return stream.poll_next(cx);
            }

            // need to re-project because `this.stream` was moved
            let mut this = self.as_mut().project();

            match this.fut.poll(cx) {
                Poll::Pending => {
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    *this.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Ok(stream)) => {
                    if stream.schema().as_ref() == this.schema.as_ref() {
                        this.stream.as_mut().replace(stream);
                        continue;
                    } else {
                        *this.done = true;
                        return Poll::Ready(Some(Err(ArrowError::SchemaError(
                            "Delayed stream has wrong schema".into(),
                        ))));
                    }
                }
            }
        }
    }
}

impl<F> RecordBatchStream for DelayedRecordBatchStream<F>
where
    F: Future<Output = ArrowResult<SendableRecordBatchStream>>,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl<F> std::fmt::Debug for DelayedRecordBatchStream<F>
where
    F: Future<Output = ArrowResult<SendableRecordBatchStream>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelayedSendableRecordBatchStream")
            .field("schema", &self.schema)
            .field("done", &self.done)
            .field("fut", &"<fut>")
            .field(
                "stream",
                match &self.stream {
                    Some(_) => &"Some(...)",
                    None => &"None",
                },
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::{array::TimestampNanosecondArray, error::ArrowError};
    use futures::StreamExt;
    use schema::{builder::SchemaBuilder, InfluxFieldType};
    use tokio::pin;

    use crate::{stream_from_batches, stream_from_schema};

    use super::*;

    #[tokio::test]
    async fn test_empty() {
        let stream =
            DelayedRecordBatchStream::new(async { Ok(stream_from_schema(schema())) }, schema());
        pin!(stream);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_schema_mismatch_errors() {
        let stream = DelayedRecordBatchStream::new(
            async { Ok(stream_from_schema(other_schema())) },
            schema(),
        );
        pin!(stream);

        assert_eq!(
            stream.next().await.unwrap().unwrap_err().to_string(),
            "Schema error: Delayed stream has wrong schema",
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_fut_errors() {
        let stream = DelayedRecordBatchStream::new(
            async { Err(ArrowError::InvalidArgumentError(String::from("foo"))) },
            schema(),
        );
        pin!(stream);

        assert_eq!(
            stream.next().await.unwrap().unwrap_err().to_string(),
            "Invalid argument error: foo",
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_batches() {
        let stream = DelayedRecordBatchStream::new(
            async { Ok(stream_from_batches(vec![batch(), batch()])) },
            schema(),
        );
        pin!(stream);

        stream.next().await.unwrap().unwrap();
        stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_error_propagation() {
        let stream = DelayedRecordBatchStream::new(
            async { Err(ArrowError::InvalidArgumentError(String::from("foo"))) },
            schema(),
        );
        let stream: SendableRecordBatchStream = Box::pin(stream);
        let stream = DelayedRecordBatchStream::new(async move { Ok(stream) }, schema());
        pin!(stream);

        assert_eq!(
            stream.next().await.unwrap().unwrap_err().to_string(),
            "Invalid argument error: foo",
        );
        assert!(stream.next().await.is_none());
    }

    fn schema() -> SchemaRef {
        SchemaBuilder::new().timestamp().build().unwrap().as_arrow()
    }

    fn other_schema() -> SchemaRef {
        SchemaBuilder::new()
            .influx_field("foo", InfluxFieldType::Boolean)
            .build()
            .unwrap()
            .as_arrow()
    }

    fn batch() -> Arc<RecordBatch> {
        let array = Arc::new(TimestampNanosecondArray::from(vec![Some(1)])) as _;
        Arc::new(RecordBatch::try_new(schema(), vec![array]).unwrap())
    }
}
