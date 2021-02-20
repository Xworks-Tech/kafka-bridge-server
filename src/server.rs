use std::pin::Pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::producer::{Producer, Record, RequiredAcks};

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use bridge::kafka_stream_server::{KafkaStream, KafkaStreamServer};
use bridge::{KafkaResponse, PublishRequest};

pub mod bridge {
    tonic::include_proto!("bridge"); // The string specified here must match the proto package name
}
#[derive(Default)]
pub struct KafkaStreamService {}

pub fn create_kafka_consumer(topic: String) -> Consumer {
    Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic(topic.to_owned())
        .with_fallback_offset(FetchOffset::Latest)
        .with_group("".to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap()
}

pub fn create_kafka_producer() -> Producer {
    Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap()
}

#[tonic::async_trait]
impl KafkaStream for KafkaStreamService {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<KafkaResponse, Status>> + Send + Sync + 'static>>;

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<PublishRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        println!("Initiated stream!");
        let mut stream = request.into_inner();
        let mut producer: Producer = create_kafka_producer();
        let mut stream_topic = String::from("");
        let (tx, rx): (
            mpsc::UnboundedSender<Result<bridge::KafkaResponse, tonic::Status>>,
            mpsc::UnboundedReceiver<Result<bridge::KafkaResponse, tonic::Status>>,
        ) = mpsc::unbounded_channel();
        let (sender, receiver) = oneshot::channel::<Consumer>();

        // Spawn the thread that listens to the client and publishes to kafka
        tokio::spawn(async move {
            let mut sender = Some(sender);
            while let Some(publication) = stream.next().await {
                let message = match publication {
                    Ok(data) => data,
                    Err(_) => panic!("Error decoding publication"),
                };
                let topic = message.topic.clone();
                if let Some(sender) = sender.take() {
                    // Check if a consumer has been created
                    stream_topic = topic.clone();
                    sender.send(create_kafka_consumer(topic)).unwrap();
                }
                // Check if the incoming message had any content to publish
                let content: Vec<u8> = match message.optional_content.clone() {
                    Some(bridge::publish_request::OptionalContent::Content(message_content)) => {
                        message_content
                    }
                    None => vec![],
                };

                // Publish if message had content
                if content.len() > 0 {
                    producer
                        .send(&Record::from_value(&stream_topic, content))
                        .unwrap()
                }
            }
        });

        // Spawn the thread that listens to kafka and writes to the client
        tokio::spawn(async move {
            tokio::select! {
                consumed = receiver =>{
                    let mut consumer = consumed.unwrap();
                    loop {
                        for ms in consumer.poll().unwrap().iter()  {
                            for m in ms.messages() {
                                tx.send(Ok(KafkaResponse {
                                    success: true,
                                    optional_content: Some(
                                        bridge::kafka_response::OptionalContent::Content(Vec::from(
                                            m.value,
                                        )),
                                    ),
                                })).unwrap();
                            }
                            consumer.consume_messageset(ms).unwrap();
                        }
                        consumer.commit_consumed().unwrap();
                    }
                }
            }
        });

        // Pin the Receiver from the mpsc channel in memory to start stream
        Ok(Response::new(
            Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
                as Self::SubscribeStream,
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();

    println!("KafkaService listening on: {}", addr);

    let svc = KafkaStreamServer::new(KafkaStreamService::default());

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
