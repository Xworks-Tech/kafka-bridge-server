use std::env;
use std::pin::Pin;
use std::time::Duration;

extern crate pretty_env_logger;
#[macro_use] extern crate log;

use futures::{Stream, StreamExt};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use bridge::kafka_stream_server::{KafkaStream, KafkaStreamServer};
use bridge::{KafkaResponse, PublishRequest};

pub fn get_broker() -> String {
    let host = env::var("KAFKA_HOST").unwrap();
    let port = env::var("KAFKA_PORT").unwrap();
    format!("{}:{}", host, port)
}
pub mod bridge {
    tonic::include_proto!("bridge"); // The string specified here must match the proto package name
}
#[derive(Default)]
pub struct KafkaStreamService {}

pub fn create_kafka_consumer(topic: String) -> StreamConsumer {
    let client: StreamConsumer = ClientConfig::new()
        .set("group.id", "honne")
        .set("bootstrap.servers", get_broker())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("allow.auto.create.topics", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    client.subscribe(&vec![&topic[..]]).unwrap();
    client
}

pub fn create_kafka_producer() -> FutureProducer {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_broker())
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    producer
}

#[tonic::async_trait]
impl KafkaStream for KafkaStreamService {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<KafkaResponse, Status>> + Send + Sync + 'static>>;

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<PublishRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        info!("Initiated stream");

        let producer: FutureProducer = create_kafka_producer();
        let mut stream = request.into_inner();
        let mut stream_topic = String::from("");
        let (tx, rx): (
            mpsc::UnboundedSender<Result<bridge::KafkaResponse, tonic::Status>>,
            mpsc::UnboundedReceiver<Result<bridge::KafkaResponse, tonic::Status>>,
        ) = mpsc::unbounded_channel();
        let (sender, receiver) = oneshot::channel::<StreamConsumer>();

        // Spawn the thread that listens to the client and publishes to kafka
        tokio::spawn(async move {
            let mut sender = Some(sender);
            while let Some(publication) = stream.next().await {
                let message = match publication {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("Error initialising client listener: {}", e);
                        break;
                    }
                };
                let topic = message.topic.clone();
                if let Some(sender) = sender.take() {
                    // Check if a consumer has been created
                    stream_topic = topic.clone();
                    match sender.send(create_kafka_consumer(topic)) {
                        Ok(_) => (),
                        Err(_) => {
                            warn!("Error sending content to broker on topic: {}", stream_topic);
                            break;
                        }
                    };
                }
                // Check if the incoming message had any content to publish
                let content: Vec<u8> = match message.optional_content.clone() {
                    Some(bridge::publish_request::OptionalContent::Content(message_content)) => {
                        message_content
                    }
                    None => continue,
                };

                producer
                    .send::<Vec<u8>, _, _>(
                        FutureRecord::to(&stream_topic)
                            .payload(&String::from_utf8(content).unwrap()),
                        Duration::from_secs(0),
                    )
                    .await
                    .unwrap();
            }
        });

        // Spawn the thread that listens to kafka and writes to the client
        tokio::spawn(async move {
            tokio::select! {
                consumed = receiver =>{
                    let consumer = match consumed{
                        Ok(cons) =>cons,
                        Err(e) => {
                            warn!("Error retrieving consumer from mpsc channel: {}",e);
                            return
                        }
                    };
                    loop {
                        match consumer.recv().await {
                            Err(e) => {
                                warn!("Error consuming from kafka broker: {}", e);
                                break},
                            Ok(message) => {
                                let payload = match message.payload_view::<str>() {
                                    None => "",
                                    Some(Ok(s)) => s,
                                    Some(Err(e)) => {
                                        warn!("Error viewing payload contents: {}", e);
                                        ""
                                    }
                                };
                                if payload.len() > 0 {
                                    tx.send(Ok(KafkaResponse {
                                        success: true,
                                        optional_content: Some(
                                            bridge::kafka_response::OptionalContent::Content(payload.as_bytes().to_vec()),
                                        ),
                                    })).unwrap();
                                }
                            }
                        }
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
    pretty_env_logger::init();
    let addr = "[::0]:50051".parse().unwrap();

    info!( "KafkaService listening on: {}", addr);

    let svc = KafkaStreamServer::new(KafkaStreamService::default());

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
