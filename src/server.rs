use std::env;
use std::pin::Pin;
use std::time::Duration;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

use futures::{Stream, StreamExt};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};

use futures::executor::block_on;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use bridge::kafka_stream_server::{KafkaStream, KafkaStreamServer};
use bridge::{ConsumeRequest, KafkaResponse, ProduceResponse, PublishRequest};

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
        .set("session.timeout.ms", "60000")
        .set("enable.auto.commit", "true")
        .set("allow.auto.create.topics", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    client.subscribe(&vec![&topic[..]]).unwrap();
    info!("Consumer successfully subscribed to {}", topic);
    client
}

pub fn create_kafka_producer() -> FutureProducer {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", get_broker())
        .set("message.timeout.ms", "60000")
        .create()
        .expect("Producer creation failed");
    producer
}

pub fn print_metadata(brokers: &str, topic: Option<&str>, timeout: Duration, fetch_offsets: bool) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Consumer creation failed");

    trace!("Consumer created");

    let metadata = consumer
        .fetch_metadata(topic, timeout)
        .expect("Failed to fetch metadata");

    let mut message_count = 0;

    println!("Cluster information:");
    println!("  Broker count: {}", metadata.brokers().len());
    println!("  Topics count: {}", metadata.topics().len());
    println!("  Metadata broker name: {}", metadata.orig_broker_name());
    println!("  Metadata broker id: {}\n", metadata.orig_broker_id());

    println!("Brokers:");
    for broker in metadata.brokers() {
        println!(
            "  Id: {}  Host: {}:{}  ",
            broker.id(),
            broker.host(),
            broker.port()
        );
    }

    println!("\nTopics:");
    for topic in metadata.topics() {
        println!("  Topic: {}  Err: {:?}", topic.name(), topic.error());
        for partition in topic.partitions() {
            println!(
                "     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                partition.id(),
                partition.leader(),
                partition.replicas(),
                partition.isr(),
                partition.error()
            );
            if fetch_offsets {
                let (low, high) = consumer
                    .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                    .unwrap_or((-1, -1));
                println!(
                    "       Low watermark: {}  High watermark: {} (difference: {})",
                    low,
                    high,
                    high - low
                );
                message_count += high - low;
            }
        }
        if fetch_offsets {
            println!("     Total message count: {}", message_count);
        }
    }
}

#[tonic::async_trait]
impl KafkaStream for KafkaStreamService {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<KafkaResponse, Status>> + Send + Sync + 'static>>;

    type ConsumeStream =
        Pin<Box<dyn Stream<Item = Result<KafkaResponse, Status>> + Send + Sync + 'static>>;

    type ProduceStream =
        Pin<Box<dyn Stream<Item = Result<ProduceResponse, Status>> + Send + Sync + 'static>>;

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<PublishRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        info!("Initiated read-write stream");

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
                        error!("Error initialising client listener: {}", e);
                        break;
                    }
                };
                info!("Received message");
                let topic = message.topic.clone();
                if let Some(sender) = sender.take() {
                    // Check if a consumer has been created
                    stream_topic = topic.clone();
                    match sender.send(create_kafka_consumer(topic)) {
                        Ok(_) => (),
                        Err(_) => {
                            error!("Error sending content to broker on topic: {}", stream_topic);
                            break;
                        }
                    };
                }
                // Check if the incoming message had any content to publish
                let content: String = match message.optional_content.clone() {
                    Some(bridge::publish_request::OptionalContent::Content(message_content)) => {
                        String::from_utf8(message_content).unwrap()
                    }
                    None => {
                        warn!("No content detected in PublishRequest");
                        continue;
                    }
                };
                info!("Writing ({}) on topic {} to broker", content, stream_topic);
                // TODO: better error checking around here
                producer
                    .send::<Vec<u8>, _, _>(
                        FutureRecord::to(&stream_topic).payload(&content),
                        Duration::from_secs(0),
                    )
                    .await
                    .unwrap();
                info!("Published to topic: {}", stream_topic)
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
                                error!("Error consuming from kafka broker: {}", e);
                                break},
                            Ok(message) => {
                                let payload = match message.payload_view::<str>() {
                                    None => "",
                                    Some(Ok(s)) => s,
                                    Some(Err(e)) => {
                                        error!("Error viewing payload contents: {}", e);
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
                                } else {
                                    warn!("No content detected in payload from broker");
                                }
                                consumer.commit_message(&message, CommitMode::Async).unwrap();
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

    async fn consume(
        &self,
        request: Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        info!("Initiated read-only stream");
        tokio::spawn(async move {
            let message = match Some(request.get_ref()) {
                Some(x) => x,
                None => return,
            };
            let topic = message.topic.clone();
            info!("Consuming on topic: {}", topic);
            let consumer = create_kafka_consumer(topic);
            loop {
                let result = consumer.stream().next().await;
                match result {
                    None => {
                        warn!("Received none-type from consumer stream");
                        continue;
                    }
                    Some(Err(e)) => {
                        error!("Error consuming from kafka broker: {:?}", e);
                        continue;
                    }
                    Some(Ok(message)) => {
                        let payload = match message.payload_view::<str>() {
                            None => {
                                warn!("Recived none-type when unpacking payload");
                                continue;
                            }
                            Some(Ok(s)) => {
                                info!("Received payload: {:?}", s);
                                s
                            }
                            Some(Err(e)) => {
                                error!("Error viewing payload contents: {}", e);
                                return;
                            }
                        };
                        info!("Received message from broker in read-only stream");
                        if payload.len() > 0 {
                            info!("Sending payload {:?}", payload);
                            match block_on(tx.send(Ok(KafkaResponse {
                                success: true,
                                optional_content: Some(
                                    bridge::kafka_response::OptionalContent::Content(
                                        (*payload).as_bytes().to_vec(),
                                    ),
                                ),
                            }))) {
                                Ok(_) => info!("Successfully sent payload to client"),
                                Err(e) => {
                                    trace!("GRPC error sending message to client {:?}", e);
                                    return;
                                }
                            }
                        } else {
                            warn!("No content detected in payload from broker");
                        }
                        match consumer.commit_message(&message, CommitMode::Async) {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error commiting a consumed message: {:?}", e);
                                return;
                            }
                        }
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn produce(
        &self,
        request: Request<tonic::Streaming<PublishRequest>>,
    ) -> Result<Response<Self::ProduceStream>, Status> {
        let producer: FutureProducer = create_kafka_producer();
        let mut stream = request.into_inner();
        let (tx, rx): (
            mpsc::UnboundedSender<Result<bridge::ProduceResponse, tonic::Status>>,
            mpsc::UnboundedReceiver<Result<bridge::ProduceResponse, tonic::Status>>,
        ) = mpsc::unbounded_channel();

        info!("Initiated write-only stream");

        tokio::spawn(async move {
            let ack = |content: String, success: bool| {
                match tx.send(Ok(ProduceResponse {
                    success: success,
                    message: Some(bridge::produce_response::Message::Content(format!(
                        "{}",
                        content
                    ))),
                })) {
                    Ok(_) => true,
                    Err(e) => {
                        error!("Error acking client: {}", e);
                        false
                    }
                };
            };
            while let Some(publication) = stream.next().await {
                let message = match publication {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Error receiving message from client: {}", e);
                        ack((format!("{:?}", e)), false);
                        break;
                    }
                };
                info!("Received message");
                let topic = message.topic.clone();
                // Check if the incoming message had any content to publish
                let content: String = match message.optional_content.clone() {
                    Some(bridge::publish_request::OptionalContent::Content(message_content)) => {
                        String::from_utf8(message_content).unwrap()
                    }
                    None => {
                        warn!("No content detected in PublishRequest from client");
                        ack("Error unwrapping content".to_string(), false);
                        continue;
                    }
                };
                info!("Writing ({}) on topic {} to broker", content, topic);
                let action = producer.send::<Vec<u8>, _, _>(
                    FutureRecord::to(&topic).payload(&content),
                    Duration::from_secs(0),
                );
                match action.await {
                    Ok(_) => {
                        info!("Successfully commited message to broker");
                        ack("Successfully commited message to broker".to_string(), true)
                    }
                    Err(e) => {
                        error!("Error commiting content to broker: {:?}", e);
                        ack((format!("{:?}", e)), false)
                    }
                };
            }
        });

        Ok(Response::new(
            Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
                as Self::ProduceStream,
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let addr = "[::0]:50051".parse().unwrap();

    info!("Bridge service listening on: {}", addr);
    info!("Kafka broker connected on: {}", get_broker());
    print_metadata(&(get_broker()), None, Duration::from_millis(10000), false);
    let svc = KafkaStreamServer::new(KafkaStreamService::default());

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
