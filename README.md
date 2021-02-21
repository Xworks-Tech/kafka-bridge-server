# A bridge service written in Rust to connect to Redpanda kafka clients on different clusters

## TODO:

- Unit tests
- Integration tests
- Improve `.proto` kafka definitions
- Improve error handling and logging (i.e log client IPs)

## src/server.rs

- This contains a bi-directional GRPC streaming service that listens and publishes to a kafka client
- Clients subscribe to the stream by sending a `PublishRequest` message containing a topic and possibly content
- If the `PublishRequest` contains content, it is written to the broker
- The client can send multiple `PublishRequest`s to write messages to topic associated with the connection
- The GRPC service simultaneously listens to the kafka broker and forwards `KafkaResponse` messages back to the client containing the information

#### Client listener

```rust
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
```

#### Kafka listener

```rust
// Spawn the thread that listens to kafka and writes to the client
        tokio::spawn(async move {
            tokio::select! {
                consumed = receiver =>{
                    let mut consumer = consumed.unwrap();
                    loop {
                        for ms in consumer.poll().unwrap().iter()  {
                            for m in ms.messages() {
                                let send_to_broker = || async {
                                    tx.send(Ok(KafkaResponse {
                                        success: true,
                                        optional_content: Some(
                                            bridge::kafka_response::OptionalContent::Content(Vec::from(
                                                m.value,
                                            )),
                                        ),
                                    })).await.unwrap();
                                };
                                block_on(send_to_broker());
                            }
                            consumer.consume_messageset(ms).unwrap();
                        }
                        consumer.commit_consumed().unwrap();
                    }
                }
            }
        });
```
