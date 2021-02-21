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
```

#### Kafka listener

```rust
// Spawn the thread that listens to kafka and writes to the client
tokio::spawn(async move {
    tokio::select! {
        consumed = receiver =>{
            let consumer = consumed.unwrap();
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
});
```
