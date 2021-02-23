# A bridge service written in Rust to connect to Redpanda kafka clients on different clusters

## TODO:

- Unit tests
- Integration tests
- Improve `.proto` kafka definitions
- Improve error handling and logging (i.e log client IPs)

## src/server.rs

#### Subscribe rpc

- This contains a bi-directional GRPC streaming service that listens and publishes to a kafka client
- Clients subscribe to the stream by sending a `PublishRequest` message containing a topic and possibly content
- If the `PublishRequest` contains content, it is written to the broker
- The client can send multiple `PublishRequest`s to write messages to topic associated with the connection
- The GRPC service simultaneously listens to the kafka broker and forwards `KafkaResponse` messages back to the client containing the information

#### Consume rpc

- This rpc is a read-only channel for a particular topic
- Clients send a unary request for a topic: `ConsumeRequest`
- The server then streams back responses from the kafka broker in the form of `KafkaResponse`s

#### Produce rpc

- This rpc is a write-only channel for a particular topic
- The client streams `PublishRequest`s which are synchronously ACK'd by `ProduceResponse`s
- A `ProduceResponse` can optionally content string messages with error codes if the response was unsuccessful
