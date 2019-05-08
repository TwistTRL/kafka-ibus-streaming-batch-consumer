# kafka-ibus-streaming-batch-consumer
This is a script that opens a TCP server listening for a single incoming connection to dump the data to. At the same time, operational logs are stored onto Kafka cluster into another topic.
The script does message batching with specified interval (in seconds)

## Installation
```
pip install git+https://github.com/TwistTRL/kafka-ibus-streaming-batch-consumer
```

## Usage:
```
kafka-ibus-streaming-batch-consumer.py <kafkaHost> <kafkaPort> <tcpHost> <tcpPort> <groupId> <topic> <logTopic> <interval>
```
# kafka-ibus-streaming-batch-consumer
