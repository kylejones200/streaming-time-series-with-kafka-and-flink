# Description: Short example for Streaming Time Series with Kafka and Flink.



from kafka import KafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json
import random
import time

bin/kafka-topics.sh --create --topic temperature-sensor --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

sensor_ids = ["sensor_1", "sensor_2", "sensor_3"]

while True:
    data = {
        "sensor_id": random.choice(sensor_ids),
        "timestamp": int(time.time() * 1000),
        "temperature": round(random.uniform(15.0, 35.0), 2)
    }
    producer.send('temperature-sensor', value=data)
    time.sleep(1)


def parse_json(value):
    record = json.loads(value)
    record['timestamp'] = int(record['timestamp'])
    record['temperature'] = float(record['temperature'])
    return record

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_consumer = FlinkKafkaConsumer(
        topics='temperature-sensor',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'temperature-group'
        }
    )

    kafka_producer = FlinkKafkaProducer(
        topic='temperature-averages',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )

    stream = env.add_source(kafka_consumer)
    
    parsed_stream = stream.map(parse_json)

    # Calculate rolling average
    averaged_stream = parsed_stream.key_by(lambda x: x['sensor_id']) \
                                 .time_window(Time.seconds(10)) \
                                 .reduce(lambda a, b: {
                                     'sensor_id': a['sensor_id'],
                                     'timestamp': max(a['timestamp'], b['timestamp']),
                                     'temperature': (a['temperature'] + b['temperature']) / 2
                                 })

    averaged_stream.map(lambda x: json.dumps(x)).add_sink(kafka_producer)

    env.execute("Kafka Flink Streaming")

if __name__ == '__main__':
    main()
