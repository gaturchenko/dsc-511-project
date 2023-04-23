from kafka import KafkaProducer


def start_producing(message: str, topic: str, kafka_host: str = 'localhost:9092') -> None:
    """
    Function to send the message to consumer

    Parameters:

    `message` : `str`, utf-8 encoded JSON bytes string, the message to be delivered to the consumer

    `topic` : `str`, one of ['app_request', 'prediction_request', 'prediction_complete']

    `kafka_host` : `str`, the address of the kafka host, defaults to localhost:9092
    """
    producer = KafkaProducer(bootstrap_servers=kafka_host)
    producer.send(topic, message)
    producer.flush()