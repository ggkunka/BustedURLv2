from kafka import KafkaProducer

class KafkaBroker:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def send_message(self, topic, message):
        self.producer.send(topic, message.encode('utf-8'))
