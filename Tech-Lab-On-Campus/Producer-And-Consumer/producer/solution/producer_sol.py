import pika
import os

from producer.producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    routing_key = None
    exchange_name = None
    connection = None
    channel = None

    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to class variables
        routing_key = routing_key
        exchange_name = exchange_name

        # Call setupRMQConnection
        mqProducer.setupRMQConnection(self)
        

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        mqProducer.channel = connection.channel()

        # Create the exchange if not already present
        mqProducer.exchange = mqProducer.channel.exchange_declare(exchange=mqProducer.exchange_name)

        mqProducer.publishOrder("test message")

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        mqProducer.channel.basic_publish(
            exchange=mqProducer.exchange,
            routing_key=mqProducer.routing_key,
            body=message,
        )

        # Close Channel
        mqProducer.channel.close()

        # Close Connection
        mqProducer.connection.close()
