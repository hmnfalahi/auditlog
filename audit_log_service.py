import time
import pika


class RMQService:

    def __init__(self):
        self._conn = None
        self._channel = None

    def connect(self):
        if not self._conn or self._conn.is_closed:
            self._conn = pika.BlockingConnection(
                parameters=pika.ConnectionParameters(host='localhost')
            )
            self._channel = self._conn.channel()
            self._channel.exchange_declare(
                exchange='exchange',
                exchange_type="x-delayed-message",
                arguments={"x-delayed-type": "direct"}
            )
            self._channel.queue_declare(
                queue='queue',
                durable=True,
            )
            self._channel.queue_bind(
                exchange='exchange',
                queue='queue',
                routing_key='queue',
            )

    def callback(self, ch, method, proper, body):
        print(str(body), 'DOING')
        print(proper)
        self._channel.basic_ack(delivery_tag=method.delivery_tag)
        print(str(body), 'DONE')

    def _receiver(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue='queue',
            on_message_callback=self.callback,
        )
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self._channel.start_consuming()

    def receiver(self):
        self.connect()

        try:
            self._receiver()
        except Exception as e:
            print(e)
            self.connect()
            self._receiver()

    def _sender(self, message, delay=0):
        print('{} || SENDING'.format(message))
        self._channel.basic_publish(
            exchange='exchange',
            routing_key='queue',
            body=message,
            properties=pika.BasicProperties(headers={"x-delay": delay}),
        )
        print("{} || SENT to RabbitMQ".format(message))

    def sender(self, message, delay=0):
        self.connect()

        try:
            self._sender(message, delay)
        except Exception as e:
            print(e)
            self.connect()
            self._sender(message, delay)

        self._conn.close()

