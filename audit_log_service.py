import time
import pika


class AuditLogService:

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
                exchange='sample_exchange',
                exchange_type='direct',
            )
            self._channel.queue_declare(
                queue='sample_q',
                durable=True,
            )
            self._channel.queue_bind(
                exchange='sample_exchange',
                queue='sample_q',
                routing_key='sample_q',
            )

    def callback(self, ch, method, proper, body):
        print(str(body), 'DOING')
        time.sleep(1)
        self._channel.basic_ack(delivery_tag=method.delivery_tag)
        print(str(body), 'DONE')

    def _receiver(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue='sample_q',
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

    def _sender(self, message):
        print('{} || SENDING'.format(message))
        self._channel.basic_publish(
            exchange='sample_exchange',
            routing_key='sample_q',
            body=message,
            properties=pika.BasicProperties(delivery_mode=1),
        )
        print("{} || SENT to RabbitMQ".format(message))

    def sender(self, message):
        self.connect()

        try:
            self._sender(message)
        except Exception as e:
            print(e)
            self.connect()
            self._sender(message)

        self._conn.close()

