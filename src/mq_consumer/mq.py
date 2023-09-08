"""MQ Consumer"""
import aio_pika
import json



# ToDo replace
class MQPublisher:

    def __init__(self, mq_host, mq_user: str, mq_pwd: str, mq_exchange: str, routing_key: str):

        self._connection = None
        self._mq_dsn = f"amqp://{mq_user}:{mq_pwd}@{mq_host}/"
        self._routing_key = routing_key

        self._org_event_exchange = None
        self._mq_exchange = mq_exchange

    async def push_message(self, event_name: str, body: dict[any, any]) -> None:
        async with await aio_pika.connect_robust(self._mq_dsn) as conn:
            channel = await conn.channel()
            self._org_event_exchange = await channel.declare_exchange(
                self._mq_exchange, aio_pika.ExchangeType.DIRECT,
            )

            await self._org_event_exchange.publish(
                aio_pika.Message(
                    headers={
                        "EVENT_NAME": event_name,
                    },
                    body=json.dumps(body).encode(),
                ),
                routing_key=self._routing_key,
            )


mq_publisher = MQPublisher(
    mq_host="127.0.0.1",
    mq_user="test",
    mq_pwd="test",
    mq_exchange="test",
    routing_key="callback",
)


class MQConsumer:

    def __init__(self, mq_host, mq_user: str, mq_pwd: str, queue: str, routing_key: str, mq_exchange: str):

        self._connection = None
        self._mq_dsn = f"amqp://{mq_user}:{mq_pwd}@{mq_host}/"
        # self._routing_key = routing_key

        self._queue = queue

        self._routing_key = routing_key
        self._mq_exchange = mq_exchange

    async def consume(self, loop):
        """Setup message listener with the current running loop"""
        connection = await aio_pika.connect_robust(
            self._mq_dsn,
            # timeout=0.1,
            # loop=loop,
        )
        channel = await connection.channel()
        queue = await channel.declare_queue(self._queue, durable=True)
        await queue.bind(
            exchange=self._mq_exchange,
            routing_key=self._routing_key,
        )
        await queue.consume(self.process_incoming_message, no_ack=False)
        return connection

    async def process_incoming_message(self, message):
        """Processing incoming message from RabbitMQ"""
        await message.ack()
        body = message.body
        if body:
            # ToDo
            print("*"*10)
            print(json.loads(body))
            req = json.loads(body)
            new_order_id = req.get("new_order_id", None)
            if new_order_id is not None:

                if req.get("do_error", None) is not None:
                    # Error
                    status = "error"
                else:
                    status = "OK"

                await mq_publisher.push_message(
                    event_name="new_order",
                    body={
                        "order_id": new_order_id,
                        "status": status,
                    },
                )
