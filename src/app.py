# from src.config import Config
# from src.rest.order import order_router
import asyncio
# import aioamqp


from src.mq_consumer.mq import MQConsumer

# ToDo
mq_consumer = MQConsumer(
    mq_host="127.0.0.1",
    mq_user="test",
    mq_pwd="test",
    mq_exchange="test",
    routing_key="test",
    queue="payment",
)


def create_app(
    # config: Config,
):
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(mq_consumer.consume(loop=event_loop))
    event_loop.run_forever()
