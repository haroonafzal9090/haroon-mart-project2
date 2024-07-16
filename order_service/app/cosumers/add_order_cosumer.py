from aiokafka import AIOKafkaConsumer
from app.models.order_models import Order
from app.crud.order_crud import add_new_order_item
from app.dependency import get_session
from app import order_pb2


async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="add-order-consumer-group",
        # auto_offset_reset="earliest",
    )

    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        

        async for message in consumer:
            
            print(f"\n\n Consumer Raw message Vaue: {message.value}")
#             # Here you can add code to process each message.
#             # Example: parse the message, store it in a database, etc.

            new_order = order_pb2.Order()
            new_order.ParseFromString(message.value)
            print(f"\n\n Consumer Deserialized data: {new_order}")

            new_order_data = {
                "id": new_order.id,
                "product_id": new_order.product_id,
                "quantity": new_order.quantity
            }
            print("ORDER_DATA", new_order_data)
            

            with next(get_session()) as session:
                print("SAVING DATA TO DATABSE")
                db_insert_order = add_new_order_item(
                        order_item_data=Order(**new_order_data),
                        session=session
                    )
                print("DB_INSERT_ORDER", db_insert_order)
                
                # Event EMIT In NEW TOPIC

            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
    