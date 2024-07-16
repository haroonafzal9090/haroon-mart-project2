from aiokafka import AIOKafkaConsumer
import json
from app.models.product_models import Product
from app.crud.product_crud import add_new_product
from app.dependency import get_session
from app import product_pb2


async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-product-consumer-group",
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

            new_product = product_pb2.Product()
            new_product.ParseFromString(message.value)
            print(f"\n\n Consumer Deserialized data: {new_product}")

            new_product_data = {
                "id": new_product.id,
                "name": new_product.name,
                "description": new_product.description,
                "price": new_product.price,
                "expiry": new_product.expiry,
                "brand": new_product.brand,
                "weight": new_product.weight,
                "category": new_product.category,
                "sku": new_product.sku
                # Add other fields as per your protobuf definition
            }
            print("PRODUCT_DATA", new_product_data)
            

            with next(get_session()) as session:
                print("SAVING DATA TO DATABSE")
                db_insert_product = add_new_product(
                        session=session,
                        product=Product(**new_product_data)
                    )
                print("DB_INSERT_PRODUCT", db_insert_product)
                
                # Event EMIT In NEW TOPIC

            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
    