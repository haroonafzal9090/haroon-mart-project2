from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app import inventory_pb2
from app.crud.product_crud import validate_product_by_id
from app.dependency import get_session
from app.models.inventory_models import InventoryItem

async def consume_inventory_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="inventory-add-group",
        auto_offset_reset="earliest",  # Set to earliest if you want to consume from the beginning
    )

    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        async for message in consumer:
            print("\n\n RAW INVENTORY MESSAGE\n\n ")
            print(f"Received message on topic {message.topic}")
            print(f"Message Value {message.value}")

            # Deserialize the message using the protobuf library inventory_pb2
            inventory_data = inventory_pb2.InventoryItem()
            inventory_data.ParseFromString(message.value)
            print("DESERIALIZED INVENTORY DATA", inventory_data)

            new_product_id = inventory_data.product_id
            print("PRODUCT ID", new_product_id)
            print(type(new_product_id))

            # Validate product id in database
            with next(get_session()) as session:
                product = validate_product_by_id(product_id=new_product_id, session=session)
                print("PRODUCT VALIDATION CHECK", product)

                # If product is valid, proceed to produce message
                if product is not None:
                    print("PRODUCT VALIDATION CHECK NOT NONE")
                    
                    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
                    await producer.start()
                    try:
                        await producer.send_and_wait(
                            "inventory-add-stock-response",
                            message.value
                        )
                    finally:
                        await producer.stop()

    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
