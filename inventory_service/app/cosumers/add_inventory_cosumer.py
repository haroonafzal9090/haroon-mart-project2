from aiokafka import AIOKafkaConsumer
from app.models.inventory_models import InventoryItem
from app.crud.inventory_crud import add_new_inventory_item
from app.dependency import get_session
from app import inventory_pb2


async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="add-inventory-consumer-group",
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

            new_inventory_item = inventory_pb2.InventoryItem()
            new_inventory_item.ParseFromString(message.value)
            print(f"\n\n Consumer Deserialized data: {new_inventory_item}")

            new_inventory_data = {
                "id": new_inventory_item.id,
                "product_id": new_inventory_item.product_id,
                "quantity": new_inventory_item.quantity,
                "status": new_inventory_item.status
            }
            print("NEW_INVENTORY_DATA", new_inventory_data)
            

            with next(get_session()) as session:
                print("SAVING DATA TO DATABSE")
                db_insert_inventory_item = add_new_inventory_item(
                        inventory_item_data=InventoryItem(**new_inventory_data),
                        session=session
                    )
                print("DB_INSERT_INVENTORY", db_insert_inventory_item)
                
                # Event EMIT In NEW TOPIC

            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()
    