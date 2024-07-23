# # from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
# # from app import order_pb2
# # from app.crud.inventory_crud import validate_inventory_item_by_product_id
# # from app.dependency import get_session
# # from app.models.order_models import OrderItem
# # from fastapi import HTTPException

# # async def consume_order_messages(topic, bootstrap_servers):
# #     # Create a consumer instance.
# #     consumer = AIOKafkaConsumer(
# #         topic,
# #         bootstrap_servers=bootstrap_servers,
# #         group_id="order-add-group",
# #         auto_offset_reset="earliest",  # Set to earliest if you want to consume from the beginning
# #     )

# #     # Start the consumer.
# #     await consumer.start()
# #     try:
# #         # Continuously listen for messages.
# #         async for message in consumer:
# #             print("\n\n RAW ORDER MESSAGE\n\n ")
# #             print(f"Received message on topic {message.topic}")
# #             print(f"Message Value {message.value}")

# #             # Deserialize the message using the protobuf library inventory_pb2
# #             order_data = order_pb2.OrderItem()
# #             order_data.ParseFromString(message.value)
# #             print("DESERIALIZED ORDER DATA", order_data)

# #               # Validate product id in database
# #             inventory_product_id = order_data.inventory_product_id
# #             print("INVENTORY ITEM ID", inventory_product_id)

# #             # Validate product id in database
# #             with next(get_session()) as session:
# #                 inventory_item = validate_inventory_item_by_product_id(inventory_product_id, session)
# #                 print("INVENTORY VALIDATION CHECK", inventory_item)

# #                 if inventory_item is None:
# #                     # Log the error or handle it appropriately without raising HTTPException
# #                 print(f"Inventory Item with Product ID {inventory_product_id} not found")
# #                 continue  # Skip further processing for this message

# #                 # If product is valid, proceed to produce message
# #                 print("INVENTORY VALIDATION CHECK NOT NONE")
                    
# #                     producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
# #                     await producer.start()
# #                     try:
# #                         await producer.send_and_wait(
# #                             "order-add-stock-response",
# #                             message.value
# #                         )
# #                     finally:
# #                         await producer.stop()

# #     finally:
# #         # Ensure to close the consumer when done.
# #         await consumer.stop()


from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app import order_pb2
from app.crud.inventory_crud import validate_inventory_item_by_product_id
from app.dependency import get_session
from fastapi import HTTPException

# async def consume_order_messages(topic, bootstrap_servers):
#     # Create a consumer instance.
#     consumer = AIOKafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         group_id="order-add-group",
#         auto_offset_reset="earliest",  # Set to earliest if you want to consume from the beginning
#     )

#     # Start the consumer.
#     await consumer.start()
#     try:
#         # Continuously listen for messages.
#         async for message in consumer:
#             print("\n\n RAW ORDER MESSAGE\n\n ")
#             print(f"Received message on topic {message.topic}")
#             print(f"Message Value {message.value}")

#             # Deserialize the message using the protobuf library order_pb2
#             order_data = order_pb2.OrderItem()
#             order_data.ParseFromString(message.value)
#             print("DESERIALIZED ORDER DATA", order_data)

#             # Validate product id in database
#             validate_inventory_product_id = order_data.inventory_product_id
#             print("INVENTORY PRODUCT ID", validate_inventory_product_id)
            
#             with next(get_session()) as session:
#                 inventory_item = validate_inventory_item_by_product_id(inventory_product_id=validate_inventory_product_id, session=session)
                
#                 if inventory_item is None:
#                     # Log the error or handle it appropriately without raising HTTPException
#                     print(f"Inventory Item with Product ID {validate_inventory_product_id} not found")
#                     continue  # Skip further processing for this message
                
#                 if inventory_item is not None:
#                     print("INVENTORY VALIDATION CHECK NOT NONE")
#                     producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
#                     await producer.start()
#                     try:
#                         await producer.send_and_wait(
#                             "order-add-stock-response",
#                             message.value
#                         )
#                     finally:
#                         await producer.stop()

               
        

#     finally:
#         # Ensure to close the consumer when done.
#         await consumer.stop()


# async def consume_order_messages(topic, bootstrap_servers):
#     # Create a Kafka consumer instance.
#     consumer = AIOKafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         group_id="order-add-group",
#         auto_offset_reset="earliest",  # Set to earliest if you want to consume from the beginning
#     )

#     # Start the Kafka consumer.
#     await consumer.start()

#     # Initialize Kafka producer outside of the loop.
#     producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
#     await producer.start()

#     try:
#         # Continuously listen for messages.
#         async for message in consumer:
#             print("\n\n RAW ORDER MESSAGE\n\n ")
#             print(f"Received message on topic {message.topic}")
#             print(f"Message Value {message.value}")

#             # Deserialize the message using the protobuf library order_pb2
#             order_data = order_pb2.OrderItem()
#             order_data.ParseFromString(message.value)
#             print("DESERIALIZED ORDER DATA", order_data)

#             # Validate product id in database
#             inventory_product_id = order_data.inventory_product_id
#             print("INVENTORY PRODUCT ID", inventory_product_id)

#             # Validate product id in database
#             with next(get_session()) as session:
#                 inventory_item = validate_inventory_item_by_product_id(inventory_product_id=inventory_product_id, session=session)
                
#                 if inventory_item is None:
#                     # Log the error or handle it appropriately without raising HTTPException
#                     print(f"Inventory Item with Product ID {inventory_product_id} not found")
#                     continue  # Skip further processing for this message

#                 # If product is valid, proceed to produce message
#                 print("INVENTORY VALIDATION CHECK NOT NONE")
#                 try:
#                     await producer.send_and_wait(
#                         "order-add-stock-response",
#                         message.value
#                     )
#                 except Exception as e:
#                     print(f"Failed to produce message: {e}")
#                     # Handle exception if sending message fails

#     except Exception as e:
#         print(f"Exception occurred in consumer loop: {e}")
#         # Handle exceptions raised during message consumption

#     finally:
#         # Stop the Kafka consumer and producer when done.
#         await consumer.stop()
#         await producer.stop()




# order_consumer.py

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.dependency import get_session
from app.crud.inventory_crud import validate_inventory_item_by_product_id, update_inventory_item_quantity
from app import order_pb2  # Assuming you have defined order_pb2 for order item serialization
from app import inventory_pb2  # Assuming you have defined inventory_pb2 for inventory item serialization

async def consume_order_messages(topic, bootstrap_servers):
    # Create a Kafka consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="order-add-group",
        auto_offset_reset="earliest",  # Set to earliest if you want to consume from the beginning
    )

    # Create a Kafka producer instance.
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')

    try:
        # Start the Kafka consumer.
        await consumer.start()
        await producer.start()

        # Continuously listen for messages.
        async for message in consumer:
            print("\n\n RAW ORDER MESSAGE\n\n ")
            print(f"Received message on topic {message.topic}")
            print(f"Message Value {message.value}")

            # Deserialize the message using the protobuf library order_pb2
            order_data = order_pb2.OrderItem()
            order_data.ParseFromString(message.value)
            print("DESERIALIZED ORDER DATA", order_data)

            # Validate product id in database
            inventory_product_id = order_data.inventory_product_id
            print("INVENTORY PRODUCT ID", inventory_product_id)

            # Validate product id in database
            with next(get_session()) as session:
                inventory_item = validate_inventory_item_by_product_id(inventory_product_id=inventory_product_id, session=session)
                
                if inventory_item is None:
                    # Log the error or handle it appropriately without raising HTTPException
                    print(f"Inventory Item with Product ID {inventory_product_id} not found")
                    continue  # Skip further processing for this message

                # If product is valid, proceed to update inventory and produce message
                print("INVENTORY VALIDATION CHECK NOT NONE")
                try:
                    # Deduct ordered quantity from inventory
                    ordered_quantity = order_data.quantity
                    if inventory_item.quantity >= ordered_quantity:
                        inventory_item.quantity -= ordered_quantity
                        session.commit()

                        # Update inventory in database
                        updated_inventory_item = update_inventory_item_quantity(inventory_product_id, inventory_item.quantity, session)
                        if updated_inventory_item:
                            print(f"Inventory item updated successfully after order: {updated_inventory_item}")
                        else:
                            print(f"Failed to update inventory item with product_id {inventory_product_id}")

                        # Send response message to "order-add-stock-response" topic
                        await producer.send_and_wait(
                            "order-add-stock-response",
                            message.value
                        )
                    else:
                        print(f"Not enough inventory for product {inventory_product_id}")
                except Exception as e:
                    print(f"Failed to process order message: {e}")
                    # Handle exception if processing fails

    except Exception as e:
        print(f"Exception occurred in consumer loop: {e}")
        # Handle exceptions raised during message consumption

    finally:
        # Stop the Kafka consumer and producer when done.
        await consumer.stop()
        await producer.stop()
