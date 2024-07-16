
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app import order_pb2
from app.crud.product_crud import validate_product_by_id
from app.dependency import get_session

async def consume_order_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="order-add-group",
        # auto_offset_reset="earliest",
    )

    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        async for message in consumer:
            print("\n\n RAW INVENTORY MESSAGE\n\n ")
            print(f"Received message on topic {message.topic}")
            print(f"Message Value {message.value}")

            

           
            new_product_id = order_pb2.Order.ParseFromString(message.value).product_id

            # 2. Check if Product Id is Valid
            with next(get_session()) as session:
                product = validate_product_by_id(
                    product_id=new_product_id, session=session)
                print("PRODUCT VALIDATION CHECK", product)
                # 3. If Valid
                # if product is None:
                    # email_body = chat_completion(f"Admin has Sent InCorrect Product. Write Email to Admin {product_id}")
                    
                if product is not None:
                        # - Write New Topic
                    print("PRODUCT VALIDATION CHECK NOT NONE")
                    
                    producer = AIOKafkaProducer(
                        bootstrap_servers='broker:19092')
                    await producer.start()
                    try:
                        await producer.send_and_wait(
                            "order-add-stock-response",
                            message.value
                        )
                    finally:
                        await producer.stop()

            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()


# async def consume_order_messages(topic, bootstrap_servers):
#     consumer = AIOKafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         group_id="order_add_group",
#         auto_offset_reset="earliest",
#     )
    
#     await consumer.start()
#     try:
#         print("Kafka consumer started successfully.")

#         async for message in consumer:
#             print(f"Received message on topic {message.topic}")
#             print(f"Message Value {message.value}")

#             # Example: Deserialize protobuf message
#             order = order_pb2.Order()
#             order.ParseFromString(message.value)
#             print(f"Parsed Order: {order}")

#             # Example: Accessing fields from protobuf message
#             product_id = order.product_id
#             print(f"Product ID: {product_id}")

#             # Example: Database interaction
#             with next(get_session()) as session:
#                 product = validate_product_by_id(product_id=product_id, session=session)
#                 print("Product Validation Result:", product)

#     finally:
#         await consumer.stop()
#         print("Kafka consumer stopped.")