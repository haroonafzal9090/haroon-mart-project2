from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.order_models import OrderItem


# Add a New Order Item to the Database
# def add_new_order_item(order_item_data: OrderItem, session: Session):
#     try:
#         existing_order = session.exec(select(OrderItem).where(OrderItem.id == order_item_data.id)).one_or_none()
#         if existing_order:
#             print(f"Error: An order with ID {order_item_data.id} already exists in the database.")
#             return None
        
#         session.add(order_item_data)
#         session.commit()
#         session.refresh(order_item_data)
#         return order_item_data
#     finally:
#         session.close()


def add_new_order_item(order_item_data: OrderItem, session: Session):
    try:
        existing_order_item = session.exec(select(OrderItem).filter(OrderItem.id == order_item_data.id)).one_or_none()
        if existing_order_item:
            # Item already exists, update the quantity
            existing_order_item.quantity += order_item_data.quantity
            session.commit()
            session.refresh(existing_order_item)
            return existing_order_item
        else:
            # Item does not exist, add new item
            session.add(order_item_data)
            session.commit()
            session.refresh(order_item_data)
            return order_item_data
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


# Get All Orders from the Database
def get_all_order_items(session: Session):
    all_order_items = session.exec(select(OrderItem)).all()
    return all_order_items


# Get an Order Item by ID
def get_order_item_by_id(order_item_id: int, session: Session):
    order_item = session.exec(select(OrderItem).where(OrderItem.id == order_item_id)).one_or_none()
    if order_item is None:
        raise HTTPException(status_code=404, detail="Order Item not found")
    return order_item

# Delete Order Item by ID
def delete_order_item_by_id(order_item_id: int, session: Session):
    # Step 1: Get the order Item by ID
    order_item = session.exec(select(OrderItem).where(OrderItem.id == order_item_id)).one_or_none()
    if order_item is None:
        raise HTTPException(status_code=404, detail="order Item not found")
    # Step 2: Delete the order Item
    session.delete(order_item)
    session.commit()
    return {"message": "order Item Deleted Successfully"}
