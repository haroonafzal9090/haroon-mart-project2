from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.order_models import Order

# Add a New Order Item to the Database
def add_new_order_item(order_item_data: Order, session: Session):
    print("Adding order Item to Database")
    
    session.add(order_item_data)
    session.commit()
    session.refresh(order_item_data)
    return order_item_data


# Get All Orders from the Database
def get_all_orders(session: Session):
    all_orders = session.exec(select(Order)).all()
    return all_orders


# Get an Order Item by ID
def get_order_item_by_id(order_item_id: int, session: Session):
    order_item = session.exec(select(Order).where(Order.id == order_item_id)).one_or_none()
    if order_item is None:
        raise HTTPException(status_code=404, detail="Order Item not found")
    return order_item

# Delete Order Item by ID
def delete_order_item_by_id(order_item_id: int, session: Session):
    # Step 1: Get the order Item by ID
    order_item = session.exec(select(Order).where(Order.id == order_item_id)).one_or_none()
    if order_item is None:
        raise HTTPException(status_code=404, detail="order Item not found")
    # Step 2: Delete the order Item
    session.delete(order_item)
    session.commit()
    return {"message": "order Item Deleted Successfully"}