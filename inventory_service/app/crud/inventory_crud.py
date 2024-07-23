from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.inventory_models import InventoryItem
from app.models.order_models import OrderItem

# Add a New Inventory Item to the Database
def add_new_inventory_item(inventory_item_data: InventoryItem, session: Session):
    try:
        existing_inventory_item = session.exec(select(InventoryItem).filter(InventoryItem.id == inventory_item_data.id)).one_or_none()
        if existing_inventory_item:
            # Item already exists, update the quantity
            existing_inventory_item.quantity += inventory_item_data.quantity
            session.commit()
            session.refresh(existing_inventory_item)
            return existing_inventory_item
        else:
            # Item does not exist, add new item
            session.add(inventory_item_data)
            session.commit()
            session.refresh(inventory_item_data)
            return inventory_item_data
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
        


# Get All Orders from the Database
def get_all_inventory_items(session: Session):
    all_inventory_items = session.exec(select(InventoryItem)).all()
    return all_inventory_items


# Get an Order Item by ID
def get_inventory_item_by_id(inventory_item_id: int, session: Session):
    inventory_item = session.exec(select(InventoryItem).where(InventoryItem.id == inventory_item_id)).one_or_none()
    if inventory_item is None:
        raise HTTPException(status_code=404, detail="Inventory Item not found")
    return inventory_item

# Delete Order Item by ID
def delete_inventory_item_by_id(inventory_item_id: int, session: Session):
    # Step 1: Get the order Item by ID
    inventory_item = session.exec(select(InventoryItem).where(InventoryItem.id == inventory_item_id)).one_or_none()
    if inventory_item is None:
        raise HTTPException(status_code=404, detail="Inventory Item not found")
    # Step 2: Delete the order Item
    session.delete(inventory_item)
    session.commit()
    return {"message": "Inventory Item Deleted Successfully"}


def validate_inventory_item_by_product_id(inventory_product_id: int, session: Session)-> InventoryItem | None:
    inventory_item = session.exec(select(InventoryItem).where(InventoryItem.product_id == inventory_product_id)).one_or_none()
    if inventory_item is None:
        # raise HTTPException(status_code=404, detail="Inventory Item Of Product ID not found")
        print("Inventory item not found")
    return inventory_item


def update_inventory_item_quantity(product_id: int, new_quantity: int, session: Session) -> InventoryItem:
    """ Update the quantity of an inventory item """
    inventory_item = session.exec(select(InventoryItem).filter(InventoryItem.product_id == product_id)).one_or_none()
    
    if inventory_item:
        inventory_item.quantity = new_quantity
        session.add(inventory_item)
        session.commit()
    
    return inventory_item


