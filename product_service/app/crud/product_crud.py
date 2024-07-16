from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.product_models import Product, ProductUpdate


#Add product to database

def add_new_product(session: Session, product: Product):
    print("Adding Product To Database")
    session.add(product)
    session.commit()
    session.refresh(product)
    return product

# Get all products
def get_all_products(session: Session):
    all_products = session.exec(select(Product)).all()
    return all_products
    

# Get product by id

def get_product_by_id(session: Session, product_id: int):
    product = session.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


# Delete product by id

def delete_product_by_id(session: Session, product_id: int):
    product = session.exec(select(Product).where(Product.id == product_id)).one_or_none()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    session.delete(product)
    session.commit()
    return {"message": "Product deleted successfully"}


# Update product by id

def update_product_by_id(session: Session, product_id: int, to_update_product_data: ProductUpdate):
    
    # Get Product By ID
    product = session.exec(select(Product).where(Product.id == product_id)).one_or_none()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    product_data = to_update_product_data.model_dump(exclude_unset=True)
    product.sqlmodel_update(product_data)
    session.add(product)
    session.commit()
    session.refresh(product)
    return product


# Validate Product by ID
def validate_product_by_id(product_id: int, session: Session) -> Product | None:
    product = session.exec(select(Product).where(Product.id == product_id)).one_or_none()
    return product