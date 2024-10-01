from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import databases
import sqlalchemy
from contextlib import asynccontextmanager
from sqlalchemy import func, select

DATABASE_URL = 'sqlite:///./test_products.db'
database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()

items = sqlalchemy.Table(
    "items",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key = True),
    sqlalchemy.Column("name", sqlalchemy.String, nullable = False),
    sqlalchemy.Column("description", sqlalchemy.String, nullable = False),
    sqlalchemy.Column("brand", sqlalchemy.String, nullable = False),
    sqlalchemy.Column("price", sqlalchemy.Float, nullable = False)
)

engine = sqlalchemy.create_engine(DATABASE_URL)
metadata.create_all(engine)

app = FastAPI()

class Item(BaseModel):
    name: str
    description: str = None
    brand: str
    price: float

@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    yield
    await database.disconnect()
    

app.add_event_handler("startup", lambda: database.connect())
app.add_event_handler("shutdown", lambda: database.connect())
app.dependency_overrides[database] = lifespan


@app.get("/")
def read_root():
    return {"message": "Hello world"}

@app.get("/items/{item_id}")
async def read_item(item_id: int):
    query = items.select().where(items.c.id == item_id)
    item = await database.fetch_one(query)
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return item
    

@app.post("/items/")
async def create_item(item: Item):
    query = items.insert().values(
        name =item.name, 
        description = item.description,
        brand = item.brand,
        price = item.price
        )
    print(query)
    last_record_id = await database.execute(query)
    return {**item.dict(), "id": last_record_id}



@app.put("/items/{item_id}")
async def update_item(item_id: int ,item: Item):
    query = items.update().where(item.c.id == item_id).values(name = item.name, description = item.description)
    await database.execute(query)
    return {**item.dict(), "id": item_id}



@app.delete("/items/{item_id}")
async def delete_item(item_id: int):
    query = items.delete().where(items.c.id == item_id)
    await database.execute(query)
    return {"meessage": "item_deleted"}


@app.get("/items/brand/{brand_name}")
async def get_branch_items(brand_name: str):
    query = items.select().where(items.c.brand == brand_name)
    name_item = await database.fetch_all(query)
    if name_item is None:
        raise HTTPException(status_code=404, detail="Item brand name not found.")
    return name_item

@app.get("/items/brand/{brand_name}/total_sales")
async def get_total_sales_per_brand(brand_name: str):
    query = select(func.sum(items.c.price)).where(items.c.brand == brand_name)
    total_sales_per_brand = await database.fetch_val(query)
    
    # Cambia la lógica aquí: fetch_val devuelve 0 si no hay ventas.
    if total_sales_per_brand is None:
        total_sales_per_brand = 0  # Puedes decidir qué devolver aquí, como 0 o algún mensaje específico.
    
    return {"brand": brand_name, "total_sales": total_sales_per_brand}
