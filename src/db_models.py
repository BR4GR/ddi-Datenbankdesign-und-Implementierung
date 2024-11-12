from sqlalchemy import Column, Float, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from .sql_db import Base

# Association table for the many-to-many relationship between products and categories
product_category = Table(
    "product_category",
    Base.metadata,
    Column("product_id", ForeignKey("product.migros_id"), primary_key=True),
    Column("category_id", ForeignKey("categorie.id"), primary_key=True),
)


class Product(Base):
    __tablename__ = "product"
    migros_id = Column(String, primary_key=True)
    name = Column(String)
    brandLine = Column(String)
    title = Column(String)
    origin = Column(String)
    description = Column(String)
    ingredients = Column(String)
    nutrient_id = Column(Integer, ForeignKey("nutrients.id"))
    offer_id = Column(Integer, ForeignKey("offer.id"))
    gtin_id = Column(Integer, ForeignKey("gtin.id"))

    nutrients = relationship("NutrientsInformation")
    offer = relationship("Offer")
    gtin = relationship("Gtin")


class NutrientsInformation(Base):
    __tablename__ = "nutrients"
    id = Column(Integer, primary_key=True)
    unit = Column(String)
    quantity = Column(String)
    energy = Column(String)
    fat = Column(String)
    saturates = Column(String)
    carbohydrate = Column(String)
    sugars = Column(String)
    fibre = Column(String)
    protein = Column(String)
    salt = Column(String)


class Offer(Base):
    __tablename__ = "offer"
    id = Column(Integer, primary_key=True)
    price = Column(Float)
    quantity = Column(String)
    unit_price = Column(Float)
    price_range_min = Column(Float)
    price_range_max = Column(Float)


class Gtin(Base):
    __tablename__ = "gtin"
    id = Column(Integer, primary_key=True)
    gtin_number = Column(String)


class Categorie(Base):
    __tablename__ = "categorie"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    path = Column(String)
    slug = Column(String)

    products = relationship(
        "Product", secondary=product_category, back_populates="categories"
    )
