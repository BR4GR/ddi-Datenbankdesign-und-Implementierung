## General questions

### What is your prior knowledge of databases from your previous practical experience?

### What was the most important insight for you?

### Are there any other important points that had an important influence on your learning success?


## Specific questions

### Which NoSQL database did you choose and why?
MongoDB seems a good fit for this problem due to its document-oriented nature. Product data, with its nested structures (nutrition, offer, potentially lists like ingredients and GTINs), maps well to JSON documents. The productInformation and offer objects can be embedded directly within the product document, avoiding joins for common queries and simplifying data retrieval. Categories could be handled either by embedding a list of category IDs/names in the product document or by having a separate category collection and linking by ID. For simplicity and to demonstrate a common NoSQL pattern, we'll embed relevant category information

### What does the problem look like?
We're analyzing product data from Migros (a Swiss retailer), including product details, nutritional information, offers, and categories. The key non-trivial aspect is the relationship between products and categories (many-to-many), and the inclusion of nutritional information and offer details as sub-entities. Also products can have multiple entries over time, allowing for historical data analysis.

### What does the conceptual data model look like?

The conceptual data model consists of the following main entities and relationships:

#### Core Entities:
- **Product**: Central entity containing basic product information (name, brand, description, GTINs)
- **Category**: Product classification hierarchy (name, slug, path, level)
- **Nutrition**: Nutritional information per 100g/ml (energy, macronutrients, micronutrients)
- **Offer**: Pricing and availability information (price, quantity, promotions)

#### Relationships:
- **Product ↔ Category**: Many-to-many relationship (products can belong to multiple categories)
- **Product → Nutrition**: One-to-one relationship (each product has one nutrition profile)
- **Product → Offer**: One-to-many relationship (products can have multiple offers over time)

#### Key Attributes:
- Products are identified by `migrosId` and tracked over time via `scraped_at`
- Categories form a hierarchy with `level` and `path` attributes
- Nutrition data includes both energy values (kJ/kcal) and macro/micronutrients
- Offers include current pricing, unit pricing, and promotional information

#### Conceptual Diagram:
```
[Product] ←→ [Category]
    ↓
[Nutrition]
    ↓
[Offer]
```

This model supports both historical tracking (multiple scrapes of the same product) and flexible categorization while maintaining referential integrity between related entities.

### What does the implementation in the two databases look like?

#### MongoDB Implementation

**Document Structure:**
```json
{
  "migrosId": "100100300000",
  "name": "Chocolate bars",
  "brand": "Frey",
  "title": "Frey · Chocolate bars · Milk chocolate with hazelnuts",
  "description": "...",
  "origin": "Switzerland",
  "ingredients": "Milk chocolate (sugar, cocoa butter...)",
  "gtins": ["7616500010031"],
  "scraped_at": "2024-09-16T15:15:05Z",
  "nutrition": {
    "unit": "g",
    "quantity": 100.0,
    "kJ": 2255,
    "kcal": 539,
    "fat": 35.0,
    "saturates": 21.0,
    "carbohydrate": 51.0,
    "sugars": 49.0,
    "protein": 6.8,
    "salt": 0.14
  },
  "offer": {
    "price": 3.95,
    "quantity": "100g",
    "unit_price": 39.50,
    "promotion_price": null
  },
  "categories": [
    {
      "id": 7494736,
      "name": "Snacks & sweets",
      "slug": "snacks-sweets"
    }
  ]
}
```

**Collections:**
- `products`: Main collection containing all product data with embedded nutrition, offer, and category information
- `categories`: Separate collection for category master data

#### PostgreSQL Implementation

**Table Structure:**
```sql
-- Core product information
CREATE TABLE product (
    id SERIAL PRIMARY KEY,
    migros_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    title TEXT,
    description TEXT,
    origin VARCHAR(100),
    ingredients TEXT,
    gtins TEXT[],
    scraped_at TIMESTAMP NOT NULL
);

-- Category hierarchy
CREATE TABLE category (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(100),
    path VARCHAR(500)
);

-- Many-to-many relationship
CREATE TABLE product_category (
    product_id INTEGER REFERENCES product(id),
    category_id INTEGER REFERENCES category(id),
    scraped_at TIMESTAMP NOT NULL,
    PRIMARY KEY (product_id, category_id, scraped_at)
);

-- Nutritional information
CREATE TABLE nutrition (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES product(id),
    unit VARCHAR(10),
    quantity DECIMAL(10,2),
    energy_kj DECIMAL(10,2),
    energy_kcal DECIMAL(10,2),
    fat DECIMAL(10,2),
    saturates DECIMAL(10,2),
    carbohydrate DECIMAL(10,2),
    sugars DECIMAL(10,2),
    protein DECIMAL(10,2),
    salt DECIMAL(10,2),
    scraped_at TIMESTAMP NOT NULL
);

-- Pricing and offers
CREATE TABLE offer (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES product(id),
    price DECIMAL(10,2),
    quantity VARCHAR(50),
    unit_price DECIMAL(10,2),
    promotion_price DECIMAL(10,2),
    scraped_at TIMESTAMP NOT NULL
);
```

#### Key Implementation Differences

**MongoDB Approach:**
- **Denormalized**: All related data embedded in product document
- **Flexible Schema**: Easy to add new fields without schema changes
- **Single Query Access**: Retrieve complete product info in one operation
- **Category Embedding**: Category information duplicated in each product

**PostgreSQL Approach:**
- **Normalized**: Separate tables with foreign key relationships
- **Referential Integrity**: Database enforces data consistency
- **Historical Tracking**: Multiple entries per product over time via `scraped_at`
- **Storage Efficiency**: Category data stored once, referenced by ID

**Data Processing:**
- MongoDB: Direct JSON insertion with minimal transformation
- PostgreSQL: Complex data transformation using factory pattern to map JSON to relational structure

### What advantages did you expect from the NoSQL database?
- Flexibility: Easily accommodate schema changes (e.g., new product attributes, changes in nutrient structure) without altering a rigid schema.
- Performance for Reads (Embedded Data): Faster retrieval for product details, nutrition, and offers as they are stored in a single document, reducing the need for joins.
- Scalability: Horizontal scaling is generally easier with NoSQL databases like MongoDB.


### What measurement criteria did you define?

### What do the measurement results look like?

### Interpret and reflect on these measurement results.
### 
