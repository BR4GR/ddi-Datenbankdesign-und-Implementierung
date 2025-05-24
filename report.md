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

### What does the implementation in the two databases look like?

### What advantages did you expect from the NoSQL database?
- Flexibility: Easily accommodate schema changes (e.g., new product attributes, changes in nutrient structure) without altering a rigid schema.
- Performance for Reads (Embedded Data): Faster retrieval for product details, nutrition, and offers as they are stored in a single document, reducing the need for joins.
- Scalability: Horizontal scaling is generally easier with NoSQL databases like MongoDB.
### What measurement criteria did you define?

### What do the measurement results look like?

### Interpret and reflect on these measurement results.
### 
