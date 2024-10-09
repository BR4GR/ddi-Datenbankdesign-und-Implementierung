CREATE DATABASE migros;
USE migros;

CREATE TABLE product (
    migros_id VARCHAR(30) NOT NULL PRIMARY KEY,
    name VARCHAR(100),
    brandLine VARCHAR(100),
    title VARCHAR(100),
    origin VARCHAR(50),
    description TEXT,
    ingredients TEXT,
    nutrient_id INT,
    offer_id INT,
    gtin_id INT,
    FOREIGN KEY (nutrient_id) REFERENCES nutrients(id),
    FOREIGN KEY (offer_id) REFERENCES offer(id),
    FOREIGN KEY (gtin_id) REFERENCES gtin(id)
);

CREATE TABLE nutrients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    unit VARCHAR(5),
    quantity INT,
    energy VARCHAR(50),
    fat VARCHAR(50),
    saturates VARCHAR(50),
    carbohydrate VARCHAR(50),
    sugars VARCHAR(50),
    fibre VARCHAR(50),
    protein VARCHAR(50),
    salt VARCHAR(50)
);

CREATE TABLE offer (
    id INT AUTO_INCREMENT PRIMARY KEY,
    price DECIMAL(10, 2),
    quantity VARCHAR(50),
    unit_price DECIMAL(10, 2),
    price_range_min DECIMAL(10, 2),
    price_range_max DECIMAL(10, 2)
);

CREATE TABLE gtin (
    id INT AUTO_INCREMENT PRIMARY KEY,
    gtin_number VARCHAR(50)
);

CREATE TABLE categorie (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    path VARCHAR(100),
    slug VARCHAR(100)
);

CREATE TABLE productcategory ( 
    product_id VARCHAR(30),
    category_id INT,
    PRIMARY KEY (product_id, category_id),
    FOREIGN KEY (product_id) REFERENCES product(migros_id),
    FOREIGN KEY (category_id) REFERENCES categorie(id)
);
