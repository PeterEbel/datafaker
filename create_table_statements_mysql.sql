CREATE TABLE shop.customers
(
	id INT NOT NULL primary key AUTO_INCREMENT,
	entity_id CHAR(4),
	customer_number CHAR(9),
	valid_from_date DATE,
	valid_to_date DATE,
	gender_code CHAR(1),
	last_name VARCHAR(50),
	first_name VARCHAR(50),
	birth_date DATE,
	country_code CHAR(2),
	postal_code CHAR(5),
	city VARCHAR(50),
	street VARCHAR(50),
	data_date_part DATE
);

CREATE TABLE shop.balances
(
	record_number INT NOT NULL AUTO_INCREMENT,
	entity_id CHAR(4),
	customer_number CHAR(9),
	instalment_amount DECIMAL(15,2),
	term INT,
	residual_debt DECIMAL(15,2),
	data_date_part DATE
);



LOAD DATA LOCAL INFILE '/home/peter/projects/spark/customers.csv' INTO TABLE shop.customers
	FIELDS TERMINATED BY '|'
	IGNORE 1 LINES;
	
LOAD DATA LOCAL INFILE '/home/peter/projects/spark/balances.csv' INTO TABLE shop.balances
	FIELDS TERMINATED BY '|'
	IGNORE 1 LINES;
