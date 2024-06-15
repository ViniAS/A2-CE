CREATE TABLE your_table_name (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO your_table_name (name, age)
VALUES 
('John Doe', 45),
('Jane Smith', 28),
('Alice Johnson', 35),
('Robert Brown', 50),
('Emily Davis', 22),
('Michael Wilson', 40),
('Jessica Garcia', 30),
('William Martinez', 33),
('Sarah Robinson', 29),
('David Clark', 27);

-- Create a table to write processed data
CREATE TABLE processed_table_name (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);