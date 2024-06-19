CREATE TABLE log_user_behavior (
    id SERIAL PRIMARY KEY,
    user_author_id INT,
    action VARCHAR(100),
    button_product_id INT,
    stimulus VARCHAR(100),
    component VARCHAR(100),
    text_content TEXT,
    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);