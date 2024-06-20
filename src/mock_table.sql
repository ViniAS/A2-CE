CREATE TABLE user_behavior_log (
    user_author_id INT,
    action VARCHAR(100),
    button_product_id INT,
    stimulus VARCHAR(100),
    component VARCHAR(100),
    text_content TEXT,
    date TIMESTAMP
);