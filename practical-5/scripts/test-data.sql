INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'CDC Product', 2);
UPDATE users SET email = 'updated@example.com' WHERE id = 1;
UPDATE orders SET quantity = 7 WHERE id = 1;
