CREATE USER 'ledger_user'@'%' IDENTIFIED BY 'Auth123';

CREATE DATABASE ledger;

GRANT ALL PRIVILEGES ON ledger.* TO 'ledger_user'@'%';

USE ledger;

CREATE TABLE `ledger` (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_id VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  amount INT NOT NULL,
  operation VARCHAR(255) NOT NULL,
  transaction_date VARCHAR(255) NOT NULL
);

INSERT INTO ledger (order_id, user_id, amount, operation, transaction_date) VALUES ('1', '1', '100', 'CREDIT', '2024-03-19');
INSERT INTO ledger (order_id, user_id, amount, operation, transaction_date) VALUES ('2', '1', '100', 'CREDIT', '2024-03-19');
INSERT INTO ledger (order_id, user_id, amount, operation, transaction_date) VALUES ('3', '1', '100', 'CREDIT', '2024-03-19');
INSERT INTO ledger (order_id, user_id, amount, operation, transaction_date) VALUES ('4', '1', '100', 'CREDIT', '2024-03-19');
INSERT INTO ledger (order_id, user_id, amount, operation, transaction_date) VALUES ('5', '1', '100', 'CREDIT', '2024-03-19');
INSERT INTO ledger (order_id, user_id, amount, operation, transaction_date) VALUES ('6', '1', '100', 'CREDIT', '2024-03-19');