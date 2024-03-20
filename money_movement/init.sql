CREATE USER 'money_movement_user'@'%' IDENTIFIED BY 'Auth123';

CREATE DATABASE money_movement;

GRANT ALL PRIVILEGES ON money_movement.* TO 'money_movement_user'@'%';

USE money_movement;

CREATE TABLE `wallet` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    wallet_type VARCHAR(255) NOT NULL,
    INDEX(user_id)
);

CREATE TABLE `account` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cents INT NOT NULL Default 0,
    account_type VARCHAR(255) NOT NULL,
    wallet_id INT NOT NULL,
    FOREIGN KEY (wallet_id) REFERENCES wallet(id)
);

CREATE TABLE `transaction` (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    pid VARCHAR(255) NOT NULL,
    src_user_id VARCHAR(255) NOT NULL,
    dst_user_id VARCHAR(255) NOT NULL,
    src_wallet_id INT NOT NULL,
    dst_wallet_id INT NOT NULL,
    src_account_id INT NOT NULL,
    dst_account_id INT NOT NULL,
    src_account_type VARCHAR(255) NOT NULL,
    dst_account_type VARCHAR(255) NOT NULL,
    final_dst_merchant_wallet_id INT NOT NULL,
    amount INT NOT NULL,
    INDEX(pid)
);

-- merchant and customer wallets
INSERT INTO wallet (id, user_id, wallet_type) VALUES (1,'kvs676767@gmail.com','CUSTOMER');
INSERT INTO wallet (id, user_id, wallet_type) VALUES (2,'merchant_id','MERCHANT');

-- customer accounts
INSERT INTO account (cents, account_type, wallet_id) VALUES (5000000, 'DEFAULT', 1);
INSERT INTO account (cents, account_type, wallet_id) VALUES (0, 'PAYMENT', 1);

-- merchant accounts
INSERT INTO account (cents, account_type, wallet_id) VALUES (0, 'INCOMING', 2);