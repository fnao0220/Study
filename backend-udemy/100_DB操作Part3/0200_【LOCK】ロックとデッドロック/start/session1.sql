-- セッション1用
start transaction;

update test_db.txn_stocks set amount = 500
where product_id = 1 and shop_id = 1;

update test_db.txn_stocks set amount = 500
where product_id = 1 and shop_id = 2;
commit;

-- lock解除待ちの確認
-- MySQL 5.7用
select * from information_schema.innodb_lock_waits;
-- MySQL 8.0.1以降用
SELECT * FROM performance_schema.data_lock_waits;

-- deadlockの確認
show engine innodb status;

