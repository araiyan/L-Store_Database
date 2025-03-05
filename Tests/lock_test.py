import sys
sys.path.append("..")

import threading
import time
from lstore.lock import LockManager

def test_shared_lock():
    lock_manager = LockManager()
    transaction_id = 1
    record_id = 100

    def transaction():
        print(f"Transaction {transaction_id} started")
        lock_manager.acquire_lock(transaction_id, record_id, 'S')
        print(f"Transaction {transaction_id} acquired shared lock on record {record_id}")
        time.sleep(1)
        lock_manager.release_lock(transaction_id, record_id, 'S')
        print(f"Transaction {transaction_id} released shared lock on record {record_id}")

    thread = threading.Thread(target=transaction)
    thread.start()
    thread.join()

def test_exclusive_lock():
    lock_manager = LockManager()
    transaction_id = 2
    record_id = 200

    def transaction():
        lock_manager.acquire_lock(transaction_id, record_id, 'X')
        print(f"Transaction {transaction_id} acquired exclusive lock on record {record_id}")
        time.sleep(1)
        lock_manager.release_lock(transaction_id, record_id, 'X')
        print(f"Transaction {transaction_id} released exclusive lock on record {record_id}")

    thread = threading.Thread(target=transaction)
    thread.start()
    thread.join()

def test_intention_shared_lock():
    lock_manager = LockManager()
    transaction_id = 3
    record_id = 300

    def transaction():
        lock_manager.acquire_lock(transaction_id, record_id, 'IS')
        print(f"Transaction {transaction_id} acquired intention shared lock on record {record_id}")
        time.sleep(1)
        lock_manager.release_lock(transaction_id, record_id, 'IS')
        print(f"Transaction {transaction_id} released intention shared lock on record {record_id}")

    thread = threading.Thread(target=transaction)
    thread.start()
    thread.join()

def test_intention_exclusive_lock():
    lock_manager = LockManager()
    transaction_id = 4
    record_id = 400

    def transaction():
        lock_manager.acquire_lock(transaction_id, record_id, 'IX')
        print(f"Transaction {transaction_id} acquired intention exclusive lock on record {record_id}")
        time.sleep(1)
        lock_manager.release_lock(transaction_id, record_id, 'IX')
        print(f"Transaction {transaction_id} released intention exclusive lock on record {record_id}")

    thread = threading.Thread(target=transaction)
    thread.start()
    thread.join()

def test_concurrent_locks():
    lock_manager = LockManager()
    record_id = 500

    def transaction(transaction_id, lock_type):
        lock_manager.acquire_lock(transaction_id, record_id, lock_type)
        print(f"Transaction {transaction_id} acquired {lock_type} lock on record {record_id}")
        time.sleep(1)
        lock_manager.release_lock(transaction_id, record_id, lock_type)
        print(f"Transaction {transaction_id} released {lock_type} lock on record {record_id}")

    threads = []
    for i in range(5):
        t = threading.Thread(target=transaction, args=(i, 'S' if i % 2 == 0 else 'IS'))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    print("Testing shared lock")
    test_shared_lock()
    print("\nTesting exclusive lock")
    test_exclusive_lock()
    print("\nTesting intention shared lock")
    test_intention_shared_lock()
    print("\nTesting intention exclusive lock")
    test_intention_exclusive_lock()
    print("\nTesting concurrent locks")
    test_concurrent_locks()