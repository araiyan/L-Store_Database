import sys
sys.path.append("..")

import threading
import time
from lstore.lock import LockManager

def test_deadlock_detection():
    lock_manager = LockManager()

    def transaction_1():
        try:
            lock_manager.acquire_lock(1, 'record1', 'X')
            time.sleep(1)
            lock_manager.acquire_lock(1, 'record2', 'S')
        except Exception as e:
            print(f"Transaction 1: {e}")

    def transaction_2():
        try:
            lock_manager.acquire_lock(2, 'record2', 'X')
            time.sleep(1)
            lock_manager.acquire_lock(2, 'record1', 'S')
        except Exception as e:
            print(f"Transaction 2: {e}")

    # Create threads for the transactions
    t1 = threading.Thread(target=transaction_1)
    t2 = threading.Thread(target=transaction_2)

    # Start the transactions
    t1.start()
    t2.start()

    # Wait for the transactions to complete
    t1.join()
    t2.join()

    # Check if the deadlock was detected and handled
    print("Test completed. Check the output for deadlock detection messages.")

if __name__ == "__main__":
    test_deadlock_detection()