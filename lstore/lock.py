import threading
from collections import defaultdict

class Latch:
    def __init__(self, count=0):
        self.count = count
        self.lock = threading.Condition()

    def count_down(self):
        with self.lock:
            self.count -= 1
            if self.count <= 0:
                self.lock.notify_all()

    def count_up(self):
        with self.lock:
            self.count += 1

    def await_latch(self):
        with self.lock:
            while self.count > 0:
                self.lock.wait()
                

class LockManager:
    def __init__(self):
        self.lock_table = defaultdict(lambda: {'S': set(), 'X': set(), 'IS': Latch(), 'IX': Latch()})
        self.condition = threading.Condition()

        self.transaction_states = defaultdict(lambda: 'growing')

    def acquire_lock(self, transaction_id, record_id, lock_type):
        """
        Acquires a lock on the given record for the specified transaction.
        Lock types: 'S' for shared, 'X' for exclusive, 'IS' for intention to read, 'IX' for intention to write.
        """
        with self.condition:
            # Raises exception if the transaction is locked by 2PL
            if self.transaction_states[transaction_id] == 'shrinking':
                raise Exception(f"Transaction {transaction_id} is in the shrinking phase and cannot acquire more locks.")

            if lock_type == 'X':
                # Wait until no other transaction holds a lock on the record
                while self.lock_table[record_id]['S'] or self.lock_table[record_id]['X'] or self.lock_table[record_id]['IS'].count > 0 or self.lock_table[record_id]['IX'].count > 0:
                    self.condition.wait()
                self.lock_table[record_id]['X'].add(transaction_id)
            elif lock_type == 'S':
                # print("Acquiring shared lock", self.lock_table[record_id]['X'], self.lock_table[record_id]['IX'].count)
                # Wait until no other transaction holds an exclusive lock on the record
                while self.lock_table[record_id]['X'] or self.lock_table[record_id]['IX'].count > 0:
                    self.condition.wait()
                self.lock_table[record_id]['S'].add(transaction_id)
            elif lock_type == 'IS':
                # Wait until no other transaction holds an exclusive lock on the record
                while self.lock_table[record_id]['X']:
                    self.condition.wait()
                self.lock_table[record_id]['IS'].count_up()
            elif lock_type == 'IX':
                # Wait until no other transaction holds an exclusive lock on the record
                while self.lock_table[record_id]['X']:
                    self.condition.wait()
                self.lock_table[record_id]['IX'].count_up()
            else:
                raise ValueError("Invalid lock type")

    def release_lock(self, transaction_id, record_id, lock_type):
        """
        Releases the lock held by the specified transaction on the given record.
        """
        with self.condition:
            if lock_type in ['IS', 'IX']:
                self.lock_table[record_id][lock_type].count_down()
            else:
                if transaction_id in self.lock_table[record_id][lock_type]:
                    self.lock_table[record_id][lock_type].remove(transaction_id)
                    if not any(self.lock_table[record_id].values()):
                        del self.lock_table[record_id]
            self.condition.notify_all()

            self.transaction_states[transaction_id] = 'shrinking' 

    def release_all_locks(self, transaction_id):
        """
        Releases all locks held by the specified transaction.
        """
        with self.condition:
            for record_id in list(self.lock_table.keys()):
                for lock_type in self.lock_table[record_id]:
                    if lock_type in ['IS', 'IX']:
                        self.lock_table[record_id][lock_type].count_down()
                    elif transaction_id in self.lock_table[record_id][lock_type]:
                        self.lock_table[record_id][lock_type].remove(transaction_id)
                        if not any(self.lock_table[record_id].values()):
                            del self.lock_table[record_id]
            self.condition.notify_all()

            self.transaction_states[transaction_id] = 'shrinking'