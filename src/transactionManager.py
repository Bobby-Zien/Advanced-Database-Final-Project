from dataManager import *
from ExceptionHandler import *
from collections import deque

# Transaction status
class TRAN_STATUS(Enum):
    ABORTED = 'ABORTED'
    COMMITTED = 'COMMITTED'
    NONE = 'NONE'

class Transaction:
    def __init__(self, id: int, timestamp: int, readOnly: bool) -> None:
        self.id = id
        self.status = TRAN_STATUS.NONE
        self.timestamp = timestamp
        self.readOnly = readOnly
        
class TransactionManager:
    def __init__(self) -> None:
        self.sites = [None] * 10 # List of data manager
        self.transactions = defaultdict(Transaction)
        self.transaction_queue = deque() # a queue to store transactions
        self.timestamp = 0

        # Initialize the data managers
        for i in range(10):
            self.sites[i] = DataManager(i)

    def begin(self, transaction_id: int) -> None:
        """
        Begin a transaction
        """
        self.transactions[transaction_id] = Transaction(transaction_id, self.timestamp, readOnly=False)
    
    def beginRO(self, transaction_id: int) -> None:
        """
        Begin a read-only transaction
        """
        self.transactions[transaction_id] = Transaction(transaction_id, self.timestamp, readOnly=True)

    def read(self) -> bool:
        for site in self.sites:
            if site.is_working():
                pass

    def write(self) -> bool:
        pass

    def dump(self) -> None:
        """
        Dump all data managers
        """
        for site in self.sites:
            site.dump()

    def end(self, transaction_id: int) -> None:
        if self.transactions[transaction_id].status == TRAN_STATUS.ABORTED:
            self.abort(transaction_id)
        elif self.transactions[transaction_id].status == TRAN_STATUS.COMMITTED:
            self.commit(transaction_id)

    def fail(self, site_id: int) -> None:
        if site_id not in set(range(10)):
            raise InvalidInputError("Error: Invalid site id: {}".format(site_id))
        
        site : DataManager = self.sites[site_id]
        site.fail()
    
    def recover(self, site_id: int) -> None:
        if site_id not in set(range(10)):
            raise InvalidInputError("Error: Invalid site id: {}".format(site_id))

        site : DataManager = self.sites[site_id]
        if site.status == DM_STATUS.WORKING:
            # No need to recover
            return

        site.recover()

    def abort(self, transaction_id: int) -> None:
        """
        Abort the transaction to all the data managers
        """
        for site in self.sites:
            site.abort(transaction_id)
        self.transactions.pop(transaction_id)

    def commit(self, transaction_id: int) -> None:
        """
        Commits the transaction to all the data managers
        """
        for site in self.sites:
            site.commit(transaction_id, self.timestamp)
        self.transactions.pop(transaction_id)

    def deadlock_detection(self) -> bool:
        graph = []
        visited = set()

        # Using dfs to detect if there's any cycle in the transaction graph
        def dfs(root) -> bool:
            if root in visited:
                return True
            visited.add(root)
            has_cycle = False
            for neighbor in graph[root]:
                has_cycle = dfs(neighbor)
                if has_cycle: return True
            return False