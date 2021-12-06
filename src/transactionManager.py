from os import write
from dataManager import *
from collections import deque
from iohandler import Parser

# Transaction status
class TRAN_STATUS(Enum):
    ABORTED = 'ABORTED'
    COMMITTED = 'COMMITTED'

class Transaction:
    def __init__(self, id: str, timestamp: int, readOnly: bool) -> None:
        self.id = id
        self.status = TRAN_STATUS.COMMITTED
        self.timestamp = timestamp
        self.readOnly = readOnly

class Operation:
    def __init__(self, type: str, transaction_id: str, variable_id: str, val: int=0):
        self.type = type
        self.transaction_id = transaction_id
        self.variable_id = variable_id
        self.val = val

class TransactionManager:
    def __init__(self) -> None:
        """"
        sites (list): List of data manager
        transactions: (defaultdict(transaction_id:str, Transaction))
        transaction_queue (deque): queue to store Read and Write transactions
        timestamp (int): current time
        """
        self.sites = [None] * 10
        self.transactions = defaultdict(Transaction)
        self.operation_queue = deque()
        self.visited_transactions = set()
        self.timestamp = 0
        self.debug = False

        # Initialize the data managers
        for i in range(10):
            self.sites[i] = DataManager(i+1)

    def operate(self, args: list) -> None:
        """
        Called by the main function to run a operation
        """
        if len(args) == 0: # check an empty line
            return
        type = args.pop(0) # get the operation
        if type == "begin":
            self.begin(args[0]) # transaction_id
        elif type == "beginRO":
            self.beginRO(args[0]) # transaction_id
        elif type == "R":
            # add the read operation to the operation queue
            self.operation_queue.append(Operation('R', args[0], args[1])) # (R, transaction_id, variable_id)
        elif type == "W":
            # add the write operation to the operation queue
            self.operation_queue.append(Operation('W', args[0], args[1], args[2])) # (W, transaction_id, variable_id, value)
        elif type == "end":
            self.end(args[0])
        elif type == "fail":
            self.fail(int(args[0])-1) # Because we store indexes in self.sites
        elif type == "recover":
            self.recover(int(args[0])-1)
        elif type == "dump":
            self.dump()
        else:
            # Simply ignore invalid inputs
            return
            
        self.timestamp += 1

        ##### TODO #### revise it
        self.__execute()
        if self.__deadlock_detection():
            self.execute()

    #### TODO  ## - revise it
    def __execute(self):
        """
        Go through the operation queue, execute those could be run
        If a transaction does not exists, remove it from the operation queue
        """
        for ope in list(self.operation_queue):
            ope : Operation
            if not self.transactions.get(ope.transaction_id):
                self.operation_queue.remove(ope)
            else:
                res = False
                if ope.type == 'R':
                    res = self.read(ope.transaction_id, ope.variable_id)
                elif ope.type == 'W':
                    res = self.write(ope.transaction_id, ope.variable_id, ope.val)
                if res:
                    self.operation_queue.remove(ope)

    def begin(self, transaction_id: str) -> None:
        """
        Begin a transaction
        """
        self.transactions[transaction_id] = Transaction(transaction_id, self.timestamp, readOnly=False)
        if self.debug: print("Transaction: {} begins".format(transaction_id))
    
    def beginRO(self, transaction_id: str) -> None:
        """
        Begin a read-only transaction
        """
        self.transactions[transaction_id] = Transaction(transaction_id, self.timestamp, readOnly=True)
        if self.debug: print("Read-Only Transaction: {} begins".format(transaction_id))

    def read(self, transaction_id: str, variable_id: str) -> bool:
        """
        Read the transaction from any working sites
        """
        # Iterate all the sites and read from the sites
        for site in self.sites:
            site : DataManager
            if site.on_flag == False:
                continue
            
            # read only transaction, read by snapshot
            ts : Transaction = self.transactions[transaction_id]
            if ts.readOnly == True:
                ret, val = site.snapshot(ts.timestamp, variable_id)
                if ret == True:
                    if self.debug: print("Read-only transaction: {},  read from site {} -- {}: {}".format(transaction_id, site.id, variable_id, val))
                    return True

            # Normal transactions
            else:
                ret, val = site.read(variable_id, transaction_id)
                if ret == True:
                    if self.debug: print("Transaction: {}, read from site {} -- {}: {}".format(transaction_id, site.id, variable_id, val))
                    return True
        return False

    def write(self, transaction_id: str, variable_id: str, val: int) -> bool:
        """
        Write the transaction to all working sites
        """
        # all_can_write = True
        # for site in self.sites:
        #     site : DataManager
        #     if site.on_flag == False or site.if_can_write(transaction_id, variable_id) == False:
        #         all_can_write = False

        # if all_can_write == False:
        #     return False

        write_sites = []
        for site in self.sites:
            site : DataManager
            if site.on_flag == True:
                if site.if_can_write(transaction_id, variable_id) == True:
                    ret = site.local_write(variable_id, val, transaction_id)
                    if ret: write_sites.append(site.id)

        if self.debug: print("Transaction: {}, writes {}: {} in sites: {}".format(transaction_id, variable_id, val, write_sites))
        return True    

    def dump(self) -> None:
        """
        Dump all data managers
        """
        print()
        for site in self.sites:
            site : DataManager
            site.dump()

    def end(self, transaction_id: str) -> None:
        """"
        Aborts the transaction if TRAN_STATUS.ABORTED when ends
        Commit the transaction otherwise
        """
        if transaction_id not in self.transactions:
            if self.debug: print("Error: Invalid transaction_id: {} to end".format(transaction_id))
            return
        ts : Transaction = self.transactions[transaction_id]
        if ts.status == TRAN_STATUS.ABORTED:
            self.__abort(transaction_id)
        elif ts.status == TRAN_STATUS.COMMITTED:
            self.__commit(transaction_id)

    def fail(self, site_id: int) -> None:
        if site_id not in set(range(10)):
            if self.debug: print("Error: Invalid site id: {} to fail".format(site_id + 1)) # site_id is index
            return
        
        site : DataManager = self.sites[site_id]
        site.fail()
        if self.debug: print("Site: {} failed".format(site_id + 1)) # site_id is index

        ######## TODO #########
    
    def recover(self, site_id: int) -> None:
        if site_id not in set(range(10)):
            if self.debug: print("Error: Invalid site id: {} to recover".format(site_id + 1)) # site_id is index
            return

        site : DataManager = self.sites[site_id]
        # No need to recover
        if site.on_flag == True:
            return

        ret = site.recover()
        if self.debug and ret: print("Successfully recovered site %s." % (site_id + 1)) # site_id is index

    def __abort(self, transaction_id: str) -> None:
        """
        Called by self.end()
        Abort the transaction to all the data managers
        """
        for site in self.sites:
            site : DataManager
            site.abort(transaction_id)

        ts : Transaction = self.transactions[transaction_id]
        ts.status = TRAN_STATUS.ABORTED
        self.transactions.pop(transaction_id)
        if self.debug: print("Aborted transaction :{}".format(transaction_id))

    def __commit(self, transaction_id: str) -> None:
        """
        Called by self.end()
        Commits the transaction to all the data managers
        """
        for site in self.sites:
            site : DataManager
            site.commit(transaction_id, self.timestamp)
        self.transactions.pop(transaction_id)
        if self.debug: print("Commited transaction: {}".format(transaction_id))

    ########### TODO #################
    def __deadlock_detection(self) -> bool:
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

        return False