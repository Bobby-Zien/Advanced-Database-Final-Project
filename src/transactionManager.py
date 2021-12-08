from os import write
from dataManager import *
from collections import deque
from iohandler import Parser

# Transaction status
class TRAN_STATUS(Enum):
    ABORTED = 'ABORTED'
    COMMITTED = 'COMMITTED'

# Command types
class COMMAND_TYPE(Enum):
    BEGIN = 'begin'
    BEGINRO = 'beginRO'
    READ = 'R'
    WRITE = 'W'
    END = 'end'
    FAIL = 'fail'
    RECOVER = 'recover'
    DUMP = 'dump'

class Transaction:
    def __init__(self, id: str, timestamp: int, readOnly: bool) -> None:
        self.id = id
        self.status = TRAN_STATUS.COMMITTED
        self.timestamp = timestamp
        self.readOnly = readOnly

class Command:
    def __init__(self, type: COMMAND_TYPE, transaction_id: str, variable_id: str, val: int=0):
        self.type = type
        self.transaction_id = transaction_id
        self.variable_id = variable_id
        self.val = val

class TransactionManager:
    def __init__(self) -> None:
        """"
        sites (list): List of data manager
        transactions: (defaultdict(transaction_id:str, Transaction))
        command_queue (deque): queue to store Read and Write transactions
        timestamp (int): current time
        debug (bool): flag to print debugging logs
        """
        self.sites = [None] * 10
        self.transactions = defaultdict(Transaction)
        self.command_queue = deque()
        self.timestamp = 0
        self.debug = False

        # Initialize the data managers
        for i in range(10):
            self.sites[i] = DataManager(i+1)

    def __udpate_command_queue(self) -> None:
        """
        Iterate the command queue, execute those commands that can run
        """
        for cmd in list(self.command_queue):
            cmd : Command
            if cmd.transaction_id not in self.transactions:
                self.command_queue.remove(cmd)
                return

            flag = False
            if cmd.type == COMMAND_TYPE.READ:
                flag = self.read(cmd.transaction_id, cmd.variable_id)
            elif cmd.type == COMMAND_TYPE.WRITE:
                flag = self.write(cmd.transaction_id, cmd.variable_id, cmd.val)
            if flag:
                # remove executed commands
                self.command_queue.remove(cmd)
        return

    def operate(self, args: list) -> None:
        """
        Called by the main function to run a command
        """
        if len(args) == 0: # check an empty line
            return

        cmd = args.pop(0) # get the command

        if cmd == COMMAND_TYPE.BEGIN.value:
            self.begin(args[0]) # transaction_id
        elif cmd == COMMAND_TYPE.BEGINRO.value:
            self.beginRO(args[0]) # transaction_id
        elif cmd == COMMAND_TYPE.READ.value:
            # add the read command to the command queue
            self.command_queue.append(Command(COMMAND_TYPE.READ, args[0], args[1])) # (R, transaction_id, variable_id)
        elif cmd == COMMAND_TYPE.WRITE.value:
            # add the write command to the command queue
            self.command_queue.append(Command(COMMAND_TYPE.WRITE, args[0], args[1], args[2])) # (W, transaction_id, variable_id, value)
        elif cmd == COMMAND_TYPE.END.value:
            self.end(args[0])
        elif cmd == COMMAND_TYPE.FAIL.value:
            self.fail(int(args[0])-1) # Because we store indexes in self.sites
        elif cmd == COMMAND_TYPE.RECOVER.value:
            self.recover(int(args[0])-1)
        elif cmd == COMMAND_TYPE.DUMP.value:
            self.dump()
        else:
            # Simply ignore invalid inputs
            return
            
        self.timestamp += 1
        
        self.__udpate_command_queue()
        if self.__deadlock_detection():
            self.__udpate_command_queue()

    def begin(self, transaction_id: str) -> None:
        """
        Begin a transaction
        """
        self.transactions[transaction_id] = Transaction(transaction_id, self.timestamp, readOnly=False)
        if self.debug: print("{:7} --- Transaction: {} begins".format("Begin", transaction_id))
    
    def beginRO(self, transaction_id: str) -> None:
        """
        Begin a read-only transaction
        """
        self.transactions[transaction_id] = Transaction(transaction_id, self.timestamp, readOnly=True)
        if self.debug: print("{:7} --- Read-Only Transaction: {} begins".format("BeginRO", transaction_id))

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
                    if self.debug: print("{:7} --- Read-only transaction: {},  read from site {} -- {}: {}".format("Read", transaction_id, site.id, variable_id, val))
                    return True

            # Normal transactions
            else:
                ret, val = site.read(variable_id, transaction_id)
                if ret == True:
                    if self.debug: print("{:7} --- Transaction: {}, read from site {} -- {}: {}".format("Read", transaction_id, site.id, variable_id, val))
                    return True
        return False

    def write(self, transaction_id: str, variable_id: str, val: int) -> bool:
        """
        Write the transaction to all working sites
        """
        write_sites = []
        all_can_write = True
        for site in self.sites:
            site : DataManager
            if site.on_flag == True:
                if site.if_can_write(transaction_id, variable_id) == True:
                    ret = site.local_write(variable_id, val, transaction_id)
                    if ret: write_sites.append(site.id)
                else:
                    return False

        if self.debug: print("{:7} --- Transaction: {}, writes {}: {} in sites: {}".format("Write", transaction_id, variable_id, val, write_sites))
        return True    

    def dump(self) -> None:
        """
        Dump all data managers
        """
        print("\nDUMP\n")
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

        for tid in site.visiting_variables.keys():
            if self.transactions.get(tid):
                t : Transaction = self.transactions[tid]
                t.status = TRAN_STATUS.ABORTED

        if self.debug: print("Site: {} failed".format(site_id + 1)) # site_id is index
    
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
        
    def __deadlock_detection(self) -> bool:
        graph = defaultdict(set)    # Using adjacency list to represent the waits-for graph
                                    # graph[T1] = set(T2), means that T1 -> T2

        def lock_check(lock, q_lk, v):
            # (waiting_lock_type, waiting_transaction)
            if lock == LOCK.READ:
                if q_lk[0] == LOCK.READ or (len(v.read_lock_list) == 1 and q_lk[1] in v.read_lock_list):
                    return False
                return True
            return v.lock_by_trans_id != q_lk[1]
        
        def q_check(lk1,lk2):
            if lk1[0] == LOCK.READ and lk2[0] == LOCK.READ:
                 return False
            return not lk1[1] == lk2[1]

        # Generate the wait-for graph for the Data Manager 
        def generate_graph(site : DataManager) -> None:
            # Iterate all of the variables on the Data Manager
            for var in site.variables.values():
                var : Variable
                if not var.lock_waiting_queue or var.lock == LOCK.NONE:
                    continue

                curr_lock = var.lock
                # Iterate through the variable's lock_waiting_queue
                for lk in list(var.lock_waiting_queue):
                    if lock_check(curr_lock,lk,var):
                        if curr_lock == LOCK.READ:
                            for tid in var.read_lock_list:
                                if tid != lk[1]:
                                    graph[lk[1]].add(tid)
                                    # print(lk[11],tid)
                        else:
                            if var.lock_by_trans_id == lk[1]:
                                continue
                            graph[lk[1]].add(var.lock_by_trans_id)
                            # print(lk[1],var.lock_by_trans_id)

                # T’ is ahead of T on the wait queue for x and T’ seeks a conflicting lock on x.
                for j in range(len(var.lock_waiting_queue)):
                    for i in range(j):
                        if q_check(var.lock_waiting_queue[i],var.lock_waiting_queue[j]):
                            graph[var.lock_waiting_queue[j][1]].add(var.lock_waiting_queue[i][1])

        # Using dfs to detect if there's any cycle in the transaction graph
        def cycle(n, root, visited : set, g : defaultdict(set)) -> bool:
            visited.add(n)
            for neighbor in g[n]:
                if neighbor == root:
                    return True
                if neighbor not in visited:
                    if cycle(neighbor,root,visited,g):
                        return True
            return False
        
        # Step 1: Generate the waits-for graph for all working Data manager
        for site in self.sites:
            site : DataManager
            if site.on_flag == False:
                continue
            generate_graph(site)
        
        # Step 2: Deadlock detection using dfs
        aborted_transaction_id = None
        aborted_transaction_timestamp = float('-inf')
        for node in list(graph.keys()):
            if cycle(node,node, visited=set(), g=graph):
                aborted_transaction : Transaction = self.transactions[node]
  
                # finding the youngest transaction to abort
                if aborted_transaction.timestamp > aborted_transaction_timestamp:
                    aborted_transaction_id = node
                    aborted_transaction_timestamp = aborted_transaction.timestamp

        # Step 3: Generating outputs
        if aborted_transaction_id != None:
            print("Deadlock! Transaction {} aborted".format(aborted_transaction_id))
            self.__abort(aborted_transaction_id)
            return True
        return False