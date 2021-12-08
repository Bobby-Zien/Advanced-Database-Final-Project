from collections import defaultdict, OrderedDict, deque
from enum import Enum

# Lock status for a variable
class LOCK(Enum):
    NONE = 'NONE'
    READ = 'READ'
    WRITE = 'WRITE'

# Data manager's status
class DM_STATUS(Enum):
    WORKING = 'WORKING'
    DOWN = 'DOWN'
    POWEROFF = 'POWEROFF'

class VAR_STATUS(Enum):
    READY = 'READY'
    UNAVAILABLE = 'UNAVAILABLE'
    RECOVERING = 'RECOVERING'

class Variable:
    def __init__(self, id: str, val: int, lock: LOCK, even: bool) -> None:
        """
        id (str): variable id
        val (int): value
        lock (LOCK): lock type 
        even (bool): even variables
        commited_val(dict): key is timestamp, value is the value commited at that time
        current_val(int): local modification that haven't been submitted

        lock_waiting_queue:each item is a tuple of (lock_type, transaction_id)
        """
        self.id = id
        self.value = val
        self.commited_val = OrderedDict({0:val})
        self.even = even
        self.current_val = val
        self.status = VAR_STATUS.READY

        # lock 
        self.lock = lock
        self.lock_by_trans_id = None
        self.read_lock_list = set()
        self.lock_waiting_queue = deque()  

    def promote_lock(self, tid: str) -> bool:
        """[summary]
        promote the current read lock to write lock
        Args:
            tid ([str]): transaction id

        Returns:
            bool:  successful or not
        """
        if tid == self.lock_by_trans_id and len(self.read_lock_list) == 1:
            self.read_lock_list = set()
            self.lock = LOCK.WRITE
        
    def has_write_waiting(self) -> bool:
        for l in self.lock_waiting_queue:
            if l[0] == LOCK.WRITE:
                return True
        return False
    
    def need_wait_to_write(self, tid: str) -> bool:
        """
        tid (str)：transaction_id
        """
        if len(self.read_lock_list) > 1 or tid not in self.read_lock_list or self.has_write_waiting():
            return True
        return False

    def release_lock(self, tid: str) -> None:
        """
        tid: transaction ID
        """
        if self.lock == LOCK.NONE:
            return

        if self.lock == LOCK.WRITE and tid == self.lock_by_trans_id:
            self.lock = LOCK.NONE
            self.lock_by_trans_id = None  
        elif self.lock == LOCK.READ and tid in self.read_lock_list:
            self.read_lock_list.remove(tid)
            if len(self.read_lock_list) <1:
                self.lock = LOCK.NONE
                self.lock_by_trans_id = None  

    def add_lock_waiting_queue(self, lock : LOCK, trans_id: str) -> None:
        for type, tid in list(self.lock_waiting_queue):
            # Avoid duplicates
            if type == lock and tid == trans_id:
                return
        self.lock_waiting_queue.append((lock, trans_id))

    def update_lock_waiting_queue(self) -> None:
        if self.lock_waiting_queue:
            if self.lock == LOCK.NONE:
                self.lock, self.lock_by_trans_id = self.lock_waiting_queue.popleft()
                #print("update: {}, {}".format(self.lock, self.lock_by_trans_id))
            elif self.lock == LOCK.READ:
                for lck in list(self.lock_waiting_queue):
                    lock_type, trans_id = lck
                    if lock_type == LOCK.WRITE and len(self.read_lock_list) == 1 and trans_id in self.read_lock_list:
                        self.promote_lock(trans_id)
                        self.lock_waiting_queue.remove(lck)
                        break
                    if not self.need_wait_to_write(trans_id):
                        self.read_lock_list.add(trans_id)
                        self.lock_waiting_queue.remove(lck)
    
    def remain_lock(self, tid: str):
        for _, l in self.lock_waiting_queue:
            if l == tid:
                return True
        return False

class DataManager:
    def __init__(self, id: int) -> None:
        """[summary]
        variables (dict): 
        visiting_variables (dict) (transaction_id, set(Variable)) :
        """

        self.id = id
        self.variables = defaultdict(Variable)
        self.visiting_variables = defaultdict(set)
        self.on_flag = True

        # Initialize variable table
        for i in range(1, 21):
            variable_id = "x" + str(i)
            if i % 2 == 0:
                self.variables[variable_id] = Variable(variable_id, i * 10, LOCK.NONE, even=True)
            elif i % 10 + 1 == id:
                self.variables[variable_id] = Variable(variable_id, i * 10, LOCK.NONE, even=False)
  
    def add_lock(self, variable_id: str, lock: LOCK) -> bool:
        if variable_id in self.variables:
            if self.variables[variable_id].lock == LOCK.NONE:
                self.variables[variable_id].lock = lock
                return True
            elif self.variables[variable_id].lock == LOCK.READ and lock == LOCK.READ:
                return True
            elif self.variables[variable_id].lock == LOCK.WRITE:
                return False
            else:
                return False
        return False

    def release_lock(self, variable_id: str) -> bool:
        if variable_id in self.variables:
            self.variables[variable_id].lock = LOCK.NONE
            return True
        return False

    def if_can_write(self, trans_id: str, var_id: str) -> bool:
        """
        check if a transaction can wirte the variable on this site

        Args:
            trans_id ([str]): transaction id
            var_id ([str]): variable id

        Returns:
            bool: True means can, False means no
        """
        if var_id not in self.variables:
            return True
        var : Variable = self.variables[var_id]
        if var.lock == LOCK.NONE:
            var.lock = LOCK.WRITE
            var.lock_by_trans_id = trans_id
            return True
        elif var.lock == LOCK.READ:
            if var.need_wait_to_write(trans_id):
                var.add_lock_waiting_queue(LOCK.WRITE, trans_id)
                return False
            var.promote_lock(trans_id)
            return True
        else:
            if var.lock_by_trans_id == trans_id:
                return True 
            var.add_lock_waiting_queue(LOCK.WRITE, trans_id)
            return False
        return False

    def local_write(self, variable_id: str, val: int, transaction_id: str) -> bool:
        if variable_id in self.variables:
            self.variables[variable_id].current_val = val
            v : Variable = self.variables[variable_id]
            self.visiting_variables[transaction_id].add(v) 
            return True
        return False
    
    def read(self, variable_id: str, tid: str):
        """[summary]
        a transaction is asked to read a variable
        Returning None means the read is not succeed
        Args:
            variable_id (str): 
            tid (str): transaction id 

        Returns:
            int: the value of the variable
        """
        if variable_id in self.variables:
            var : Variable = self.variables[variable_id]
            self.visiting_variables[tid].add(var)
            if var.status != VAR_STATUS.READY:
                return False, None
            if var.lock == LOCK.NONE:
                var.lock = LOCK.READ
                var.lock_by_trans_id = tid
                var.read_lock_list.add(tid)
                return True, var.commited_val[next(reversed(var.commited_val))]
            elif var.lock == LOCK.READ:
                if tid in var.read_lock_list:
                    return True, var.commited_val[next(reversed(var.commited_val))]
                if var.has_write_waiting():
                    var.add_lock_waiting_queue(LOCK.READ, tid)
                    return False, None
                else:
                    var.read_lock_list.add(tid)
                    return True, var.commited_val[next(reversed(var.commited_val))]
            elif var.lock_by_trans_id == tid:
                return True, var.commited_val[next(reversed(var.commited_val))]
            var.add_lock_waiting_queue(LOCK.READ, tid)
        return False, None

    def commit(self, transaction_id: str, ts: int) -> None:
        error = False
        for var in self.variables.values():
            var : Variable
            if var.lock == LOCK.WRITE and var.lock_by_trans_id == transaction_id:
                var.commited_val[ts] = var.current_val
                var.status = VAR_STATUS.READY
            var.release_lock(transaction_id)
            if var.remain_lock(transaction_id):
                error = True
                break
            var.update_lock_waiting_queue()

        if error: 
            print("COMMIT ERROR: transaction {} has remaining locks".format(transaction_id))
            return False
        return True

    def abort(self, transaction_id: str)-> None:
        """[summary]
        delete all the waiting locks from this transaction in the lock waiting queue and release the lock
        and update the variable with a new lock
        Args:
            transaction_id (str): 
        """
        for var in self.variables.values():
            var : Variable
            var.release_lock(transaction_id)
            for l in list(var.lock_waiting_queue):
                if l[1] == transaction_id:
                    var.lock_waiting_queue.remove(l)
            var.update_lock_waiting_queue()
        return True

    def fail(self) -> None:
        for variable in self.variables.values():
            variable : Variable
            variable.lock = LOCK.NONE
            variable.status = VAR_STATUS.UNAVAILABLE
            variable.lock_waiting_queue = deque()
            variable.read_lock_list = set()
        self.on_flag = False

    def recover(self) -> bool:
        if self.on_flag:
            print("Can't recover. The site is already working")
            return False
        else:
            self.on_flag = True
            for variable in self.variables.values():
                variable : Variable
                if variable.even:
                    variable.status = VAR_STATUS.RECOVERING
                else:
                    variable.status = VAR_STATUS.READY
            return True
        return False

    def snapshot(self, timestamp: int, var_id: str):
        """[summary]
        Read function for read-only transactions 

        Args:
            timestamp (int): start time of the RO transaction
            var_id (str): var_id

        Returns:
            bool: successful or not
            int or None: int for the value, None if fail
        """
        if var_id in self.variables:
            current_variable : Variable = self.variables[var_id]
            value = 0
            for ts, v in current_variable.commited_val.items():
                if ts <= timestamp:
                    value = v
                else:
                    break
            return True, value
        return False, None

    def dump(self) -> None:
        """[summary]
        print the info
        """
        # Generating outputs
        return_str = "Site {:2} - ".format(self.id)
        for var in self.variables.values():
            var : Variable
            return_str += " {:2}: {:<5}".format(var.id, var.commited_val[next(reversed(var.commited_val))])
        print(return_str[:-1])  