from _typeshed import ReadableBuffer
from collections import defaultdict, OrderedDict, deque
from ExceptionHandler import *
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
        copied (bool): copied to multiple sites or not
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

    def promote_lock(self, tid) -> bool:
        """[summary]
        promote the current read lock to write lock
        Args:
            tid ([type]): transaction id

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
    

    def need_wait_to_write(self, tid):
        if len(self.read_lock_list) > 1 or tid not in var.read_lock_list or self.has_write_waiting():
            return True

    def release_lock(self, tid):
        if self.lock == LOCK.WRITE and tid == self.lock_by_trans_id:
            self.current_lock = None    
        elif self.lock == LOCK.READ and tid in self.read_lock_list:
            self.read_lock_list.remove(tid)
            if len(self.read_lock_list) <1:
                self.lock = LOCK.NONE  

    
    def lock_waiting_queue_pop(self):
        if self.lock_waiting_queue:
            if self.lock == LOCK.NONE:
                self.lock, self.lock_by_trans_id = self.lock_waiting_queue.popleft()
            elif self.lock == LOCK.READ:
                for lock in list(self.lock_waiting_queue):
                    if lock[0] == LOCK.WRITE and len(self.read_lock_list) == 1:
                        self.promote_current_lock(tid)
                        self.lock_queue.remove(lk)
                        break
                    self.read_lock_list.add(lock[1])
                    self.lock_queue.remove(lock) 
    
    def remain_lock(self, tid):
        for _,l in self.lock_waiting_queue:
            if l == tid:
                return True
        return False



class DataManager:
    def __init__(self, id: int) -> None:
        """[summary]
        variables (dict): 
        uncommited_variables (dict):
        """

        self.id = id
        self.variables = defaultdict(Variable)
        self.uncommited_variables = defaultdict(set)
        self.on_flag = True

        # Initialize variable table
        for i in range(1, 21):
            variable_id = "x" + str(i)
            if i % 2 == 0:
                self.variables[variable_id] = Variable(variable_id, i * 10, LOCK.NONE, copied=True)
            elif i % 10 + 1 == id:
                self.variables[variable_id] = Variable(variable_id, i * 10, LOCK.NONE, copied=False)
  
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

    def if_can_write(self, trans_id, var_id) -> bool:
        """
        check if a transaction can wirte the variable on this site

        Args:
            trans_id ([type]): transaction id
            var_id ([type]): variable id

        Returns:
            bool: True means can, False means no
        """
        if var_id not in self.variables:
            return True
        var = self.variables[var_id]
        if var.lock == LOCK.NONE:
            var.lock = LOCK.WRITE
            var.lock_by_trans_id = trans_id
            return True
        elif var.lock == LOCK.READ:
            if var.need_wait_to_write:
                var.lock_waiting_queue.append((LOCK.WRITE, trans_id))
                return False
            var.promote_lock(trans_id)
            return True
        else:
            if var.lock_by_trans_id == trans_id:
                return True
            var.lock_waiting_queue.append((LOCK.WRITE, trans_id))
            return False
        

    def local_write(self, variable_id: str, val: int, transaction_id) -> bool:
        if variable_id in self.variables:
            self.variables[variable_id].current_val = val
            self.uncommited_variables[transaction_id].add(self.variables[variable_id]) 
            return True
        return False
    
    def read(self, variable_id: str, tid) -> int:
        """[summary]
        a transaction is asked to read a variable
        Returning None means the read is not succeed
        Args:
            variable_id (str): 
            tid : transaction id 

        Returns:
            int: the value of the variable
        """
        if variable_id in self.variables:
            var = self.variables[var_id]
            if var.status != VAR_STATUS.READY:
                return None
            if var.lock == LOCK.NONE:
                var.lock = LOCK.READ
                var.lock_by_trans_id = tid
                var.read_lock_list.add(tid)
                return var.commited_val[next(reversed(var.commited_val))]
            elif var.lock == LOCK.READ:
                if tid in var.read_lock_list:
                    return var.commited_val[next(reversed(var.commited_val))]
                if var.has_write_waiting():
                    var.lock_waiting_queue.append((LOCK.READ, tid))
                    return None
                else:
                    var.read_lock_list.add(tid)
                    return var.commited_val[next(reversed(var.commited_val))]
            elif var.lock_by_trans_id == tid:
                return var.commited_val[next(reversed(var.commited_val))]
            var.lock_waiting_queue.append((LOCK.READ, tid))
        return None

    
    def commit(self, transaction_id, ts:int) -> None:
        """[summary]
        commit the uncommited variable written by this transaction 

        Args:
            transaction_id ([type]): transaction_id
            ts (int): timestamp of the transaciton ending(commiting)
        """
        if len(self.uncommited_variables[transaction_id]) < 1:
            return 
        for var in self.uncommited_variables[transaction_id]:
            var.commited_val[ts] = var.current_val
            var.status = VAR_STATUS.READY
            var.lock_waiting_queue_pop()
        del self.uncommited_variables[transaction_id]

        for var in self.variables.values():
            var.release_current_lock(transaction_id)
            if var.remain_lock():
                print("COMMIT ERROR: transaction {} has remaining locks".format(trans_id))
            var.lock_waiting_queue_pop()

    def abort(self, transaction_id: int)-> None:
        """[summary]
        delete all the waiting locks from this transaction in the lock waiting queue and release the lock
        and update the variable with a new lock
        Args:
            transaction_id (int): 
        """
        for var in self.variables.values():
            var.release_current_lock(transaction_id)
            for l in list(var.lock_waiting_queue):
                if l[1] == transaction_id:
                    var.lock_waiting_queue.remove(l)
            var.lock_waiting_queue_pop()

    def fail(self) -> None:
        for variable in self.variables.values():
            variable.lock = LOCK.NONE
            variable.status = VAR_STATUS.UNAVAILABLE
            variable.lock_waiting_queue = deque()
            variable.read_lock_list = set()
        self.uncommited_variables = defaultdict(set)
        self.on_flag = False

    def recover(self) -> bool:
        if self.on_flag:
            print("Can't recover. The site is already working")
            return False
        else:
            self.on_flag = True
            for variable in self.variables.values():
                if variable.even:
                    self.status = VAR_STATUS.RECOVERING
                else:
                    self.status = VAR_STATUS.READY
            print("Successfully recovered site %s ." % (self.id))
            return True


    def snapshot(self, timestamp: int, var: str):
        """[summary]
        Read function for read-only transactions 

        Args:
            timestamp (int): start time of the RO transaction
            var (str): var_id

        Returns:
            bool: successful or not
            int or None: int for the value, None if fail
        """
        if var in self.variables:
            current_variable = self.variables[var]
            value = 0
            for ts, v in current_variable.commited_val.items():
                if ts <= timestamp:
                    value = v
                else:
                    break
            return True, value
        return False, None

    def dump():
        """[summary]
        print the info
        """
        return_str = "Site {} - ".format(self.id)
        for var in self.variables.values():
            res += " {}: {} ,".format(var.id, var.commited_val[next(reversed(var.commited_val))])
        print(res[:-1])  

    # TODO
    def generate_var_graph(self):
        pass