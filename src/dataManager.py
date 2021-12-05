from collections import defaultdict
from enum import Enum

class LOCK(Enum):
    NONE = 'NONE'
    READ = 'READ'
    WRITE = 'WRITE'

class Variable:
    def __init__(self, id: str, val: int, lock: LOCK, copied: bool) -> None:
        """
        id (str): variable id
        val (int): value
        lock (LOCK): lock type 
        copied (bool): copied to multiple sites or not
        commited_val(dict): key is timestamp, value is the value commited at that time
        current_val(int): local modification that haven't been submitted
        """
        self.id = id
        self.value = val
        self.lock = lock
        self.commited_val = {0:val}
        self.copied = copied
        self.current_val = val

class DataManager:
    def __init__(self, id: int) -> None:
        self.id = id
        self.variables = defaultdict(Variable)
        self.uncommited_variables = []
        self.on_flag = True
        self.current_transactions = set()

        # Initialize variable table
        for i in range(1, 21):
            variable_id = "x" + str(i)
            if i % 2 == 0:
                self.variables[variable_id] = Variable(variable_id, i * 10, LOCK.NONE, copied=True)
            elif i % 10 + 1 == id:
                self.variables[variable_id] = Variable(variable_id, i * 10, LOCK.NONE, copied=False)
  
    #######TODO############
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

    #######TODO############
    def release_lock(self, variable_id: str) -> bool:
        if variable_id in self.variables:
            self.variables[variable_id].lock = LOCK.NONE
            return True
        return False



    def _local_write(self, variable_id: str, val: int, transaction_id) -> bool:
        if variable_id in self.variables:
            self.variables[variable_id].current_val = val
            self.current_transactions(transaction_id)
            return True
        return False
    
    #######TODO############
    def get_value(self, variable_id: str) -> int:
        if variable_id in self.variables:
            return self.variables[variable_id].value
        return -1

    #######TODO############
    def commit(self, variable_id: str, value: int) -> bool:
        pass

    #######TODO############
    def abort(self, transaction_id: int) -> bool:
        pass

    def fail(self) -> None:
        for variable in self.variables:
            variable.lock = LOCK.NONE
        self.uncommited_variables = []
        self.on_flag = False

    def recover(self) -> bool:
        if self.on_flag:
            print("Can't recover. The site is already working")
            return False
        else:
            self.on_flag = True
            self.current_transactions = set()
            print("Successfully recovered site %s ." % (self.id))
            return True


    def snapshot(self, timestamp: int, var: str):
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

    # TODO
    def generate_var_graph(self):
        pass