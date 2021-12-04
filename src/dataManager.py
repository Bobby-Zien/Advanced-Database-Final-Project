from collections import defaultdict
from enum import Enum

class LOCK(Enum):
    NONE = 'NONE'
    READ = 'READ'
    WRITE = 'WRITE'

class Variable:
    def __init__(self, id: str, val: int, lock: LOCK) -> None:
        self.id = id
        self.value = val
        self.lock = lock

class DataManager:
    def __init__(self, id: int) -> None:
        self.id = id
        self.variables = defaultdict(Variable)
        self.uncommited_variables = []

        # Initialize variable table
        for i in range(1, 21):
            variable_id = "x" + str(i)
            self.variables[variable_id] = Variable(variable_id, i * 10, LOCK.NONE)
  
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

    def update(self, variable_id: str, val: int) -> bool:
        if variable_id in self.variables:
            self.variables[variable_id].value = val
            return True
        return False
    
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

    #######TODO############
    def recover(self) -> bool:
        pass

    #######TODO############
    def snapshot(self):
        pass