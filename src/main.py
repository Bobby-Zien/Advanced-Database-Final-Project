import sys
from transactionManager import *
from iohandler import Parser

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        filename = sys.argv[1]
        parser = Parser(filename)
        tm = TransactionManager()
        tm.debug = True # set to true if you'd like to see more debug logs
        parser.parse_file()
        
        print("\n----- RUNNING TRANSACTION MANAGER -----")
        cmd = parser.get_operation()
        while cmd is not None:
            tm.get_operation(cmd) 
            cmd = parser.get_operation()
        print("\n-------- FINISHED ---------\n")
    else:
        print('ERROR: PLEASE INPUT THE COMMAND FILE!')

