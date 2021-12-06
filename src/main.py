import sys
from transactionManager import *
from iohandler import Parser

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        filename = sys.argv[1]
        parser = Parser(filename)
        tm = TransactionManager()
        parser.parse_file()
        
        cmd = parser.get_operation()
        while cmd is not None:
            tm.get_operation(cmd) 
            print(cmd)
            cmd = parser.get_operation()
    else:
        print('ERROR: PLEASE INPUT THE COMMAND FILE!')

