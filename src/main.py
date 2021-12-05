import parser
from transactionManager import *
from iohandler import Parser
if __name__ == '__main__':
    if len(sys.argv) >= 2:
        filename = sys.argv[1]
        parser = Parser(filename)
        tm = TransactionManager()
        parser.parse_file()  
        
        while cmd is not None:
            TransactionManager.get_operation(cmd) 
            cmd = parser.get_operation()
    else:
        print('ERROR: PLEASE INPUT THE COMMAND FILE!')

