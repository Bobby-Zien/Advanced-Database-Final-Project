import parser

from iohandler import Parser
if __name__ == '__main__':
    if len(sys.argv) >= 2:
        filename = sys.argv[1]
        parser = Parser(filename)
        # call transaction manager inside parser.parse_file() when traversing the operation lines
        parser.parse_file()  
    else:
        print('ERROR: PLEASE INPUT THE COMMAND FILE!')

