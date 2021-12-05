import re
class Parser:

    def __init__(self, file_name=None) -> None:
        self.FINISH_FLAG = False
        self.file_name = file_name
        self.opertaions = []
    
    def parse_file(self) -> None:
        try:
            with open(self.file_name, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    args = self._parse_line(line)
                    self.opertaions.append(args)
        except IOError:
            print("FATAL ERROR: Invalid input file {}".format(self.file_name))


    def parse_line(self, line:str):
        line = line.split('//')[0].strip()
        return re.findall(r"[\w]+", line)

    def get_operation(self):
        if len(self.opertaions) > 0:
            return self.opertaions.pop(0)
        return None