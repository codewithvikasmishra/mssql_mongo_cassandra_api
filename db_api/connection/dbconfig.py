from configparser import ConfigParser
import os

class Read_Config():
    def __init__(self)->None:
        pass

    def read_config(self,section,filename='config.ini'):
        path_file=os.path.dirname(__file__)
        file=path_file+'/'+filename
        parser = ConfigParser()
        parser.read(file)
        db = {}
        if parser.has_section(section):
            items = parser.items(section)
            for item in items:
                db[item[0]] = item[1]
        else:
            raise Exception('{0} not found in the {1} file'.format(section, filename))
        return db