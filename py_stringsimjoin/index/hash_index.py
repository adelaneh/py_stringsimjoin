from py_stringsimjoin.index.index import Index


class HashIndex(Index):
    """Builds a hash index on the input column in the input table.                                                                  
                                                             
    Hash index is used by exact join.
                                                                                
    Args:                                                                       
        table (list): Input table as list of tuples.
        index_attr (int): Position of the column in the tuple, on which hash
            index has to be built.
                                                                                
    Attributes:                                                                 
        table (list): An attribute to store the input table.                            
        index_attr (int): An attribute to store the position of the column in
            the tuple.                                              
        index (dict): A Python dictionary storing the hash index where the
            key is the column value and value is a list of tuple ids. 
            Currently, we use the index of the tuple in the table as its id.
    """ 

    def __init__(self, table, index_attr):
        
        self.table = table
        self.index_attr = index_attr
        self.index = None
        super(self.__class__, self).__init__()

    def build(self):
        """Build hash index."""
        self.index = {}
        row_id = 0
        for row in self.table:
            index_string = row[self.index_attr]

            if self.index.get(index_string) is None:
                self.index[index_string] = []
            self.index.get(index_string).append(row_id)

            row_id += 1

        return True

    def probe(self, probe_string):
        """Probe the hash index using the input string."""
        return self.index.get(probe_string, [])
