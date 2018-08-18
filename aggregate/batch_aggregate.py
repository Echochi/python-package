from typing import List

class BatchAggregate:
    
    def __init__(self):
        self.col_app = ['weight_', 
                        'sum_', 
                        'mode_', 
                        'mean_', 
                        'median_', 
                        'count_', 
                        'max_', 
                        'min_']
        self.agg_ops = ['sum', 
                        'sum', 
                        lambda x: x.value_counts().index[0] if len(x.value_counts()) else None, 
                        'mean', 
                        'median', 
                        'count', 
                        'max', 
                        'min']
        self._weight_cols = []
        self._sum_cols=[]
        self._mode_cols=[]
        self._mean_cols=[]
        self._median_cols=[]
        self._count_cols=[]
        self._max_cols=[]
        self._min_cols=[]
        pass
        

    def create_aggdict(self):
        """Input a list of columns names, naming the list according to the type of aggregation to be done and
        output dictionary for aggregation and a list of the new column names
        
        Returns:
            dictionary -- dictionary that can be used with with .agg
            list -- new column names of the resultant aggregated dataframe
        """
        agg_list = [self._weight_cols, 
                    self._sum_cols, 
                    self._mode_cols, 
                    self._mean_cols, 
                    self._median_cols, 
                    self._count_cols, 
                    self._max_cols,
                    self._min_cols]
        batch_dict = {}
        col_list = []
        
        for index, aggregation in enumerate(agg_list):
            for col_name in aggregation:
                if col_name in batch_dict:
                    batch_dict[col_name] += [self.agg_ops[index]]
                else:
                    batch_dict[col_name] = [self.agg_ops[index]]
                col_list.append(self.col_app[index] + col_name)
            
        return batch_dict, col_list

    @property
    def weight_cols(self):
        """List of column names to undergo 'weighted sum' aggregation."""
        print("getter of weight_cols called")
        return self._weight_cols

    @weight_cols.setter
    def weight_cols(self, value: List[str]):
        print("setter of weight_cols called")
        self._weight_cols = value

    @weight_cols.deleter
    def weight_cols(self):
        print("deleter of weight_cols called")
        self._weight_cols = []

    @property
    def sum_cols(self):
        """List of column names to undergo 'sum' aggregation."""
        print("getter of sum_cols called")
        return self._sum_cols

    @sum_cols.setter
    def sum_cols(self, value: List[str]):
        print("setter of sum_cols called")
        self._sum_cols = value

    @sum_cols.deleter
    def sum_cols(self):
        print("deleter of sum_cols called")
        self._sum_cols = []

    @property
    def mode_cols(self):
        """List of column names to undergo 'mode' aggregation."""
        print("getter of mode_cols called")
        return self._mode_cols

    @mode_cols.setter
    def mode_cols(self, value: List[str]):
        print("setter of mode_cols called")
        self._mode_cols = value

    @mode_cols.deleter
    def mode_cols(self):
        print("deleter of mode_cols called")
        self._mode_cols = []

    @property
    def mean_cols(self):
        """List of column names to undergo 'mean' aggregation."""
        print("getter of mean_cols called")
        return self._mean_cols

    @mean_cols.setter
    def mean_cols(self, value: List[str]):
        print("setter of mean_cols called")
        self._mean_cols = value

    @mean_cols.deleter
    def mean_cols(self):
        print("deleter of mean_cols called")
        self._mean_cols = []

    @property
    def median_cols(self):
        """List of column names to undergo 'median' aggregation."""
        print("getter of median_cols called")
        return self._median_cols

    @median_cols.setter
    def median_cols(self, value: List[str]):
        print("setter of median_cols called")
        self._median_cols = value

    @median_cols.deleter
    def median_cols(self):
        print("deleter of median_cols called")
        self._median_cols = []

    @property
    def count_cols(self):
        """List of column names to undergo 'count' aggregation."""
        print("getter of count_cols called")
        return self._count_cols

    @count_cols.setter
    def count_cols(self, value: List[str]):
        print("setter of count_cols called")
        self._count_cols = value

    @count_cols.deleter
    def count_cols(self):
        print("deleter of count_cols called")
        self._count_cols = []

    @property
    def max_cols(self):
        """List of column names to undergo 'max' aggregation."""
        print("getter of max_cols called")
        return self._max_cols

    @max_cols.setter
    def max_cols(self, value: List[str]):
        print("setter of max_cols called")
        self._max_cols = value

    @max_cols.deleter
    def max_cols(self):
        print("deleter of max_cols called")
        self._max_cols = []

    @property
    def min_cols(self):
        """List of column names to undergo 'min' aggregation."""
        print("getter of min_cols called")
        return self._min_cols

    @min_cols.setter
    def min_cols(self, value: List[str]):
        print("setter of min_cols called")
        self._min_cols = value

    @min_cols.deleter
    def min_cols(self):
        print("deleter of min_cols called")
        self._min_cols = []
    