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
        self._weight_sum = []
        self._sum_=[]
        self._mode_=[]
        self._mean_=[]
        self._median_=[]
        self._count_=[]
        self._max_=[]
        self._min_=[]
        pass
        

    def create_aggdict(self):
        """Input a list of columns names, naming the list according to the type of aggregation to be done and
        output dictionary for aggregation and a list of the new column names
        
        Returns:
            dictionary -- dictionary that can be used with with .agg
            list -- new column names of the resultant aggregated dataframe
        """
        agg_list = [self._weight_sum, 
                    self._sum_, 
                    self._mode_, 
                    self._mean_, 
                    self._median_, 
                    self._count_, 
                    self._max_, s
                    self._min_]
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
    def weight_sum(self):
        """List of column names to undergo 'weighted sum' aggregation."""
        print("getter of weight_sum called")
        return self._weight_sum

    @weight_sum.setter
    def weight_sum(self, value: List[str]):
        print("setter of weight_sum called")
        self._weight_sum = value

    @weight_sum.deleter
    def weight_sum(self):
        print("deleter of weight_sum called")
        self._weight_sum = []

    @property
    def sum_(self):
        """List of column names to undergo 'sum' aggregation."""
        print("getter of sum_ called")
        return self._sum_

    @sum_.setter
    def sum_(self, value: List[str]):
        print("setter of sum_ called")
        self._sum_ = value

    @sum_.deleter
    def sum_(self):
        print("deleter of sum_ called")
        self._sum_ = []

    @property
    def mode_(self):
        """List of column names to undergo 'mode' aggregation."""
        print("getter of mode_ called")
        return self._mode_

    @mode_.setter
    def mode_(self, value: List[str]):
        print("setter of mode_ called")
        self._mode_ = value

    @mode_.deleter
    def mode_(self):
        print("deleter of mode_ called")
        self._mode_ = []

    @property
    def mean_(self):
        """List of column names to undergo 'mean' aggregation."""
        print("getter of mean_ called")
        return self._mean_

    @mean_.setter
    def mean_(self, value: List[str]):
        print("setter of mean_ called")
        self._mean_ = value

    @mean_.deleter
    def mean_(self):
        print("deleter of mean_ called")
        self._mean_ = []

    @property
    def median_(self):
        """List of column names to undergo 'median' aggregation."""
        print("getter of median_ called")
        return self._median_

    @median_.setter
    def median_(self, value: List[str]):
        print("setter of median_ called")
        self._median_ = value

    @median_.deleter
    def median_(self):
        print("deleter of median_ called")
        self._median_ = []

    @property
    def count_(self):
        """List of column names to undergo 'count' aggregation."""
        print("getter of count_ called")
        return self._count_

    @count_.setter
    def count_(self, value: List[str]):
        print("setter of count_ called")
        self._count_ = value

    @count_.deleter
    def count_(self):
        print("deleter of count_ called")
        self._count_ = []

    @property
    def max_(self):
        """List of column names to undergo 'max' aggregation."""
        print("getter of max_ called")
        return self._max_

    @max_.setter
    def max_(self, value: List[str]):
        print("setter of max_ called")
        self._max_ = value

    @max_.deleter
    def max_(self):
        print("deleter of max_ called")
        self._max_ = []

    @property
    def min_(self):
        """List of column names to undergo 'min' aggregation."""
        print("getter of min_ called")
        return self._min_

    @min_.setter
    def min_(self, value: List[str]):
        print("setter of min_ called")
        self._min_ = value

    @min_.deleter
    def min_(self):
        print("deleter of min_ called")
        self._min_ = []
    