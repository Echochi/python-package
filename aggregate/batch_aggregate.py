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
        pass
        

    def create_aggdict(self, sum_=[], mode_=[], mean_=[], median_=[], count_=[], max_=[], min_=[]):
        """Input a list of columns names, naming the list according to the type of aggregation to be done and
        output dictionary for aggregation and a list of the new column names
        
        Keyword Arguments:
            weight_sum {list} -- columns to be weighted, alternative to mode (default: {[]})
            sum_ {list} -- sum aggregations (default: {[]})
            mode_ {list} -- mode aggregations (default: {[]})
            mean_ {list} -- mean aggregations (default: {[]})
            median_ {list} -- median aggregations (default: {[]})
            count_ {list} -- count aggregations (default: {[]})
            max_ {list} -- max aggregations (default: {[]})
            min_ {list} -- min aggregations (default: {[]})
        
        Returns:
            dictionary -- dictionary that can be used with with .agg
            list -- new column names of the resultant aggregated dataframe
        """
        agg_list = [self._weight_sum, sum_, mode_, mean_, median_, count_, max_, min_]
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
        """The 'weight_sum' property."""
        print("getter of weight_sum called")
        return self._weight_sum

    @weight_sum.setter
    def weight_sum(self, value):
        print("setter of weight_sum called")
        self._weight_sum = value

    @weight_sum.deleter
    def weight_sum(self):
        print("deleter of weight_sum called")
        self._weight_sum = []