class BatchAgg:
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
        
        return

    def create_aggdict(self, weight_sum=[], sum_=[], mode_=[], mean_=[], median_=[], count_=[], max_=[], min_=[]):
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
        agg_list = [weight_sum, sum_, mode_, mean_, median_, count_, max_, min_]
        x = {}
        col_list = []
        
        for index, aggregation in enumerate(agg_list):
            for col_name in aggregation:
                if col_name in x:
                    x[col_name] += [self.agg_ops[index]]
                else:
                    x[col_name] = [self.agg_ops[index]]
                col_list.append(self.col_app[index] + col_name)
            
        return x, col_list