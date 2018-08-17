class BatchAgg:
    def __init__(self):
        return

    def create_aggdict(self, weight_sum=[], sum_=[], mode_=[], mean_=[], median_=[], count_=[], max_=[], min_=[]):
        agg_list = [weight_sum, sum_, mode_, mean_, median_, count_, max_, min_]
        agg_ops = ['sum', 'sum', lambda x: x.value_counts().index[0] if len(x.value_counts()) else None, 'mean', 'median', 'count', 'max', 'min']
        col_app = ['weight_', 'sum_', 'mode_', 'mean_', 'median_', 'count_', 'max_', 'min_']
        x = {}
        col_list = []
        
        for index, aggregation in enumerate(agg_list):
            for col_name in aggregation:
                if col_name in x:
                    x[col_name] += [agg_ops[index]]
                else:
                    x[col_name] = [agg_ops[index]]
                col_list.append(col_app[index] + col_name)
            
        return x, col_list