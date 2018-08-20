import unittest
from BatchAggregate.batch_aggregate import BatchAggregate

class Test_AggTestCase(unittest.TestCase):

    def test_dict_sum(self):
        agg = BatchAggregate()
        agg.sum_cols = ['batch']
        agg_dict, col_list = agg.create_aggdict()
        self.assertEqual({'batch':['sum']}, agg_dict )
        self.assertEqual(['sum_batch'], col_list)

    def test_dict_weight(self):
        agg = BatchAggregate()
        agg.weight_cols = ['batch']
        agg_dict, col_list = agg.create_aggdict()
        self.assertEqual({'batch':['sum']}, agg_dict )
        self.assertEqual(['weight_batch'], col_list)

    # def test_dict_mode(self):
    #     agg = BatchAggregate()
    #     agg.mode_cols = ['batch']
    #     agg_dict, col_list = agg.create_aggdict()
    #     self.assertEqual({'batch':[}, agg_dict )
    #     self.assertEqual(['mode_batch'], col_list)

    def test_dict_mean(self):
        agg = BatchAggregate()
        agg.mean_cols = ['batch']
        agg_dict, col_list = agg.create_aggdict()
        self.assertEqual({'batch':['mean']}, agg_dict )
        self.assertEqual(['mean_batch'], col_list)

    def test_dict_median(self):
        agg = BatchAggregate()
        agg.median_cols = ['batch']
        agg_dict, col_list = agg.create_aggdict()
        self.assertEqual({'batch':['median']}, agg_dict )
        self.assertEqual(['median_batch'], col_list)

    def test_dict_count(self):
        agg = BatchAggregate()
        agg.count_cols = ['batch']
        agg_dict, col_list = agg.create_aggdict()
        self.assertEqual({'batch':['count']}, agg_dict )
        self.assertEqual(['count_batch'], col_list)

    def test_dict_min(self):
        agg = BatchAggregate()
        agg.min_cols = ['batch']
        agg_dict, col_list = agg.create_aggdict()
        self.assertEqual({'batch':['min']}, agg_dict )
        self.assertEqual(['min_batch'], col_list)

    def test_dict_max(self):
        agg = BatchAggregate()
        agg.max_cols = ['batch']
        agg_dict, col_list = agg.create_aggdict()
        self.assertEqual({'batch':['max']}, agg_dict )
        self.assertEqual(['max_batch'], col_list)

if __name__ == '__main__':
    unittest.main()
