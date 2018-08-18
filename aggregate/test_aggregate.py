import unittest
from aggregate.batch_aggregate import BatchAggregate

class Test_AggTestCase(unittest.TestCase):
    def test_dict(self):
        agg = BatchAggregate()
        agg.sum_cols = ['batch']
        agg_dict, col_list = agg.create_aggdict()
        self.assertEqual({'batch':['sum']}, agg_dict )
        self.assertEqual(['sum_batch'], col_list)

if __name__ == '__main__':
    unittest.main()
