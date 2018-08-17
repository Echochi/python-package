import unittest
from aggregate.batch_aggdict import BatchAgg

class Test_AggTestCase(unittest.TestCase):
    def test_dict(self):
        agg = BatchAgg()
        agg_dict, col_list = agg.create_aggdict(sum_=['batch'])
        self.assertEqual({'batch':['sum']}, agg_dict )
        self.assertEqual(['sum_batch'], col_list)

if __name__ == '__main__':
    unittest.main()
