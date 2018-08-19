from typing import List

import pandas as pd

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
        self._weight_cols = []          # list of original columns to be weight summed
        self._sum_cols=[]
        self._mode_cols=[]
        self._mean_cols=[]
        self._median_cols=[]
        self._count_cols=[]
        self._max_cols=[]
        self._min_cols=[]
        self._weight_var_cols = []      # new list of weighted probability columns
        self._lst_wv_cols = []          # list of all weighted column values
        self._lst_lst_vars = []         # list of lists of each weighted column's values
        self._df = pd.DataFrame()
        pass


    def create_aggdict(self):
        """Input a list of columns names, naming the list according to the type of aggregation to be done and
        output dictionary for aggregation and a list of the new column names

        Returns:
            dictionary -- dictionary that can be used with with .agg
            list -- new column names of the resultant aggregated dataframe
        """
        agg_list = [self._weight_var_cols,
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



    def weight_catcolumns(self, sort_col, sort_col2=None, linear=True, top_n_max=None):
        """Creat columkns of weights for each varaible in weighted columns

        Arguments:
            sort_col {string} -- column name of primary key column which implies order or recency,
                                 such as user_id which is unique, and which also has the order of signup implied.
                                 Date can also be used.
            sort_col2 {string} -- optional column name of secondary key column eg. order_id

        Keyword Arguments:
            linear {bool} -- Weight can be linear or exponential (default: {True})
            top_n {bool} -- Weight can be linear or exponential (default: {True})

        Returns:
            dataframe -- new dataframe containing weighted columns
        """

        self._lst_lst_vars = self.list_topn(top_n_max)

        if sort_col2:
            sort_by = [sort_col, sort_col2]
        else:
            sort_by = [sort_col]

        self._df = self._df.sort_values(by=sort_by).reset_index(drop=True)
        self._df['increment_key'] = self._df.index
        self._df['rank'] = self._df.groupby(sort_col)['increment_key'].rank(method='min')

        if linear:
            self._df = self.linear_rank(sort_col)
        else:
            self._df = self.exponential_rank()

        del self._df['rank']
        del self._df['increment_key']

        return self._df


    def list_topn(self, top_n_max=None):
        """Creates a list of lists, each list containing the top variables of the weighted columns
        Arguments:
            top_n_max {int} -- integer indicating max numbr of unique elements allowed in the column
        Returns:
            List[List] -- List of list of column variables
        """
        top_str = []
        for col in self._weight_cols:
            top_n = list(self._df[col].dropna().unique())
            if top_n_max:
                if len(top_n) > top_n_max:
                    self._df[col] = self.otherise(self._df[col], top_n_max)
                    top_n = list(self._df[col].dropna().unique())
                    if "Other" in top_n:
                        top_n.remove("Other")
            top_str.append(top_n)

        return top_str


    def prob_to_cat(self):
        """Converts columns of probability into a single categorical column

        Returns:
            dataframe -- dataframe where weighted probability columns have been condensed into a categorical one
        """
        top_col_list = []
        for index, category in enumerate(self._weight_cols):
            if self._weight_var_cols in self._df.columns[index]:
                top_col_list.append(self._df.columns[index])
        drop_columns = []
        for index, category in enumerate(self._weight_cols):
            df2 = self._df[top_col_list[index]]
            drop_columns += top_col_list[index]
            for i, col_name in enumerate(top_col_list[index]):
                self._df[str(category)] = df2.idxmax(axis=1)
            self._df[str(category)] = self._df[str(category)].apply(lambda x: x.split(str(category) + '_', 1)[-1])
        self._df = self._df.drop(drop_columns, axis=1)

        return self._df


    def otherise(self, df_col, n, lst_top_n=None):
        """Otherise dataframe columns if it contains too many values

        Arguments:
            df_col {series} -- dataframe column
            n {int} -- number of maximum values

        Returns:
            series -- dataframe column with reduced values
        """
        if lst_top_n:
            top_n = lst_top_n
        else:
            z = pd.DataFrame(df_col).copy()
            z['val'] = 1
            z.columns = ['key','val']
            top_n = z.groupby('key').size().sort_values(ascending = False)[:n]
            top_n = pd.DataFrame(top_n).reset_index()
            top_n = list(top_n.key.values)
        df_col = df_col.apply(lambda y: y if y in top_n else "Other")
        df_col = df_col.fillna("Other")

        return df_col


    def linear_rank(self, sort_col):
        """Weight columns linearly according to a group."""
        self._df['rank_sum'] = self._df.groupby(sort_col)['rank'].transform(lambda x: x.sum())
        for i, col in enumerate(self._weight_cols):
            top_n = self._lst_lst_vars[i]
            for n in top_n:
                self._df[col + '_' + str(n)] = self._df.transform(lambda x: x['rank']/x['rank_sum'] if (x[col] == n) else 0, axis=1)
        del self._df['rank_sum']

        return self._df


    def exponential_rank(self):
        """Weight columns exponentially."""
        for i, col in enumerate(self._weight_cols):
            top_n = self._lst_lst_vars[i]
            for n in top_n:
                self._df[col + '_' + str(n)] = self._df.transform(lambda x: 1/x['rank'] if ((x[col] == n) & (x['rank'] != 0)) else 0, axis=1)

        return self._df


    def list_subcols(self):
        """return new list of weighted column names."""
        lst_subcols = []
        for i, col in enumerate(self._weight_cols):
            sub_cols = []
            top_n = self._lst_lst_vars[i]
            for n in top_n:
                val =  str(n)
                sub_cols.append(str(col) + '_' + val)
            lst_subcols.append(sub_cols)
            self._weight_var_cols = lst_subcols

        return self._weight_var_cols


    def clean_agg_from_colnames(self):
        """Clean columns names from appended aggregation strings. This function assumed there are no duplicates.

        Returns:
            dataframe -- dataframe with clean column names
        """
        new_names = self._df.columns.tolist()
        for app in self._col_app:
            lst_cluster_cols = self._df.columns.tolist()
            for i, col in enumerate(lst_cluster_cols):
                col = str(col).replace(str(app), '')
                new_names[i] = col
            self._df.columns = new_names

        return self._df


    def uint8_to_int(self):
        """Stadardizes datatypes by converting uint8 to int

        Returns:
            dataframe -- dataframe containing no uint8 columns
        """
        y =self._df.copy()
        column_names = []
        for i in range(0,len(y.columns)):
            if y.dtypes[i] == 'uint8':
                #print(i,y.dtypes[i])
                col_name = str(y.columns[i])
                column_names.append(col_name)
                y[col_name + '_int'] = y[col_name].astype(int)
        y = y.drop(column_names, axis=1)

        return y


    @property
    def df(self):
        """Dataframe on which aggregations are performed."""
        print("getter of df called")
        return self._df

    @df.setter
    def df(self, value: pd.DataFrame()):
        print("setter of df called")
        self._df = value

    @df.deleter
    def df(self):
        print("deleter of df called")
        self._df = pd.DataFrame()


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
