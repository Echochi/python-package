from typing import List

import numpy as np
import pandas as pd

class BatchAggregate:

    def __init__(self):
        self._col_app = ['weight_',
                        'sum_',
                        'mode_',
                        'mean_',
                        'median_',
                        'count_',
                        'max_',
                        'min_']
        self._agg_ops = ['sum',
                        'sum',
                        lambda x: x.value_counts().index[0] if len(x.value_counts()) else None,
                        'mean',
                        'median',
                        'count',
                        'max',
                        'min']
        self._L1_aggcol = ''            # string name of primary column used to aggregate by
        self._backup_L1_aggcol = None   # in case L1_agg_aggcol contains NULL, backup id to aggregate on
        self._backup2_L1_aggcol = None  # in case L1_agg_aggcol contains NULL, backup id to aggregate on
        self._L2_aggcol = None          # string name of secondary column used to aggregate by
        self._orderby_col = None        # list of name of secondary column used to aggregate by
        self._L0_aggcol = []            # list of created agg column of non-NULL unique id's and secondary agg column
        self._weight_cols = []          # list of original columns to find the sum of the weighted probability of top values
        self._sum_cols=[]               # list of original columns to find the sum
        self._mode_cols=[]              # list of original columns to find the mode
        self._mean_cols=[]              # list of original columns to find the mean
        self._median_cols=[]            # list of original columns to find the median
        self._count_cols=[]             # list of original columns to find the count
        self._max_cols=[]               # list of original columns to find the max
        self._min_cols=[]               # list of original columns to find the min
        self._weight_var_cols = []      # new list of weighted probability columns
        self._lst_wv_cols = []          # list of all weighted column values
        self._lst_lst_vars = []         # list of lists of each weighted column's values
        self._lst_batch_cols = []       # new column names after batch aggregations
        self._key_cols = pd.DataFrame() # if backup keys are being used, save to append later
        self._batch_dict = {}           # dictionary of columns and their respective aggregations
        self._df = pd.DataFrame()       # dataframe to be aggregated
        pass

    def run_batch(self, L1_aggcol, backup_L1_aggcol=None, backup2_L1_aggcol=None, L2_aggcol=None, orderby_col=None, linear=True, top_n_max=None, clean_col_names=False, convert_prob=True):
        """Run entire class to perform batch aggregation on dataframe

        Arguments:
            L1_aggcol {string} -- primary key column to group by, eg. contact_number

        Keyword Arguments:
            backup_L1_aggcol {string} -- incase primary contains NULL values, provide secondary key, eg. sup_user_id (default: {None})
            backup2_L1_aggcol {string} -- incase primary and secondary key contains NULL values, provide tertiary key, eg. spr_user_id (default: {None})
            L2_aggcol {string} -- secondary aggregation column to group by, eg. order_id (cannot contain NULL values) (default: {None})
            orderby_col {string} -- column to order by, eg. date (descending) (default: {None})
            linear {bool} -- weight probability is lienar or exponential (default: {True})
            top_n_max {int} -- limit categorical variable number to be weighted to top_n (default: {None})
            clean_col_names {bool} -- remove aggregation prefixes (default: {False})
            convert_prob {bool} -- turn probability columns back into categorical column (default: {True})

        Returns:
            dataframe -- aggregated dataframe
        """
        self._L1_aggcol = L1_aggcol
        if backup_L1_aggcol:
            self._backup_L1_aggcol = backup_L1_aggcol
        if backup2_L1_aggcol:
            self._backup2_L1_aggcol = backup2_L1_aggcol
        if L2_aggcol:
            self._L2_aggcol = L2_aggcol
        if orderby_col:
            self._orderby_col = orderby_col
        self.sort_key_cols()
        self._df = self.weight_catcolumns(linear, top_n_max)
        self._batch_dict, self._lst_batch_cols = self.create_aggdict()
        self._df = self.uint8_to_int()
        self._df = self.groupby_batch_agg()
        if convert_prob:
            self._df = self.prob_to_cat()
        if clean_col_names:
            self._df = self.clean_agg_from_colnames()

        if self._key_cols.empty == False:
            self._df = pd.merge(self._key_cols, self._df, how='inner', on='unique_key')
            del self._df['unique_key']

        return self._df


    def sort_key_cols(self):
        """Sets list of columns to group by, list of columns to order by,
        and in the case of redundant key columns, saves them for later rejoining.
        """
        self._df['L1_aggcol'] = self._df[self._L1_aggcol].astype(str).copy()
        self._df['L1_aggcol'] = str(self._L1_aggcol) + self._df['L1_aggcol']
        if self._backup_L1_aggcol:
            self._df['backup_L1_aggcol'] = self._df[self._backup_L1_aggcol].astype(str).copy()
            self._df['backup_L1_aggcol'] = str(self._backup_L1_aggcol) + self._df['backup_L1_aggcol']
            self._df['unique_key'] = np.where(self._df[self._L1_aggcol].notnull()==True,
                                              self._df['L1_aggcol'],
                                              self._df['backup_L1_aggcol'])
            # repeat with secondary backup
            if self._backup2_L1_aggcol:
                self._df['backup2_L1_aggcol'] = self._df[self._backup2_L1_aggcol].astype(str).copy()
                self._df['backup2_L1_aggcol'] = str(self._backup2_L1_aggcol) + self._df['backup2_L1_aggcol']
                self._df['unique_key'] = np.where(((self._df[self._L1_aggcol].isnull()==True) & (self._df[self._backup_L1_aggcol].isnull()==True)),
                                                  self._df['backup2_L1_aggcol'],
                                                  self._df['unique_key'])
                # save original keys, backup key may be lost in aggregations, to append at end
                self._key_cols = self._df[['unique_key', self._L1_aggcol, self._backup_L1_aggcol, self._backup2_L1_aggcol]]
            else:
                self._key_cols = self._df[['unique_key', self._L1_aggcol, self._backup_L1_aggcol]]
        # set primary key to group by
        self._L0_aggcol = ['unique_key']
        # in case of secondary key to group by, add to column list
        if self._L2_aggcol:
            self._L0_aggcol = self._L0_aggcol + [self._L2_aggcol]
        # if there is no order by, use the unique key
        if self._orderby_col is  None:
            self._orderby_col = 'unique_key'

        if self._key_cols.empty == False:
            self._key_cols[self._orderby_col] = self._df[self._orderby_col]
            self._key_cols = self.rank_keys()

        del self._df['L1_aggcol']
        del self._df['backup_L1_aggcol']
        del self._df['backup2_L1_aggcol']

        pass


    def weight_catcolumns(self, linear=True, top_n_max=None):
        """Creat columkns of weights for each varaible in weighted columns

        Keyword Arguments:
            linear {bool} -- Weight can be linear or exponential (default: {True})
            top_n {bool} -- Weight can be linear or exponential (default: {True})

        Returns:
            dataframe -- new dataframe containing weighted columns
        """
        self._lst_lst_vars = self.list_topn(top_n_max)
        self.list_subcols()
        self._df = self._df.sort_values(by=self._orderby_col).reset_index(drop=True)
        self._df['increment_key'] = self._df.index
        self._df['rank'] = self._df.groupby(self._L0_aggcol)['increment_key'].rank(method='min')

        if linear:
            self._df = self.linear_rank()
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


    def rank_keys(self):
        """
        Deduplicates all keys and saves for appending later
        """
        self._key_cols = self._key_cols.sort_values(by=self._orderby_col).reset_index(drop=True)
        self._key_cols['increment_key'] = self._key_cols.index
        self._key_cols['rank'] = self._key_cols.groupby('unique_key')['increment_key'].rank(method='min')
        self._key_cols = self._key_cols[self._key_cols['rank'] == 1]
        del self._key_cols['rank']
        del self._key_cols['increment_key']
        del self._key_cols[self._orderby_col]

        return self._key_cols


    def list_subcols(self):
        """return new list of weighted column names."""
        lst_subcols = []
        lst_newcols = []
        for i, col in enumerate(self._weight_cols):
            sub_cols = []
            top_n = self._lst_lst_vars[i]
            for n in top_n:
                val =  str(n)
                sub_cols.append(str(col) + '_' + val)
                lst_newcols.append(str(col) + '_' + val)
            lst_subcols.append(sub_cols)
            self._weight_var_cols = lst_newcols
        self._lst_wv_cols = lst_subcols
        pass


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


    def linear_rank(self):
        """Weight columns linearly according to a group."""
        self._df['rank_sum'] = self._df.groupby(self._L0_aggcol)['rank'].transform(lambda x: x.sum())
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
        for index, aggregation in enumerate(agg_list):
            for col_name in aggregation:
                if col_name in self._batch_dict:
                    self._batch_dict[col_name] += [self._agg_ops[index]]
                else:
                    self._batch_dict[col_name] = [self._agg_ops[index]]
                self._lst_batch_cols.append(self._col_app[index] + col_name)

        return self._batch_dict, self._lst_batch_cols


    def groupby_batch_agg(self):
        """
        Call aggregation on df using batch dictionary
        # deprecation warning, for calling directly
        """
        self._df = self._df.groupby(self._L0_aggcol).agg(self._batch_dict).reset_index()
        self._df.columns = self._L0_aggcol + self._lst_batch_cols

        return self._df


    def prob_to_cat(self):
        """Converts columns of probability into a single categorical column

        Returns:
            dataframe -- dataframe where weighted probability columns have been condensed into a categorical one
        """
        top_col_list = []
        for index, category in enumerate(self._weight_cols):
            sub_list = []
            for col_name in self._weight_var_cols:
                for df_col in self._df.columns:
                    if col_name in df_col:
                        sub_list.append(df_col)
            top_col_list.append(sub_list)
        drop_columns = []
        for index, category in enumerate(self._weight_cols):
            df2 = self._df[top_col_list[index]]
            drop_columns += top_col_list[index]
            self._df[str(category)] = df2.idxmax(axis=1)
            self._df[str(category)] = self._df[str(category)].apply(lambda x: x.split(str(category) + '_', 1)[-1])
        self._df = self._df.drop(drop_columns, axis=1)

        return self._df


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


    def get_common_cols(self, df1, df2):
        """returns list of column names that two dataframes have in common
        # deprecation warning, for calling directly
        Arguments:
            df1 {dataframe} -- larger dataframe
            df2 {dataframe} -- smaller dataframe

        Returns:
            list[string] -- list of column names common to both dataframes
        """
        num_names = list(df1)
        nonum_names = list(df2)
        common_names = []

        for name in num_names:
            if name in nonum_names:
                common_names.append(name)
        return common_names


    def uint8_to_int(self):
        """Stadardizes datatypes by converting uint8 to int

        Returns:
            dataframe -- dataframe containing no uint8 columns
        """
        y =self._df.copy()
        column_names = []
        for i in range(0,len(y.columns)):
            if y.dtypes[i] == 'uint8':
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
    def l1_aggcol(self):
        """Dataframe on which aggregations are performed."""
        print("getter of L1_aggcol called")
        return self._L1_aggcol

    @l1_aggcol.setter
    def l1_aggcol(self, value):
        print("setter of L1_aggcol called")
        self._L1_aggcol = value

    @l1_aggcol.deleter
    def l1_aggcol(self):
        print("deleter of L1_aggcol called")
        self._L1_aggcol = None

    @property
    def L1_aggcol(self):
        """List of column names to undergo 'weighted sum' aggregation."""
        print("getter of L1_aggcol called")
        return self._L1_aggcol

    @L1_aggcol.setter
    def L1_aggcol(self, value: List[str]):
        print("setter of L1_aggcol called")
        self._L1_aggcol = value

    @L1_aggcol.deleter
    def L1_aggcol(self):
        print("deleter of L1_aggcol called")
        self._L1_aggcol = []


    @property
    def backup_L1_aggcol(self):
        """Dataframe on which aggregations are performed."""
        print("getter of backup_L1_aggcol called")
        return self._backup_L1_aggcol

    @backup_L1_aggcol.setter
    def backup_L1_aggcol(self, value):
        print("setter of backup_L1_aggcol called")
        self._backup_L1_aggcol = value

    @backup_L1_aggcol.deleter
    def backup_L1_aggcol(self):
        print("deleter of backup_L1_aggcol called")
        self._backup_L1_aggcol = None

    @property
    def backup2_L1_aggcol(self):
        """Dataframe on which aggregations are performed."""
        print("getter of backup2_L1_aggcol called")
        return self._backup2_L1_aggcol

    @backup2_L1_aggcol.setter
    def backup2_L1_aggcol(self, value):
        print("setter of backup2_L1_aggcol called")
        self._backup2_L1_aggcol = value

    @backup2_L1_aggcol.deleter
    def backup2_L1_aggcol(self):
        print("deleter of backup2_L1_aggcol called")
        self._backup2_L1_aggcol = None


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
