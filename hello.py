import pandas as pd
from world.flat import Earth
from aggregate.batch_aggregate import BatchAggregate

if __name__ == '__main__':
    earth = Earth()
    print('hello ' + earth.getWord())
    agg = BatchAggregate()

    #print(agg.create_aggdict(['batch']))

    print(agg.weight_cols)
    agg.weight_cols = ['one']  # setter called
    print(agg.weight_cols)
    lst_wght = agg.weight_cols    # getter called
    print(agg.weight_cols,lst_wght)
    # agg.weight_cols = [1]  # setter called
    print(agg.create_aggdict())
    # del agg.weight_cols  # deleter called
    # print(agg.weight_cols, lst_wght)

    df = pd.DataFrame({'one':[1,3,5,5,3,3,3,1], 'two':[1,2,2,2,3,1,1,1]})
    agg.df = df
    df2 = agg.weight_catcolumns('two')
    print(df2.dtypes)
    print(df2)
    print(agg.list_subcols())

    top_str = list_topn(weight_num_users, columns)
    sub_cols = list_subcols(columns, top_str)


"""
agg = BatchAggregate()

agg.weight_cols = ['one']  # setter called
otherise( df_col, n, lst_top_n=None)
weight_catcolumns( sort_col, sort_col2=None, linear=True, top_n_max=None)

create_aggdict()

df = uint8_to_int()

df2 = df.groupby(['key1','key2','order_key']).agg(agg_dict).reset_index()


prob_to_cat()

clean_agg_from_colnames()



# if doing manually with user_id and contact_number,
# join and use
common_names = get_common_cols(df1, df2)
df1 = df1[['contact_number', 'spr_user_id'] + common_names]
df2 = df2[['spr_user_id'] + common_names]
df = df1.append(df2)
"""
# TODO add successive layered roll ups of aggregation......?????