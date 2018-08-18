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
