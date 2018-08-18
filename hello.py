from world.flat import Earth
from aggregate.batch_aggregate import BatchAggregate

if __name__ == '__main__':
    earth = Earth()
    print('hello ' + earth.getWord())
    agg = BatchAggregate()
    #print(agg.create_aggdict(['batch']))


    agg = BatchAggregate()
    print(agg.weight_sum)
    agg.weight_sum = ['batch']  # setter called
    print(agg.weight_sum)
    lst_wght = agg.weight_sum    # getter called
    print(agg.weight_sum,lst_wght)
    agg.weight_sum = [1]  # setter called
    print(agg.create_aggdict())
    del agg.weight_sum  # deleter called
    print(agg.weight_sum, lst_wght)    
