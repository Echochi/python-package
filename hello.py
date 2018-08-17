from world.flat import Earth
from aggregate.batch_aggdict import BatchAgg

if __name__ == '__main__':
    earth = Earth()
    print('hello ' + earth.getWord())
    agg = BatchAgg()
    print(agg.create_aggdict('batch'))