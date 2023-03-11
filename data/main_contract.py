from data.database import *
from data.workers import *
from queue import Queue, Empty

class MainGenerator:

    def __init__(self, name, start_date, end_date):

        self.database = MongodbDatabase("raw_data")

        self.name = name

        self.start_date = start_date

        self.end_date = end_date

        self.register()

        self.container = self.set_container()

        self.data_queue = self.set_queue()

    def register(self):

        self.time_cleaner = TimerCleaner()

        self.field_cleaner = FieldCleaner()

        self.weighter = Weighter()


    def run(self):

        while not self.data_queue.empty():
            self.process_data(self.data_queue.get())

    def process_data(self,collection):

        #query = self.set_query()

        query = {"symbol": "RB2105"}

        df = self.time_cleaner.run(self.load_data(collection_name=collection, query=query))

        df = self.field_cleaner.run(df)

        df = self.weighter.run(df)

        self.save_data(df)

        return

    def load_data(self, collection_name, query=None):

        if query is None:

            query = {}

        df = self.database.load_tick_data(collection_name=collection_name, query=query)

        return df

    def save_data(self, df):

        return

    def set_queue(self):

        collection_queue = Queue()

        # 从数据库中获取所有表格的名字
        collections = sorted(self.database.get_collections())

        # 将起始到结束时间对应的collection名字加入到queue队列中
        for collection_name in collections:
            if self.start_date <= collection_name <= self.end_date:
                collection_queue.put(collection_name)

        return collection_queue

    def set_query(self):

        query = {}

        return query

    def set_container(self):

        container = {'last_day': {}, 'last_tick': {}}

        return container


class MainManager:

    def __init__(self):

        pass

if __name__ == '__main__':
    generator = MainGenerator('RB', '2020-12-30', '2021-01-07')
    generator.run()