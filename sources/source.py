
class Source:

    def __init__(self, name, mdb_collection):
        self.name = name
        self.mdb_collection = mdb_collection

    def download_data(self, symbols, params=None):
        print('%s - downloading %s' % (self.name, symbols))
        data = []
        for i in range(0, len(symbols), 10):
            data.extend(self._download_data(symbols[i:i+10], params))
        self.mdb_collection.insert_many(data)
        print('%s - done!' % self.name)

    def _download_data(self, symbols, params):
        pass
