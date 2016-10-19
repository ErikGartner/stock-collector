
class Source:

    def __init__(self, name, mdb_collection):
        self.name = name
        self.mdb_collection = mdb_collection

    def download_data(self, symbols, params=None):
        print('%s - downloading %s' % (self.name, symbols))
        data = []
        for i in range(0, len(symbols), 20):
            data.extend(self._download_data(symbols[i:i+20], params))
        self.mdb_collection.insert_many(data)
        print('%s - done!' % self.name)

    def _download_data(self, symbols, params):
        pass
