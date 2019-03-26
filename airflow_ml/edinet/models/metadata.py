class MetaData():

    def __init__(self, count=-1):
        self.count = count

    @classmethod
    def create(cls, body):
        count = body["metadata"]["resultset"]["count"]
        instance = cls(count=count)
        return instance
