import hazelcast

class LoggingRepository:
    def __init__(self, ):
        self.map = dict()
        self.client = hazelcast.HazelcastClient()
        self.map = self.client.get_map("my-distributed-map").blocking()

    def add_message(self, msg):
        self.map.put(msg.uuid, msg.msg)

    def get_logs(self):
        return ", ".join(self.map.values())