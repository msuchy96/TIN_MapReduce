namespace java MapReduce.Thrift

exception InvalidState {
    1: string message;
}
struct KeyValueEntity {
    1: required string key;
    2: required string value;
}
struct ClientListeningInfo {
    1: required i32 ip;
    2: required i32 port;
}
service MapReduceWorker {
	bool AssignWork(1:string dataFileName, 2:string mapFileName, 3:string reduceFileName, 4:list<ClientListeningInfo> workersList),
    bool StartMap() throws(1:InvalidState invalidState),
	bool StartReduce() throws(1:InvalidState invalidState),
	i32 Ping(),
    void RegisterMapPair(1:list<KeyValueEntity> pairs) throws(1:InvalidState invalidState)
}