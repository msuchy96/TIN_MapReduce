namespace java MapReduce.Thrift

exception InvalidState {
    1: string message;
}
struct KeyValueEntity {
    1: required string key;
    2: required string value;
}
service MapReduceWorker {
	bool AssignWork(1:string dataFileName, 2:string mapFileName, 3:string reduceFileName, 4:list<i32> workersList),
    bool StartMap() throws(1:InvalidState invalidState),
	bool StartReduce() throws(1:InvalidState invalidState),
	i32 Ping(),
    void RegisterMapPair(1:list<KeyValueEntity> pairs) throws(1:InvalidState invalidState)
}