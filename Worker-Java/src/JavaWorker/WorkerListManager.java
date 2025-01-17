package JavaWorker;

import MapReduce.Thrift.AutoGenerated.ClientListeningInfo;
import MapReduce.Thrift.AutoGenerated.KeyValueEntity;
import org.apache.commons.lang3.Pair;

import java.util.*;

/**
 * Created by msuchock on 31.05.2018.
 */

public class WorkerListManager {
    private Map<Integer,Map<Pair<String,Integer>,KeyValueEntity>> keyValueEntityMap;
    private Integer numberOfWorkers;
    private List<ClientListeningInfo> workersConfigurationList;

    public WorkerListManager(Integer numberOfWorkers, List<ClientListeningInfo> workersConfigurationList){
        this.keyValueEntityMap = new HashMap<>();
        this.numberOfWorkers = numberOfWorkers;
        this.workersConfigurationList = workersConfigurationList;
    }

    public synchronized void add(Pair<String,Integer> pair){
        Integer workerId = calculateId(pair.left);
        if(keyValueEntityMap.keySet().contains(workerId)){
            if(keyValueEntityMap.get(workerId).keySet().contains(pair)){
                keyValueEntityMap.get(workerId).get(pair).setQuantity(keyValueEntityMap.get(workerId).get(pair).getQuantity()+1);
            }else{
                keyValueEntityMap.get(workerId).put(pair,new KeyValueEntity(pair.left,String.valueOf(pair.right),1));
            }
        }else{
            HashMap<Pair<String,Integer>,KeyValueEntity> tmpHashMap = new HashMap<>();
            tmpHashMap.put(pair,new KeyValueEntity(pair.left,String.valueOf(pair.right),1));
            keyValueEntityMap.put(workerId,tmpHashMap);
        }
    }

    public Map<Integer,Map<Pair<String,Integer>,KeyValueEntity>> getKeyValueEntityMap(){
        return keyValueEntityMap;
    }

    public List<KeyValueEntity> getKeyValueEntityList(Integer workerId){
        List<KeyValueEntity> keyValueEntityList = new ArrayList<>();
        for(Pair<String,Integer> pair: keyValueEntityMap.get(workerId).keySet()){
            keyValueEntityList.add(keyValueEntityMap.get(workerId).get(pair));
        }
        return keyValueEntityList;
    }

    public ClientListeningInfo getClientListeningInfo(int index){
        return workersConfigurationList.get(index);
    }

    private Integer calculateId(String key){
        return (hashFunction(key) % numberOfWorkers + numberOfWorkers) % numberOfWorkers;
    }

    private Integer hashFunction(String s){
        long hash = 5381;
        for(Character c: s.toCharArray()){
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
        }
        return (int)hash;
    }








}
