package JavaWorker;

import MapReduce.Thrift.AutoGenerated.KeyValueEntity;
import Test.TestUtils;
import org.apache.commons.lang3.Pair;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by suchy on 31.05.2018.
 */


public class WorkerListManager {
    private Map<Integer,Map<Pair<String,Integer>,KeyValueEntity>> keyValueEntityMap;

    public WorkerListManager(){
        this.keyValueEntityMap = new HashMap<>();
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

    public synchronized void add(Pair<String,Integer> pair){
        Integer workerId = calculateId(pair.left);
        if(keyValueEntityMap.keySet().contains(workerId)){
            if(keyValueEntityMap.get(workerId).keySet().contains(pair)){
                keyValueEntityMap.get(workerId).get(pair).setQuantity( keyValueEntityMap.get(workerId).get(pair).getQuantity()+1);
            }else{
                keyValueEntityMap.get(workerId).put(pair,new KeyValueEntity(pair.left,pair.right.toString(),1));
            }
        }else{
            HashMap<Pair<String,Integer>,KeyValueEntity> tmpHashMap = new HashMap<>();
            tmpHashMap.put(pair,new KeyValueEntity(pair.left,pair.right.toString(),1));
            keyValueEntityMap.put(workerId,tmpHashMap);
        }
    }

    private Integer calculateId(String key){
        return hashFunction(key) % TestUtils.NUMBER_OF_WORKERS;
    }

    private Integer hashFunction(String s){
        long result = 0;
        for (int i = 0; i < s.length(); i++)
            result += (long)Math.pow(27, 40 - i - 1)*(1 + s.charAt(i) - 'a');
        return Long.valueOf(result).intValue();
    }







}
