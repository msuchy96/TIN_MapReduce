package JavaWorker;

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
    private Map<Integer,List<Pair<String,Integer>>> workersMap;

    public WorkerListManager(){
        this.workersMap = new HashMap<>();
    }

    public Map<Integer,List<Pair<String,Integer>>> getWorkersMap(){
        return workersMap;
    }

    public synchronized void add(Pair<String,Integer> pair){
        Integer workerId = calculateId(pair.left);
        if(workersMap.keySet().contains(workerId)){
            List<Pair<String,Integer>> tmpList = workersMap.get(workerId);
            tmpList.add(pair);
            workersMap.put(workerId,tmpList);
        }else{
            ArrayList<Pair<String,Integer>> newList = new ArrayList<>();
            newList.add(pair);
            workersMap.put(workerId,newList);
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
