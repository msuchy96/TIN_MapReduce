package JavaWorker;

import Test.TestUtils;
import org.apache.commons.lang3.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by suchy on 31.05.2018.
 */


public class WorkerListManager {
    private Map<Integer,List<Pair<String,Integer>>> workersMap;

    public WorkerListManager(){
        this.workersMap = new HashMap<>();
    }

    public synchronized void add(Pair<String,Integer> pair){

        Integer workerId = calculateId(pair.right);

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

    private Integer calculateId(Integer value){
        return value % TestUtils.NUMBER_OF_WORKERS;
    }


}
