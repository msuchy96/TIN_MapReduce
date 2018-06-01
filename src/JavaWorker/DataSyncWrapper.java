package JavaWorker;

import org.apache.commons.lang3.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by suchy on 31.05.2018.
 */
public class DataSyncWrapper {

    private BlockingQueue<Pair<String,Integer>> registerWorkersQueue;
    private Integer numberOfWorkers;
    private Boolean endOfRegisterWorkersQueue;
    private BlockingQueue<Boolean> startReduce;
    private BlockingQueue<Boolean> finishReduce;
    private Map<String,List<Integer>> myKeyValuesMap;
    private BlockingQueue<Pair<String,String>> resultList;

    public DataSyncWrapper(){
        endOfRegisterWorkersQueue = false;
        numberOfWorkers = 100;
        registerWorkersQueue = new ArrayBlockingQueue<>(numberOfWorkers);
        myKeyValuesMap = new HashMap<>();
    }

    public DataSyncWrapper(Integer numberOfWorkers){
        endOfRegisterWorkersQueue = false;
        this.numberOfWorkers = numberOfWorkers;
        registerWorkersQueue = new ArrayBlockingQueue<>(numberOfWorkers);
        myKeyValuesMap = new HashMap<>();
    }

    public synchronized void setEndOfRegisterWorkersQueue(Boolean end){
        this.endOfRegisterWorkersQueue = end;
    }

    public synchronized Boolean IsEndOfRegisterWorkersQueue(){
        return endOfRegisterWorkersQueue;
    }

    public synchronized Boolean isRegisterWorkersQueueEmpty(){
        return registerWorkersQueue.isEmpty();
    }

    public void putInRegisterWorkersQueue(Pair<String,Integer> pair) throws InterruptedException{
        registerWorkersQueue.put(pair);
    }

    public void reduceFinished(){
        try{
            finishReduce.put(true);
        }catch (InterruptedException e){
            System.out.println("Execption occureed during finishReduce");
            e.printStackTrace();
        }
    }

    public void waitForFinishReduce(){
        try{
            finishReduce.take();
        }catch (InterruptedException e){
            System.out.println("Execption occureed during finishReduce");
            e.printStackTrace();
        }
    }

    public Pair<String,Integer> takeFromRegisterWorkersQueue() throws InterruptedException{
        return registerWorkersQueue.take();
    }

    public synchronized void addToMyKeyValuesMap(Pair<String,Integer> kVE){
        if(myKeyValuesMap.get(kVE.left) != null){
            myKeyValuesMap.get(kVE).add(kVE.right);
        }else{
            List<Integer> newValuesList = new ArrayList<>();
            newValuesList.add(kVE.right);
            myKeyValuesMap.put(kVE.left,newValuesList);
        }
    }

    public synchronized Map<String,List<Integer>> getMyKeyValuesMap(){
        return myKeyValuesMap;
    }

    public synchronized void addToResultList(String key,String result){
        resultList.add(new Pair<>(key,result));
    }

    public synchronized BlockingQueue<Pair<String,String>> getResultList(){
        return resultList;
    }
}
