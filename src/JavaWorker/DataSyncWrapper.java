package JavaWorker;

import MapReduce.Thrift.AutoGenerated.ClientListeningInfo;
import org.apache.commons.lang3.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by suchy on 31.05.2018.
 */
public class DataSyncWrapper {

    private BlockingQueue<Pair<String,Integer>> registerWorkersQueue;
    private Boolean endOfRegisterWorkersQueue;
    private BlockingQueue<Boolean> finishReduce;
    private Map<String,List<Integer>> myKeyValuesMap;
    private BlockingQueue<Pair<String,String>> resultList;
    private List<ClientListeningInfo> workersConfigurationList;
    private BlockingQueue<Boolean> serverActionFlag;

    public DataSyncWrapper(){
        endOfRegisterWorkersQueue = false;
        registerWorkersQueue = new ArrayBlockingQueue<>(100);
        myKeyValuesMap = new HashMap<>();
        resultList = new ArrayBlockingQueue<>(100);
        finishReduce = new ArrayBlockingQueue<>(1);
        workersConfigurationList = new ArrayList<>();
        serverActionFlag = new ArrayBlockingQueue<>(100);

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

    public BlockingQueue<Pair<String,Integer>> getRegisterWorkersQueue() throws InterruptedException{
        return registerWorkersQueue;
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

    public synchronized void setWorkersConfigurationListList(List<ClientListeningInfo> workersConfigurationList){
        this.workersConfigurationList = workersConfigurationList;
    }

    public synchronized List<ClientListeningInfo> getWorkersConfigurationList(){
        return workersConfigurationList;

    }

    public void endOfAction(Boolean bool) throws InterruptedException{
        serverActionFlag.put(bool);
    }

    public void waitForServer() throws InterruptedException{
        serverActionFlag.take();
    }

}
