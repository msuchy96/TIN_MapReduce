package JavaWorker;

import org.apache.commons.lang3.Pair;

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

    public DataSyncWrapper(){
        endOfRegisterWorkersQueue = false;
        numberOfWorkers = 100;
        registerWorkersQueue = new ArrayBlockingQueue<>(numberOfWorkers);
    }

    public DataSyncWrapper(Integer numberOfWorkers){
        endOfRegisterWorkersQueue = false;
        this.numberOfWorkers = numberOfWorkers;
        registerWorkersQueue = new ArrayBlockingQueue<>(numberOfWorkers);
    }

    public synchronized void setEndOfRegisterWorkersQueue(Boolean end){
        this.endOfRegisterWorkersQueue = end;
    }

    public synchronized Boolean IsEndOfRegisterWorkersQueue(){
        return endOfRegisterWorkersQueue;
    }

    public synchronized void putInRegisterWorkersQueue(Pair<String,Integer> pair) throws InterruptedException{
        registerWorkersQueue.put(pair);
    }

    public synchronized void startReduce(){
        try{
            startReduce.put(true);
        }catch (InterruptedException e){
            System.out.println("Execption occureed during startReduce");
            e.printStackTrace();
        }

    }

    public synchronized void waitForStartReduce(){
        try{
            startReduce.take();
        }catch (InterruptedException e){
            System.out.println("Execption occureed during startReduce");
            e.printStackTrace();
        }
    }

    public synchronized Pair<String,Integer> takeFromRegisterWorkersQueue() throws InterruptedException{
        return registerWorkersQueue.take();
    }
}
