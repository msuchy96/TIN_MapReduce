package JavaWorker;

import org.apache.commons.lang3.Pair;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by suchy on 31.05.2018.
 */
public class RegisterWorkersQueueWrapper {

    private BlockingQueue<Pair<String,Integer>> registerWorkersQueue;
    private Integer numberOfWorkers;
    private Boolean isEnd;

    public RegisterWorkersQueueWrapper(){
        isEnd = false;
        numberOfWorkers = 100;
        registerWorkersQueue = new ArrayBlockingQueue<>(numberOfWorkers);
    }

    public RegisterWorkersQueueWrapper(Integer numberOfWorkers){
        isEnd = false;
        this.numberOfWorkers = numberOfWorkers;
        registerWorkersQueue = new ArrayBlockingQueue<>(numberOfWorkers);
    }

    public synchronized void setIsEnd(Boolean isEnd){
        this.isEnd = isEnd;
    }

    public synchronized Boolean getIsEnd(){
        return isEnd;
    }

    public synchronized void put(Pair<String,Integer> pair) throws InterruptedException{
        registerWorkersQueue.put(pair);
    }

    public synchronized Pair<String,Integer> take() throws InterruptedException{
        return registerWorkersQueue.take();
    }
}
