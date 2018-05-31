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
    private Boolean end;

    public RegisterWorkersQueueWrapper(){
        end = false;
        numberOfWorkers = 100;
        registerWorkersQueue = new ArrayBlockingQueue<>(numberOfWorkers);
    }

    public RegisterWorkersQueueWrapper(Integer numberOfWorkers){
        end = false;
        this.numberOfWorkers = numberOfWorkers;
        registerWorkersQueue = new ArrayBlockingQueue<>(numberOfWorkers);
    }

    public synchronized void setEnd(Boolean end){
        this.end = end;
    }

    public synchronized Boolean IsEnd(){
        return end;
    }

    public synchronized void put(Pair<String,Integer> pair) throws InterruptedException{
        registerWorkersQueue.put(pair);
    }

    public synchronized Pair<String,Integer> take() throws InterruptedException{
        return registerWorkersQueue.take();
    }
}
