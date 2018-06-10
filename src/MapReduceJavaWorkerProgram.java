import Configuration.WorkerConfiguration;
import JavaWorker.JavaWorker;
import Multicast.MulticastReceiver;

/**
 * Created by msuchock on 28.05.2018.
 */
public class MapReduceJavaWorkerProgram {
    public static void main(String[] args) {

        System.out.println("MapReduceJavaWorkerProgram started");

        WorkerConfiguration workerConfiguration = new WorkerConfiguration();
        System.out.println("Configuration loaded");

        MulticastReceiver multicastReceiver = new MulticastReceiver(workerConfiguration);
        System.out.println("MulticastReceiver created");

        multicastReceiver.run();

        // run worker
        if(multicastReceiver.isMasterReady()){
            JavaWorker worker = new JavaWorker(workerConfiguration);
            worker.start();
        }
        System.out.println("Program finished.");
    }
}
