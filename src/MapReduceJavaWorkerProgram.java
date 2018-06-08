import Configuration.ConfigurationManager;
import Configuration.WorkerConfiguration;
import JavaWorker.JavaWorker;
import Multicast.MulticastReceiver;

/**
 * Created by suchy on 28.05.2018.
 */
public class MapReduceJavaWorkerProgram {
    public static void main(String[] args) {

        System.out.println("MapReduceJavaWorkerProgram started");

        WorkerConfiguration workerConfiguration = new WorkerConfiguration();
        ConfigurationManager.loadConfiguration(workerConfiguration);
        System.out.println("Configuration loaded");

        MulticastReceiver multicastReceiver = new MulticastReceiver(workerConfiguration);
        System.out.println("MulticastReceiver created");

        multicastReceiver.run();

        // run worker
        if(multicastReceiver.isMasterReady()){
            JavaWorker worker = new JavaWorker(workerConfiguration);
            worker.start();
        }


    }
}
