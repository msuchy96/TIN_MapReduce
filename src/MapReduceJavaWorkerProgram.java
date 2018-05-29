import JavaWorker.JavaWorker;
import Multicast.MulticastReceiver;

/**
 * Created by suchy on 28.05.2018.
 */
public class MapReduceJavaWorkerProgram {
    public static void main(String[] args) {

        System.out.println("MapReduceJavaWorkerProgram started");

        MulticastReceiver multicastReceiver = new MulticastReceiver();
        System.out.println("MulticastReceiver created");

        multicastReceiver.run();

        // run worker
        if(multicastReceiver.isMasterReady()){
            JavaWorker worker = new JavaWorker();
            worker.start();
        }


    }
}
