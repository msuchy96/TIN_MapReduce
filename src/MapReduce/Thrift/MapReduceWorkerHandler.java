package MapReduce.Thrift;
import MapReduce.Thrift.ClientListeningInfo;
import MapReduce.Thrift.KeyValueEntity;
import MapReduce.Thrift.MapReduceWorker;
import java.util.List;
import java.util.Random;

/**
 * Created by msuchock on 27.05.2018.
 */

public class MapReduceWorkerHandler implements MapReduceWorker.Iface {

    public boolean AssignWork(String dataFileName, String mapFileName, String reduceFileName, List<ClientListeningInfo> workersList) {
        System.out.println("Get data: " + dataFileName + " map: " + mapFileName + " reduce: " + mapFileName);
        System.out.println("Workers:");
        workersList.forEach(x -> System.out.println(x));
        return true;
    }

    public boolean StartMap() {
        System.out.println("Map begins");
        return true;
    }

    public boolean StartReduce() {
        System.out.println("Reduce begins");
        return true;
    }

    public int Ping()
    {
        return new Random().nextInt();
    }

    public void RegisterMapPair(List<KeyValueEntity> pairs) {
        System.out.println("Otrzymalem pary:");
        pairs.forEach(x -> System.out.println(String.valueOf(x.key) + " -> " + String.valueOf(x.value)));
    }
}



