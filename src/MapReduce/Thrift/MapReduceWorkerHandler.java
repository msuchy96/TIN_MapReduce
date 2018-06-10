package MapReduce.Thrift;
import Configuration.WorkerConfiguration;
import JavaWorker.DataSyncWrapper;
import MapReduce.Thrift.AutoGenerated.ClientListeningInfo;
import MapReduce.Thrift.AutoGenerated.KeyValueEntity;
import MapReduce.Thrift.AutoGenerated.MapReduceWorker;
import PythonPoCClient.PythonRunner;
import org.apache.commons.lang3.Pair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Random;


/**
 * Created by msuchock on 27.05.2018.
 */

public class MapReduceWorkerHandler implements MapReduceWorker.Iface {
    private PythonRunner pyRunner;
    private DataSyncWrapper dataSyncWrapper;
    private WorkerConfiguration workerConfiguration;

    public MapReduceWorkerHandler(DataSyncWrapper dataSyncWrapper, WorkerConfiguration workerConfiguration){
        this.dataSyncWrapper = dataSyncWrapper;
        this.workerConfiguration = workerConfiguration;
    }

    public boolean AssignWork(String dataFileName, String mapFileName, String reduceFileName, List<ClientListeningInfo> workersList) {

        dataSyncWrapper.setWorkersConfigurationListList(workersList);
        pyRunner = new PythonRunner(dataFileName, mapFileName, reduceFileName, workerConfiguration.getDataStoragePath(), workerConfiguration.getPythonPath());

        System.out.println("SERVER: Get data: " + dataFileName + " map: " + mapFileName + " reduce: " + reduceFileName);
        System.out.println("SERVER: Workers:");
        workersList.forEach(x -> System.out.println(x.toString()));
        try{
            System.out.println("SERVER: Work assigned!");
            dataSyncWrapper.endOfServerAction(true);
        } catch(InterruptedException e){
            System.out.println("SERVER: InterruptedException occurred while syncing action between server and client");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean StartMap() {
        System.out.println("SERVER: Map begins");

        try {
            pyRunner.map(dataSyncWrapper);
            return true;
        } catch(FileNotFoundException e){
            System.out.println("SERVER: File was not found in PythonRunner");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("SERVER: IO Exception occurred in PythonRunner");
            e.printStackTrace();
        } catch(InterruptedException e){
            System.out.println("SERVER: Interrupt Exception occurred in PythonRunner");
            e.printStackTrace();
        }
        return false;
    }

    public boolean StartReduce() {
        System.out.println("SERVER: Reduce begins");
        try{
            pyRunner.reduce(dataSyncWrapper);
            dataSyncWrapper.endOfServerAction(true);

            return true;
        } catch(IOException e){
            System.out.println("SERVER: IO Exception occurred in PythonRunner");
            e.printStackTrace();
        } catch(InterruptedException e){
            System.out.println("SERVER: Interrupt Exception occurred in PythonRunner");
            e.printStackTrace();
        }
        return false;
    }

    public int Ping() { return new Random().nextInt();}

    public void RegisterMapPair(List<KeyValueEntity> pairs) {
        System.out.println("SERVER: RegisterMapPair begins");
        pairs.forEach(x -> System.out.println("SERVER: I got pair: " +x ));

        for(KeyValueEntity kVE: pairs){
            //adding as many pairs as quantity in pair
            for(int i=0; i<kVE.getQuantity(); i++){
                dataSyncWrapper.addToMyKeyValuesMap(new Pair<>(kVE.getKey(),Integer.valueOf(kVE.getValue())));
            }
        }
    }
}



