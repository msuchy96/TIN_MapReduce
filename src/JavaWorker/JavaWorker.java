package JavaWorker;

import MapReduce.Thrift.AutoGenerated.KeyValueEntity;
import MapReduce.Thrift.AutoGenerated.MapReduceMaster;
import MapReduce.Thrift.AutoGenerated.MapReduceWorker;
import MapReduce.Thrift.MapReduceWorkerHandler;
import PythonPoCClient.PythonRunner;
import Test.TestUtils;
import org.apache.commons.lang3.Pair;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import sun.net.util.IPAddressUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by suchy on 28.05.2018.
 */

public class JavaWorker {
    private int port = 9090 + new Random().nextInt() % 200;
    private static final String IPV4 = "192.168.0.123";

    public void start(){
        // creates fixed thread pool
        final ExecutorService es = Executors.newFixedThreadPool(2);

        RegisterWorkersQueueWrapper registerWorkersQueueWrapper = new RegisterWorkersQueueWrapper(TestUtils.NUMBER_OF_WORKERS);

        // server callable thread starts to execute
        final Future<Boolean> f1 = es.submit(new ServerCallClass(registerWorkersQueueWrapper));
        // client callable thread starts to execute
        final Future<Boolean> f2 = es.submit(new ClientCallClass(registerWorkersQueueWrapper));



        try{
            while ( !f1.isDone() && !f2.isDone()){
                Thread.sleep(20);
                // gets value of callable thread if done
                System.out.println("All done!");
                System.out.println("Server ended with success?: " + String.valueOf(f1.get()));
                System.out.println("Client ended with success?: " + String.valueOf(f2.get()));
            }
        } catch (Exception ex){
            System.out.println("Something went wrong with Calls statuses");
        }
    }

    protected class ServerCallClass implements Callable<Boolean>
    {
        RegisterWorkersQueueWrapper registerWorkersQueueWrapper;

        protected ServerCallClass(RegisterWorkersQueueWrapper registerWorkersQueueWrapper){
            this.registerWorkersQueueWrapper = registerWorkersQueueWrapper;
        }

        public Boolean call(){
            try{

                MapReduceWorkerHandler handler = new MapReduceWorkerHandler(registerWorkersQueueWrapper);
                MapReduceWorker.Processor processor = new MapReduceWorker.Processor(handler);

                //MY IP
                TServerTransport serverTransport = new TServerSocket(new ServerSocket(port,50 , InetAddress.getByName(IPV4)));
                TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));


                System.out.println("Starting the worker server...");
                server.serve(); // infinite loop inside

                System.out.println("Done.");
                return true;
            } catch (UnknownHostException e){
                System.out.println("UnknownHost Exception occurred!");
                e.printStackTrace();
            } catch (TTransportException e) {
                System.out.println("Thrift Transport Exception occurred!");
                e.printStackTrace();
            } finally {
                return false;
            }
        }
    }

    protected class ClientCallClass implements Callable<Boolean>
    {
        RegisterWorkersQueueWrapper registerWorkersQueueWrapper;

        protected ClientCallClass(RegisterWorkersQueueWrapper registerWorkersQueueWrapper){
            this.registerWorkersQueueWrapper = registerWorkersQueueWrapper;
        }

        public Boolean call(){
            try{

                //TODO: Tcp client?
                //TODO: Ustawianie hosta z pliku konfiguracyjnego
                TTransport transport = new TSocket("localhost", port);
                TProtocol protocol = new TBinaryProtocol(transport);
                MapReduceMaster.Client client = new MapReduceMaster.Client(protocol);

                try{
                    System.out.println("Trying to open connection...");
                    transport.open();
                } catch (TException x) {
                    System.out.println("Exception occurred: Unable to open connection!");
                    x.printStackTrace();
                    return false;
                }
                System.out.println("Client worker on...");

                try{
                    System.out.println("Register...");
                    System.in.read();
                    client.RegisterWorker(ByteBuffer.wrap(IPAddressUtil.textToNumericFormatV4(IPV4)).getInt(),port);
                    System.out.println("Register finished");


                    WorkerListManager workerListManager = new WorkerListManager();
                    //take from workersQueue till the end
                    while(!registerWorkersQueueWrapper.IsEnd()){
                        workerListManager.add(registerWorkersQueueWrapper.take());
                    }

                    sendToWorkers(workerListManager.getWorkersMap());

                    System.out.println("Finish Map");
                    System.in.read();
                    client.FinishedMap();
                    System.out.println("Finish Reduce");

                    System.in.read();
                    client.FinishedReduce();
                    System.out.println("Send Results");
                    System.in.read();
                    client.RegisterResult("key", "value");
                } finally {
                    transport.close();

                }
            } catch (IOException e){
                System.out.println("Input Exception occurred!");
                e.printStackTrace();
                return false;
            } catch (TException e){
                System.out.println("Thrift Exception occurred!");
                e.printStackTrace();
                return false;
            } catch(InterruptedException e) {
                System.out.println("Execption occureed during waiting for map results");
                e.printStackTrace();
                return false;
            }
            return true;
        }

        private void sendToWorkers(Map<Integer,List<Pair<String,Integer>>> workersMap){
            ExecutorService executor= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            try{

                for (Integer workerId: workersMap.keySet()){
                    executor.execute(new NewThread(workerId, workersMap.get(workerId)));
                }
            }catch(Exception err){
                err.printStackTrace();
            }
            executor.shutdown(); // once you are done with ExecutorService
        }

        // TODO: ustawienie hosta z pliku konfiguracyjnego na podstawie workerId
        private class NewThread implements Runnable{
            int workerId;
            List<Pair<String,Integer>> pairList;

            public NewThread(Integer workerId, List<Pair<String,Integer>> pairList){
                this.workerId = workerId;
                this.pairList = pairList;
            }
            public void run(){
                try{
                    TTransport transport = new TSocket("localhost", port);
                    TProtocol protocol = new TBinaryProtocol(transport);
                    MapReduceWorker.Client client = new MapReduceWorker.Client(protocol);

                    List<KeyValueEntity> keyValueEntityList = new ArrayList<>();

                    //TODO: debug this logic on test
                    //TODO: add ilosc to thrift and logic to it
                    int actualIndex = 0;
                    while(actualIndex < pairList.size()){
                        int inBuff = 0;
                        for(int i = actualIndex; i<pairList.size();i++){
                            if(inBuff == TestUtils.BUFF_SIZE){
                                actualIndex = i;
                                break;
                            }
                            keyValueEntityList.add(new KeyValueEntity(pairList.get(i).left,pairList.get(i).right.toString()));
                            inBuff++;
                        }
                        client.RegisterMapPair(keyValueEntityList);
                    }




                }catch(Exception err){
                    System.out.println("Exception occurred during sending to workers");
                    err.printStackTrace();
                }
            }
        }
    }


}