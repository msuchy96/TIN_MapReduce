package MapReduce.Thrift.JavaWorker;

import MapReduce.Thrift.MapReduceMaster;
import MapReduce.Thrift.MapReduceWorker;
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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by suchy on 28.05.2018.
 */
public class JavaWorker {
    public int port = 9090 + new Random().nextInt() % 200;

    public void start(){
        // creates fixed thread pool
        final ExecutorService es = Executors.newFixedThreadPool(2);
        // server callable thread starts to execute
        final Future<Boolean> f1 = es.submit(new ServerCallClass());
        // client callable thread starts to execute
        final Future<Boolean> f2 = es.submit(new ClientCallClass());

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

    private class ServerCallClass implements Callable<Boolean>
    {
        public Boolean call(){
            try{

                MapReduceWorkerHandler handler = new MapReduceWorkerHandler();
                MapReduceWorker.Processor processor = new MapReduceWorker.Processor(handler);

                TServerTransport serverTransport = new TServerSocket(new ServerSocket(port,50 , InetAddress.getByName("192.168.0.123")));
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

    private class ClientCallClass implements Callable<Boolean>
    {
        public Boolean call(){
            try{

                //TODO: Tcp client?
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
                    client.RegisterWorker(ByteBuffer.wrap(IPAddressUtil.textToNumericFormatV4("192.168.0.123")).getInt(),port);
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
            }
            return true;
        }
    }
}