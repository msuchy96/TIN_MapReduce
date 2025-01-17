package Multicast;

import Configuration.WorkerConfiguration;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;

/**
 * Created by msuchock on 28.05.2018.
 */
public class MulticastReceiver extends Thread {
    private byte[] buf = new byte[256];
    private static final String MASTER_WELCOME = "HELLOWORKERS";
    private Boolean masterReadiness;
    private WorkerConfiguration workerConfiguration;

    public MulticastReceiver(WorkerConfiguration workerConfiguration){
        this.workerConfiguration = workerConfiguration;
    }

    public Boolean isMasterReady (){ return masterReadiness; };

    public void run() {
        try{
            MulticastSocket socket = new MulticastSocket(workerConfiguration.getMulticastGroupPort());
            InetAddress group = InetAddress.getByName(workerConfiguration.getMulticastGroupAddress());
            socket.joinGroup(group);
            System.out.println("Joined multicast group");
            // try to receive as long as special statement occurs
            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                System.out.println("Waiting for Master...");
                socket.receive(packet);
                workerConfiguration.setMasterIp(packet.getAddress().getHostAddress());
                String received = new String(
                        packet.getData(), 0, packet.getLength());
                if(received.contains(MASTER_WELCOME)) {
                    System.out.println("Hello Master, nice to meet you!");
                    break;
                }
            }
            socket.leaveGroup(group);
            System.out.println("Left multicast group");
            socket.close();
            masterReadiness = true;
        } catch(IOException e){
            System.out.println("Exception during multicast occurred");
            e.printStackTrace();
            masterReadiness = false;
        }
    }


}
