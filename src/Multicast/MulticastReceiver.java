package Multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * Created by suchy on 28.05.2018.
 */
public class MulticastReceiver extends Thread {
    protected MulticastSocket socket = null;
    protected byte[] buf = new byte[256];
    private static final String MASTER_WELCOME = "HELLOIAMMASTER";
    private Boolean masterReadiness;



    /*
    TODO 1: multicast group ipv4 address, socket port
    TODO 2: what is in get datagram?

     */

    public Boolean isMasterReady (){ return masterReadiness; };

    public void run() {
        try{
            socket = new MulticastSocket(4446);
            InetAddress group = InetAddress.getByName("230.0.0.0");
            socket.joinGroup(group);
            System.out.println("Joined multicast group");
            // try to receive as long as special statement occurs
            while (true) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String received = new String(
                        packet.getData(), 0, packet.getLength());

                if(MASTER_WELCOME.equals(received)) {
                    System.out.println("Hello Master, nice to meet you!");
                    break;
                }
            }
            socket.leaveGroup(group);
            System.out.println("Left multicast group");
            socket.close();
            masterReadiness = true;
        } catch(IOException e){
            System.out.println("Exception during multicasting occured");
            e.printStackTrace();
            masterReadiness = false;
        }
    }


}
