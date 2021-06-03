package Configuration;


import org.apache.commons.lang3.ArrayUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * Created by msuchock on 02.06.2018.
 */
public class WorkerConfiguration {

    private String multicastGroupAddress;
    private Integer multicastGroupPort;
    private Integer listeningPort;
    private String ip;
    private Integer masterPort;
    private String pythonPath;
    private String dataStoragePath;
    private Integer bufferSize;
    private String masterIp;
    private Integer ipInt;

    public WorkerConfiguration(String src){
        loadConfiguration(src);
    }

    public String getMulticastGroupAddress() {
        return multicastGroupAddress;
    }

    private void setMulticastGroupAddress(String multicastGroupAddress) {
        this.multicastGroupAddress = multicastGroupAddress;
    }

    public Integer getMulticastGroupPort() {
        return multicastGroupPort;
    }

    private void setMulticastGroupPort(Integer multicastGroupPort) {
        this.multicastGroupPort = multicastGroupPort;
    }

    public Integer getListeningPort() {
        return listeningPort;
    }

    private void setListeningPort(Integer listeningPort) {
        this.listeningPort = listeningPort;
    }

    public String getIp() {
        return ip;
    }

    private void setIp(String ip) throws UnknownHostException {
        this.ip = ip;
        ipToInt(InetAddress.getByName(ip));
    }

    public Integer getIpInt() {
        return ipInt;
    }

    private void ipToInt(InetAddress ipAddr) {
        byte[] bytes = ipAddr.getAddress();
        ArrayUtils.reverse(bytes);
        ipInt = ByteBuffer.wrap(bytes).getInt();
    }

    public Integer getMasterPort() {
        return masterPort;
    }

    private void setMasterPort(Integer masterPort) {
        this.masterPort = masterPort;
    }

    public String getPythonPath() {
        return pythonPath;
    }

    private void setPythonPath(String pythonPath) {
        this.pythonPath = pythonPath;
    }

    public String getDataStoragePath() {
        return dataStoragePath;
    }

    private void setDataStoragePath(String dataStoragePath) {
        this.dataStoragePath = dataStoragePath;
    }

    public Integer getBufferSize() {
        return bufferSize;
    }

    private void setBufferSize(Integer bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getMasterIp() {
        return masterIp;
    }

    public void setMasterIp(String masterIp) {
        this.masterIp = masterIp;
    }

    public void loadConfiguration(String src) {
        try {
            JSONParser parser = new JSONParser();

            JSONObject a = (JSONObject) parser.parse(new FileReader(src));

            setMulticastGroupAddress((String) a.get("multicastGroupAddress"));
            setMulticastGroupPort((int) (long) a.get("multicastGroupPort"));
            setListeningPort((int) (long) a.get("ListeningPort"));
            setIp((String) a.get("ip"));
            setMasterPort((int) (long) a.get("masterPort"));
            setPythonPath((String) a.get("pythonPath"));
            setDataStoragePath((String) a.get("dataStoragePath"));
            setBufferSize((int) (long) a.get("bufferSize"));

        } catch (IOException e) {
            System.out.println("File not found!");
            e.printStackTrace();
        } catch (ParseException e) {
            System.out.println("Parse exception occurred found!");
            e.printStackTrace();
        }

    }
}
