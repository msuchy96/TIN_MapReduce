package Configuration;


/**
 * Created by suchy on 02.06.2018.
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

    public String getMulticastGroupAddress() {
        return multicastGroupAddress;
    }

    public void setMulticastGroupAddress(String multicastGroupAddress) {
        this.multicastGroupAddress = multicastGroupAddress;
    }

    public Integer getMulticastGroupPort() {
        return multicastGroupPort;
    }

    public void setMulticastGroupPort(Integer multicastGroupPort) {
        this.multicastGroupPort = multicastGroupPort;
    }

    public Integer getListeningPort() {
        return listeningPort;
    }

    public void setListeningPort(Integer listeningPort) {
        this.listeningPort = listeningPort;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(Integer masterPort) {
        this.masterPort = masterPort;
    }

    public String getPythonPath() {
        return pythonPath;
    }

    public void setPythonPath(String pythonPath){
        this.pythonPath = pythonPath;
    }

    public String getDataStoragePath(){
        return dataStoragePath;
    }

    public void setDataStoragePath(String dataStoragePath){
        this.dataStoragePath = dataStoragePath;
    }

    public Integer getBufferSize(){
        return bufferSize;
    }

    public void setBufferSize(Integer bufferSize){
        this.bufferSize = bufferSize;
    }

    public String getMasterIp(){
        return masterIp;
    }

    public void setMasterIp(String masterIp){
        this.masterIp = masterIp;
    }
}
