package Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Created by suchy on 02.06.2018.
 */
public class ConfigurationManager {

    public static WorkerConfiguration loadConfiguration(WorkerConfiguration workerConfiguration){
        try{
            JSONParser parser = new JSONParser();

            JSONObject a = (JSONObject) parser.parse(new FileReader("G:/ProjektyELKA/TIN/src/Configuration/sources/config.json"));

            workerConfiguration.setMulticastGroupAddress((String)a.get("multicastGroupAddress"));
            workerConfiguration.setMulticastGroupPort((int)(long)a.get("multicastGroupPort"));
            workerConfiguration.setListeningPort((int)(long)a.get("ListeningPort"));
            workerConfiguration.setIp((String)a.get("ip"));
            workerConfiguration.setMasterPort((int)(long)a.get("masterPort"));
            workerConfiguration.setPythonPath((String)a.get("pythonPath"));
            workerConfiguration.setDataStoragePath((String)a.get("dataStoragePath"));
            workerConfiguration.setBufferSize((int)(long)a.get("bufferSize"));

        }catch (IOException e){
            System.out.println("File not found!");
            e.printStackTrace();
        }catch (ParseException e){
            System.out.println("Parse exception occurred found!");
            e.printStackTrace();
        }


        return workerConfiguration;
    }
}
