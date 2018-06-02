package Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/**
 * Created by suchy on 02.06.2018.
 */
public class ConfigurationManager {

    public static WorkerConfiguration loadConfiguration(WorkerConfiguration workerConfiguration){
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            workerConfiguration = objectMapper.readValue(new File("G:/ProjektyELKA/TIN/src/Configuration/sources/config.json"), WorkerConfiguration.class);

        }catch (IOException e){
            System.out.println("File not found!");
            e.printStackTrace();
        }

        return workerConfiguration;
    }
}
