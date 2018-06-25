package PythonPoCClient;

import JavaWorker.DataSyncWrapper;
import org.apache.commons.lang3.Pair;

import java.io.*;
import java.util.Arrays;

/**
 * Created by msuchock on 29.05.2018.
 */
public class PythonRunner {
    private String dataFilePath;
    private String pythonMapPath;
    private String pythonReducePath;
    private String pythonPath;

    public PythonRunner(String dataFile, String mapFile, String reduceFile, String sourceFilePath, String pythonExePath){
        dataFilePath = sourceFilePath + dataFile;
        pythonMapPath = sourceFilePath + mapFile;
        pythonReducePath = sourceFilePath + reduceFile;
        pythonPath = pythonExePath;
    }

    public void map(DataSyncWrapper dataSyncWrapper) throws IOException, InterruptedException{
        Process process = createProcess(pythonMapPath);

        OutputStream stdin = process.getOutputStream(); // The Process OuputStream (our point of view) is the STDIN from the process point of view
        InputStream stdout = process.getInputStream();

        BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin));
        BufferedReader br = new BufferedReader(new FileReader(dataFilePath));

        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
            writer.write(line);
            writer.newLine();
        }

        writer.close();
        process.waitFor();

        while(reader.ready()) {
            String resultPair[] = reader.readLine().split("=>");
            System.out.println("Pair result from map: " + resultPair[0]+"=>"+resultPair[1]);
            // to avoid sync problem with end of the
            synchronized (dataSyncWrapper){
                dataSyncWrapper.putInPairsAfterMapQueue(new Pair<>(resultPair[0],Integer.valueOf(resultPair[1])));
                if(!reader.ready()){
                    dataSyncWrapper.setEndPairsAfterMapQueue(true);
                }
                dataSyncWrapper.endOfServerAction(true);
            }
        }
    }

    public void reduce(DataSyncWrapper dataSyncWrapper) throws IOException, InterruptedException{

        for(String key: dataSyncWrapper.getMyKeyValuesMap().keySet()){
            Process process = createProcess(pythonReducePath);

            OutputStream stdin = process.getOutputStream();
            InputStream stdout = process.getInputStream();

            BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin));

            for(Integer value: dataSyncWrapper.getMyKeyValuesMap().get(key)){
                writer.write(String.valueOf(value));
                writer.newLine();
            }
            writer.close();
            process.waitFor();

            while(reader.ready()){
                String result = reader.readLine();
                dataSyncWrapper.addToResultList(key,result);
                System.out.println("Pair result from reduce: " + key + "=>" + result);
            }
        }
        dataSyncWrapper.endOfServerAction(true);
    }

    private Process createProcess(String pythonFunctionPath) throws IOException{
        ProcessBuilder pb = new ProcessBuilder(Arrays.asList(pythonPath, pythonFunctionPath));
        pb.redirectErrorStream(true);
        pb.redirectInput();
        pb.redirectOutput();
        return  pb.start();
    }
}

