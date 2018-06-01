package PythonPoCClient;

import JavaWorker.DataSyncWrapper;
import org.apache.commons.lang3.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;


/**
 * Created by suchy on 29.05.2018.
 */
public class PythonRunner {
    private static String pythonPath = "C:/Python27/python.exe";
    private static String sourceFilePath = "G:/ProjektyELKA/TIN/src/PythonPoCClient/sources/";


    private static String dataFilePath;
    private static String pythonMapPath;
    private static String pythonReducePath;
    public ArrayList<Pair<String,Integer>> mapResults;

    public ArrayList<Pair<String,Integer>> getMapResults(){
        return mapResults;
    }

    public PythonRunner(String dataFile, String mapFile, String reduceFile){
        dataFilePath = sourceFilePath + dataFile;
        pythonMapPath = sourceFilePath + mapFile;
        pythonReducePath = sourceFilePath + reduceFile;
    }

    public void map(DataSyncWrapper dataSyncWrapper){

        //zakladamy dla uproszczenia, ze:
        //wywolujemy skrypt pythonowy, ktory czyta z wejscia standardowego
        //na wejscie standardowe MY wrzucamy dane z dataFile
        //a wyniki ma wypluwac na stdout w postaci KLUCZ=>WARTOSC, gdzie wartosc musi dac sie sparsowac na inta, klucz dowolny(string)

        try {
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
                // to avoid sync problem with end of the
                synchronized (dataSyncWrapper){
                    dataSyncWrapper.putInRegisterWorkersQueue(new Pair<>(resultPair[0],Integer.valueOf(resultPair[1])));
                    if(!reader.ready()){
                        dataSyncWrapper.setEndOfRegisterWorkersQueue(true);
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("Exception occurred in PythonRunner");
            e.printStackTrace();
        }
    }

    public void reduce(DataSyncWrapper dataSyncWrapper){

        try {
           for(String key: dataSyncWrapper.getMyKeyValuesMap().keySet()){
               Process process = createProcess(pythonReducePath);

               OutputStream stdin = process.getOutputStream();
               InputStream stdout = process.getInputStream();

               BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
               BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin));


               for(Integer value: dataSyncWrapper.getMyKeyValuesMap().get(key)){
                   writer.write(key+"=>"+String.valueOf(value));
                   writer.newLine();
               }
               writer.close();
               process.waitFor();

               while(reader.ready()){
                   String result = reader.readLine();
                   // to avoid sync problem with end of the
                   dataSyncWrapper.addToResultList(key,result);
               }
           }
           dataSyncWrapper.reduceFinished();


        } catch (Exception e) {
            System.out.println("Exception occurred in PythonRunner");
            e.printStackTrace();
        }
    }

    private static Process createProcess(String pythonFunctionPath) throws IOException{
        System.out.println("Start process execution for python");
        ProcessBuilder pb = new ProcessBuilder(Arrays.asList(pythonPath, pythonFunctionPath));
        pb.redirectErrorStream(true);
        pb.redirectInput();
        pb.redirectOutput();
        return  pb.start();
    }
}

