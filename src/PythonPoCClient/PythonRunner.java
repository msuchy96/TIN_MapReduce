package PythonPoCClient;

import JavaWorker.RegisterWorkersQueueWrapper;
import org.apache.commons.lang3.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;


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

    public static void map(RegisterWorkersQueueWrapper registerWorkersQueueWrapper){

        //zakladamy dla uproszczenia, ze:
        //wywolujemy skrypt pythonowy, ktory czyta z wejscia standardowego
        //na wejscie standardowe MY wrzucamy dane z dataFile
        //a wyniki ma wypluwac na stdout w postaci KLUCZ=>WARTOSC, gdzie wartosc musi dac sie sparsowac na inta, klucz dowolny(string)

        ArrayList<Pair<String,Integer>> mapResults= new ArrayList<Pair<String,Integer>>();

        try {
            System.out.println("Start process execution for python");
            ProcessBuilder pb = new ProcessBuilder(Arrays.asList(pythonPath, pythonMapPath));
            pb.redirectErrorStream(true);
            pb.redirectInput();
            pb.redirectOutput();
            Process process = pb.start();

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
                synchronized (registerWorkersQueueWrapper){
                    registerWorkersQueueWrapper.put(new Pair<>(resultPair[0],Integer.valueOf(resultPair[1])));
                    // to avoid sync problem
                    if(!reader.ready()){
                        registerWorkersQueueWrapper.setIsEnd(true);
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("Exception occurred in PythonRunner");
            e.printStackTrace();
        }
    }
}

