package PythonPoCClient;

import org.apache.commons.lang3.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;


/**
 * Created by suchy on 29.05.2018.
 */
public class PythonRunner {


    private static String pythonPath = " C:/Python27/python.exe";
    private static String dataFile;
    private static String pythonMapPath;
    private static String pythonReducePath;
    public ArrayList<Pair<String,Integer>> mapResults;



    public PythonRunner(String dataFile, String mapFile, String reduceFile){
        this.dataFile = dataFile;
        this.pythonMapPath = mapFile;
        this.pythonReducePath = reduceFile;
    }

    public PythonRunner(){
        this.dataFile = "G:/ProjektyELKA/TIN/src/PythonPoCClient/dane.txt";
        this.pythonMapPath = "G:/ProjektyELKA/TIN/src/PythonPoCClient/map.py";
        mapResults = new ArrayList<Pair<String, Integer>>();

    }


    public static void map(){

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

            try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                    writer.write(line);
                    writer.newLine();
                }
            }
            writer.close();
            process.waitFor();

            while(reader.ready()) {
                System.out.println(reader.readLine());
            }
        } catch (Exception e) {
            System.out.println("Exception occured");
        }
    }
}
