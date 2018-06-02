package Test;

import JavaWorker.DataSyncWrapper;
import PythonPoCClient.PythonRunner;


/**
 * Created by suchy on 29.05.2018.
 */
public class Test{

    public static void main(String[] args){
        pythonMapTest();
    }

    public static void pythonMapTest(){
        PythonRunner pyRunner = new PythonRunner("dane.txt","map.py","reduce.py","G:/ProjektyELKA/TIN/src/PythonPoCClient/sources/","C:/Python27/python.exe");
        DataSyncWrapper dataSyncWrapper = new DataSyncWrapper();
        pyRunner.map(dataSyncWrapper);

        try {
            System.in.read();
        } catch (Exception e){
            System.out.println("Exception occured while reading key");
        }
    }
}
