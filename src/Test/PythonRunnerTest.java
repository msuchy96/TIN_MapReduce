package Test;

import JavaWorker.DataSyncWrapper;
import PythonPoCClient.PythonRunner;
import org.apache.commons.lang3.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by suchy on 29.05.2018.
 */
public class PythonRunnerTest {

    public static void main(String[] args){

        PythonRunner pyRunner = new PythonRunner("dane.txt","map.py","reduce.py");
        DataSyncWrapper dataSyncWrapper = new DataSyncWrapper(TestUtils.NUMBER_OF_WORKERS);
        pyRunner.map(dataSyncWrapper);

        try {
            System.in.read();
        } catch (Exception e){
            System.out.println("Exception occured while reading key");
        }

        /* TESTING changing value in collection
        Map<Integer,Map<String,TestClass>> testMap = new HashMap<>();
        Map<String,TestClass> mapt = new HashMap<>();
        TestClass tC = new TestClass();
        tC.setQua(15);
        mapt.put("lol",tC);
        testMap.put(10,mapt);
        testMap.get(10).get("lol").setQua(testMap.get(10).get("lol").getQua()+1);
        System.out.println(testMap.get(10).get("lol").getQua());
        */

    }

    private static class TestClass{
        private Integer qua = 0;

        public void setQua(Integer qua){
            this.qua = qua;
        }

        public Integer getQua(){
            return qua;
        }
    }
}
