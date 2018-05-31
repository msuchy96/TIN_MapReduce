package Test;

import JavaWorker.RegisterWorkersQueueWrapper;
import PythonPoCClient.PythonRunner;

/**
 * Created by suchy on 29.05.2018.
 */
public class PythonRunnerTest {

    public static void main(String[] args){
        PythonRunner pyRunner = new PythonRunner("dane.txt","map.py","reduce.py");
        RegisterWorkersQueueWrapper registerWorkersQueueWrapper = new RegisterWorkersQueueWrapper(TestUtils.NUMBER_OF_WORKERS);
        pyRunner.map(registerWorkersQueueWrapper);
        try {
            System.in.read();
        } catch (Exception e){
            System.out.println("Exception occured while reading key");
        }

    }
}
