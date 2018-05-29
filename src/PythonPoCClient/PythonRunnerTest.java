package PythonPoCClient;

/**
 * Created by suchy on 29.05.2018.
 */
public class PythonRunnerTest {

    public static void main(String[] args){
        PythonRunner pyRunner = new PythonRunner();
        pyRunner.map();
        try {
            System.in.read();
        } catch (Exception e){
            System.out.println("Exception occured while reading key");
        }

    }
}
