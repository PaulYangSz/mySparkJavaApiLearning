import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/**
 * Created by Administrator on 2017/4/18.
 */
public class LaunchSparkProgram {
    public static void main(String[] args) throws Exception {
        if(false) { //Use handle
            System.out.println("Use SparkAppHandle to launch a spark application.");
            SparkAppHandle handle = new SparkLauncher()
                    .setAppResource("/home/paul/share/mySparkJavaApiLearning/target/learnSparkJavaApi-1.0.jar")
                    .setMainClass("simpleRddMain")
                    .setMaster("local")
                    .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                    .startApplication();
            //Use handle API to monitor / control application.
        }
        else { //Use process
            System.out.println("Use Process to launch a spark application.");
            Process spark = new SparkLauncher()
                    .setAppResource("/home/paul/share/mySparkJavaApiLearning/target/learnSparkJavaApi-1.0.jar")
                    .setMainClass("simpleRddMain")
                    .setMaster("local")
                    .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                    .launch();
            spark.waitFor();

        }
        System.out.println("Launch from SparkLauncher finished.");
    }
}
