package hpi.des.flink_tutorial.util;


import java.io.File;
import java.net.URL;
import java.nio.file.Paths;

public class InputFile {

    public static String getFilePath(String filename) throws Exception {
        URL res = InputFile.class.getClassLoader().getResource(filename);

        if(res == null){
            System.err.println("Check that the file "+filename+" exists in the resources folder under src/main");
            throw new Exception();
        }

        File file = Paths.get(res.toURI()).toFile();
        return file.getAbsolutePath();
    }

    public static String getInputFilePath() throws Exception {
        return getFilePath("yellow_tripdata_2020-04.csv");

    }
}
