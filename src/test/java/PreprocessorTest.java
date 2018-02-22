import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class PreprocessorTest {

    @Test
    public void testPreprocess() throws IOException {

        String dataInput = "1 1 1\n" +
                           "1 1 1\n" +
                           "1 1 1\n";
        String fileInputPath = "/home/pedro/IdeaProjects/WordCount/inputData.txt";

        PrintWriter writer = new PrintWriter(fileInputPath, "UTF-8");
        writer.print(dataInput);
        writer.close();

        String dataOutput = "1 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n" +
                "2 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n" +
                "3 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n" +
                "4 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n" +
                "5 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n" +
                "6 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n" +
                "7 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n" +
                "8 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n" +
                "9 0:0.1111111111111111 1:0.1111111111111111 2:0.1111111111111111 3:0.1111111111111111 4:0.1111111111111111 5:0.1111111111111111 6:0.1111111111111111 7:0.1111111111111111 8:0.1111111111111111\n";
        String fileOutputPath = "outputData.txt";

        Preprocessor preprocessor = new Preprocessor(fileInputPath, fileOutputPath);
        preprocessor.preprocess();


        // This will reference one line at a time
        String line = null;
        String result = "";
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileOutputPath);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                result += line + "\n";
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file");
            // Or we could just do this:
            // ex.printStackTrace();
        }

        assertEquals(dataOutput, result);

    }
}