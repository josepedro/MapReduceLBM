import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Preprocessor {

    private String fileInputPath;
    private String fileOutputPath;

    public Preprocessor(String fileInputPath, String fileOutputPath) {
        this.fileInputPath = fileInputPath;
        this.fileOutputPath = fileOutputPath;
    }

    public void preprocess(){

        // This will reference one line at a time
        String line = null;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileInputPath);

            PrintWriter writer = new PrintWriter(fileOutputPath, "UTF-8");

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            int id = 0;
            while((line = bufferedReader.readLine()) != null) {
                String[] lineCells = line.split(" ");
                for (String cell : lineCells){
                    double density = Double.parseDouble(cell);
                    id += 1;
                    writer.println(String.valueOf(id) + " " +
                            "0:" + density/9 + " " +
                            "1:" + density/9 + " " +
                            "2:" + density/9 + " " +
                            "3:" + density/9 + " " +
                            "4:" + density/9 + " " +
                            "5:" + density/9 + " " +
                            "6:" + density/9 + " " +
                            "7:" + density/9 + " " +
                            "8:" + density/9);
                }
            }

            // Always close files.
            bufferedReader.close();
            writer.close();
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




    }

}