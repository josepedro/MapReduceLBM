import java.io.*;
import java.math.BigDecimal;
import java.math.MathContext;
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

    public void generateDefaultMesh() throws IOException {
        int sizeAxial = 6;
        int sizeVertical = 6;
        BigDecimal density = new BigDecimal("1.0000");
        BigDecimal deltaDensity = new BigDecimal("0.0009");

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileInputPath))) {
            for (int vertical = 0; vertical < sizeVertical; vertical++) {
                for (int axial = 0; axial < sizeAxial; axial++) {
                    if (axial == sizeAxial - 1) {
                        String number = density.toString();
                        bw.write(number);
                        bw.write(" ");
                    }
                    else if (axial == sizeAxial / 2 - 1 && vertical == sizeVertical / 2 - 1){
                        String number = deltaDensity.toString();
                        bw.write(number);
                        bw.write("\n");
                    }
                    else {
                        String number = density.toString();
                        bw.write(number);
                        bw.write(" ");
                    }
                }
            }

            // no need to close it.
            bw.close();

        } catch (IOException e) {

            e.printStackTrace();

        }

    }

    public void preprocess(){

        // This will reference one line at a time
        String line = null;
        String[] directions = {"0:", "1:", "2:", "3:", "4:", "5:", "6:", "7:", "8:"};
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileInputPath);

            BufferedWriter bw = new BufferedWriter(new FileWriter(fileOutputPath));

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            int id = 0;
            while((line = bufferedReader.readLine()) != null) {
                String[] lineCells = line.split(" ");
                for (String cell : lineCells){
                    for (String direction : directions){
                        BigDecimal density = new BigDecimal(cell);
                        density = density.divide(new BigDecimal("9.00"), MathContext.DECIMAL128);
                        bw.write(String.valueOf(id) + " " +
                        direction + density.toString());
                    }
                }
            }

            // Always close files.
            bufferedReader.close();
            bw.close();
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
