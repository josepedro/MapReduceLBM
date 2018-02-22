import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.lang.*;

public class WordCount {
    public static int number_lines;
    public static int number_rows;

    public static int[] getIdPosition(int id, int number_lines, int number_rows){
        int idCount = 1;
        for (int line = 0; line < number_lines; line++) {
            for (int row = 0; row < number_rows; row++) {
                if (idCount == id) {
                    int[] idPosition = {line, row};
                    return idPosition;
                } else {
                    idCount += 1;
                }
            }
        }
        return null;
    }

    public static int getPositionId(int lineId, int rowId, int number_lines, int number_rows){
        int idCount = 1;
        for (int line = 0; line < number_lines; line++) {
            for (int row = 0; row < number_rows; row++) {
                if (line == lineId && row == rowId) {
                    return idCount;
                } else {
                    idCount += 1;
                }
            }
        }
        return idCount;
    }

    public static int[] get1(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            int[] result = {number_lines - 1, row};
            return result;
        }
        if (line == 0 && row == number_rows - 1){
            int[] result = {number_lines - 1, number_rows - 1};
            return result;
        }
        if (line == 0 && row > 0 && row < number_rows - 1){
            int[] result = {number_lines - 1, row};
            return result;
        }
        else{
            int[] result = {line - 1, row};
            return result;
        }
    }

    public static int get1_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get1(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get2(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            return new int[] {number_lines - 1, row + 1};
        }
        if (line == 0 && row == number_rows - 1){
            return new int[] {number_lines - 1, 0};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {number_lines - 2, 0};
        }
        if (line == 0 && row > 0 && row < number_rows - 1){
            return new int[] {number_lines - 1, row + 1};
        }
        if (line > 0 && line < number_lines - 1 && row == number_rows - 1){
            return new int[] {line - 1, 0};
        }
        else{
            return new int[] {line - 1, row + 1};
        }
    }

    public static int get2_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get2(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get3(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == number_rows - 1){
            return new int[] {line, 0};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {number_lines - 1, 0};
        }
        if (line > 0 && line < number_lines - 1 && row == number_rows - 1){
            return new int[] {line, 0};
        }
        else{
            return new int[] {line, row + 1};
        }
    }

    public static int get3_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get3(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get4(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == number_rows - 1){
            return new int[] {1, 0};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {0, 0};
        }
        if (line == number_lines - 1 && row == 0){
            return new int[] {0, 1};
        }
        if (line > 0 && line < number_lines - 1 && row == number_rows - 1){
            return new int[] {line + 1, 0};
        }
        if (line == number_lines - 1 && row > 0 && row < number_rows - 1){
            return new int[] {0, row + 1};
        }
        else{
            return new int[] {line + 1, row + 1};
        }
    }

    public static int get4_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get4(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get5(int line, int row, int number_lines, int number_rows){
        if (line == number_lines - 1 && row == 0){
            return new int[] {0, row};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {0, row};
        }
        if (line == number_lines - 1 && row > 0 && row < number_rows - 1){
            return new int[] {0, row};
        }
        else{
            return new int[] {line + 1, row};
        }
    }

    public static int get5_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get5(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get6(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            return new int[] {line + 1, number_rows - 1};
        }
        if (line ==  number_lines - 1 && row == 0){
            return new int[] {0, number_rows - 1};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {0, row - 1};
        }
        if (line > 0 && line < number_lines - 1 && row == 0){
            return new int[] {line + 1, number_rows - 1};
        }
        if (line == number_lines - 1 && row > 0 && row < number_rows - 1){
            return new int[] {0, row - 1};
        }
        else{
            return new int[] {line + 1, row - 1};
        }
    }

    public static int get6_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get6(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get7(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            return new int[] {line, number_rows - 1};
        }
        if (line == number_lines - 1 && row == 0){
            return new int[] {line, number_rows - 1};
        }
        if (line > 0 && line < number_lines - 1 && row == 0){
            return new int[] {line, number_rows - 1};
        }
        else{
            return new int[] {line, row - 1};
        }
    }

    public static int get7_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get7(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get8(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            return new int[] {number_lines - 1, number_rows - 1};
        }
        if (line == 0 && row == number_rows - 1){
            return new int[] {number_lines - 1, row - 1};
        }
        if (line == number_lines - 1 && row == 0){
            return new int[] {line - 1, number_rows - 1};
        }
        if (line > 0 && line < number_lines - 1 && row == 0){
            return new int[] {line - 1, number_rows - 1};
        }
        if (line == 0 && row > 0 && row < number_rows - 1){
            return new int[] {number_lines - 1, row - 1};
        }
        else{
            return new int[] {line - 1, row - 1};
        }
    }

    public static int get8_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get8(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            id.set(itr.nextToken());
            // for each direction
            while (itr.hasMoreTokens()) {
                String[] dirFun = itr.nextToken().split(":");
                String direction = dirFun[0];
                String ditributionFunction = dirFun[1];
                String valueDir = direction + ":" + ditributionFunction;
                Text valueDirText = new Text(valueDir);
                //String newId = streamMap.get(direction + id.toString());
                if (Integer.parseInt(direction) == 1) {
                    String newId = Integer.toString(
                            get1_id(
                                    Integer.parseInt(id.toString()), number_lines, number_rows));
                    context.write(new Text(newId), valueDirText);
                }
                else if (Integer.parseInt(direction) == 2) {
                    String newId = Integer.toString(
                            get2_id(
                                    Integer.parseInt(id.toString()), number_lines, number_rows));
                    context.write(new Text(newId), valueDirText);
                }
                else if (Integer.parseInt(direction) == 3) {
                    String newId = Integer.toString(
                            get3_id(
                                    Integer.parseInt(id.toString()), number_lines, number_rows));
                    context.write(new Text(newId), valueDirText);
                }
                else if (Integer.parseInt(direction) == 4) {
                    String newId = Integer.toString(
                            get4_id(
                                    Integer.parseInt(id.toString()), number_lines, number_rows));
                    context.write(new Text(newId), valueDirText);
                }
                else if (Integer.parseInt(direction) == 5) {
                    String newId = Integer.toString(
                            get5_id(
                                    Integer.parseInt(id.toString()), number_lines, number_rows));
                    context.write(new Text(newId), valueDirText);
                }
                else if (Integer.parseInt(direction) == 6) {
                    String newId = Integer.toString(
                            get6_id(
                                    Integer.parseInt(id.toString()), number_lines, number_rows));
                    context.write(new Text(newId), valueDirText);
                }
                else if (Integer.parseInt(direction) == 7) {
                    String newId = Integer.toString(
                            get7_id(
                                    Integer.parseInt(id.toString()), number_lines, number_rows));
                    context.write(new Text(newId), valueDirText);
                }
                else if (Integer.parseInt(direction) == 8) {
                    String newId = Integer.toString(
                            get8_id(
                                    Integer.parseInt(id.toString()), number_lines, number_rows));
                    context.write(new Text(newId), valueDirText);
                } else {
                    context.write(id, valueDirText);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // FIRST WE HAVE TO CALCULATE DENSITY AND VELOCITIES
            double density = 0;
            double velocityX = 0;
            double velocityY = 0;
            double c = 1/Math.sqrt(3);
            double c_square = c*c;
            double[] ciX = {0,0,+c,+c,+c,0,-c,-c,-c};
            double[] ciY = {0,+c,+c,0,-c,-c,-c,0,+c};

            Map<String, Double> fis = new HashMap<String, Double>();
            for (Text val : values) {
                StringTokenizer itr = new StringTokenizer(val.toString());
                try {
                    String[] directionFi = itr.nextToken().split(":");
                    String direction = directionFi[0];
                    fis.put(directionFi[0], Double.parseDouble(directionFi[1]));
                } catch (Exception e){

                };

            }

            for (Map.Entry<String, Double> entry : fis.entrySet()) {
                density += entry.getValue();
                velocityX += (ciX[Integer.parseInt(entry.getKey())])*entry.getValue();
                velocityY += (ciY[Integer.parseInt(entry.getKey())])*entry.getValue();
            }

            velocityX = velocityX/density;
            velocityY = velocityY/density;

            /*
                8  1  2
                7  0  3
                6  5  4
            */
            // NOW WE HAVE TO COLLIDE
            double omega = 1.98;
            String resultCollide = "";
            Map<String, Double> epsilons = new HashMap<String, Double>();
            epsilons.put("0", (double) 4/9);
            epsilons.put("1", (double) 1/9);
            epsilons.put("2", (double) 1/36);
            epsilons.put("3", (double) 1/9);
            epsilons.put("4", (double) 1/36);
            epsilons.put("5", (double) 1/9);
            epsilons.put("6", (double) 1/36);
            epsilons.put("7", (double) 1/9);
            epsilons.put("8", (double) 1/36);
            double densityNew = 0;
            for (Map.Entry<String, Double> entry : fis.entrySet()) {
                double fi = entry.getValue();

                double term1 = (ciY[Integer.parseInt(entry.getKey())]*velocityY
                        + ciX[Integer.parseInt(entry.getKey())]*velocityX)/c_square;

                double term2 = ((ciY[Integer.parseInt(entry.getKey())]*velocityY + ciX[Integer.parseInt(entry.getKey())]*velocityX)
                        *(ciY[Integer.parseInt(entry.getKey())]*velocityY + ciX[Integer.parseInt(entry.getKey())]*velocityX)
                        - (velocityY*velocityY + velocityX*velocityX)*(velocityY*velocityY + velocityX*velocityX)*c_square)
                        /(2*c_square*c_square);

                double fi_eq = density*epsilons.get(entry.getKey())*(1 + term1 + term2);
                fi = fi - omega*(fi - fi_eq);
                //fi = fi - fi_eq;
                densityNew += fi;
                resultCollide += " " + entry.getKey() + ":" +  String.valueOf(fi);
            }


            Text ditributionsFunctionsText = new Text(" " + String.valueOf(densityNew));
            context.write(key, ditributionsFunctionsText);

        }
    }


    public static class MacroscopicParameters
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // FIRST WE HAVE TO CALCULATE DENSITY AND VELOCITIES
            double density = 0;
            double velocityX = 0;
            double velocityY = 0;
            double c = 1/Math.sqrt(3);
            double c_square = c*c;
            double[] ciX = {0,0,+c,+c,+c,0,-c,-c,-c};
            double[] ciY = {0,+c,+c,0,-c,-c,-c,0,+c};

            Map<String, Double> fis = new HashMap<String, Double>();
            for (Text val : values) {
                StringTokenizer itr = new StringTokenizer(val.toString());
                try {
                    String[] directionFi = itr.nextToken().split(":");
                    String direction = directionFi[0];
                    fis.put(directionFi[0], Double.parseDouble(directionFi[1]));
                } catch (Exception e){

                };

            }

            for (Map.Entry<String, Double> entry : fis.entrySet()) {
                density += entry.getValue();
                velocityX += (ciX[Integer.parseInt(entry.getKey())])*entry.getValue();
                velocityY += (ciY[Integer.parseInt(entry.getKey())])*entry.getValue();
            }

            velocityX = velocityX;
            velocityY = velocityY/density;




            Text ditributionsFunctionsText = new Text(" " + String.valueOf(density));
            context.write(key, ditributionsFunctionsText);

        }
    }

    public static void main(String[] args) throws Exception {

        String inputFile = "/home/pedro/IdeaProjects/WordCount/inputData.txt";
        String inputFileMR = "/home/pedro/IdeaProjects/WordCount/input_mr/input.txt";
        String outputFile = "/home/pedro/IdeaProjects/WordCount/output_mr";

        number_lines = 3;
        number_rows = 3;

        Preprocessor preprocessor = new Preprocessor(inputFile, inputFileMR);
        preprocessor.preprocess();

        Configuration conf = new Configuration();
        boolean status = false;
        //for (int i = 0; i < 1; i++) {
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //String fileIn = args[0] + Integer.toString(i);
        //String fileOut = args[0] + Integer.toString(i+1);
        //FileInputFormat.addInputPath(job, new Path(fileIn));
        FileInputFormat.addInputPath(job, new Path(inputFileMR));
        //FileOutputFormat.setOutputPath(job, new Path(fileOut));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        status = job.waitForCompletion(true);
        //}

        System.exit(status ? 0 : 1);
    }
}