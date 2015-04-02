import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.omg.SendingContext.RunTime;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class KMeans {

    private static final String OUTPUT_FILE_NAME = "part-r-00000";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        boolean stopMapReduce = false;
        int iteration = 0;

        while (stopMapReduce == false) {
            Configuration conf = new Configuration();
            conf.set("iteration", String.valueOf(iteration));
            String oldCentroidFilePath;
            String newCentroidFilePath;



            Job job = new Job(conf);
            job.setJobName("KMeans");


            job.setJarByClass(KMeansMapper.class);
            job.setJarByClass(KMeansReducer.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);


            FileInputFormat.setInputPaths(job, new Path(args[0] + "data.txt"));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + String.valueOf(iteration) + "/"));

            job.waitForCompletion(true);

            if (iteration == 0) {
                oldCentroidFilePath = args[0];
                newCentroidFilePath = args[1] + String.valueOf(iteration) + "/";
            } else {
                oldCentroidFilePath = args[1] + String.valueOf(iteration - 1) + "/";
                newCentroidFilePath = args[1] + String.valueOf(iteration) + "/";
            }

            //check if values
            List<Double> nextIterationCentroids = new ArrayList<Double>();

            String lineFromFile;

            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fileStatuses = fs.listStatus(new Path(newCentroidFilePath));

            for(int i = 0; i< fileStatuses.length; i++){

                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(fileStatuses[i].getPath())));
                lineFromFile = bufferedReader.readLine();
                while (lineFromFile != null) {
                    String[] splitLine = lineFromFile.split("\t");
                    nextIterationCentroids.add(Double.parseDouble(splitLine[0]));
                    lineFromFile = bufferedReader.readLine();
                }
                bufferedReader.close();
            }

            List<Double> prevIterationCentroids = new ArrayList<Double>();

            Path oldDirectory = new Path(oldCentroidFilePath);
            System.out.println("************  " +oldDirectory.getName());
            FileStatus[] fileStatuses2 = fs.listStatus(oldDirectory);

            for(int i = 0; i< fileStatuses2.length; i++) {

                BufferedReader bufferedReader2 = new BufferedReader(new InputStreamReader(fs.open(fileStatuses2[i].getPath())));
                while ((lineFromFile = bufferedReader2.readLine()) != null) {
                    String[] temp = lineFromFile.split("\t");
                    prevIterationCentroids.add(Double.parseDouble(temp[0]));
                }
                bufferedReader2.close();
            }

            Collections.sort(prevIterationCentroids);
            Collections.sort(nextIterationCentroids);

            Iterator<Double> iterator = prevIterationCentroids.iterator();
            for (Double d : nextIterationCentroids) {
                if (Math.abs(iterator.next() - d) < 0.1) {
                    stopMapReduce = true;
                } else {
                    stopMapReduce = false;
                    break;
                }
            }

            iteration++;
        }
    }
}