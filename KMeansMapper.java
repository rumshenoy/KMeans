import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ramyashenoy on 12/4/14.
 */
public class KMeansMapper extends Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {

    public static List<Double> centers = new ArrayList<Double>();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {

            Configuration conf = context.getConfiguration();
            String iteration = conf.get("iteration");
            Path pt;
            String uri = conf.get("fs.default.name");
            FileSystem fs = null;
            fs = FileSystem.get(new URI(uri), conf);


            //decide file directory to read centroids from
            if (Integer.parseInt(iteration) == 0) {
                pt = new Path("/user/hduser/input/centroids.txt");
            } else {
                iteration = (String.valueOf(Integer.parseInt(iteration) - 1));
                pt = new Path("kmeansoutput/" + iteration);
            }


            centers.clear();
            FileStatus[] fileStatuses = fs.listStatus(pt);

            //read centroids from previously computer iteration
            for (int i = 0; i < fileStatuses.length; i++) {

                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(fileStatuses[i].getPath())));
                String lineFromFile;
                while ((lineFromFile = bufferedReader.readLine()) != null) {
                    String[] temp = lineFromFile.split("\t");
                    centers.add(Double.parseDouble(temp[0]));
                }
            }

            //calculate distance from each centroid to the point
            String line = value.toString();
            double point = Double.parseDouble(line);
            double min1, min2 = Double.MAX_VALUE, nearest_center = centers
                    .get(0);
            for (double c : centers) {
                min1 = c - point;
                if (Math.abs(min1) < Math.abs(min2)) {
                    nearest_center = c;
                    min2 = min1;
                }
            }

            //write output to reduce as {key: <nearest centroid>, value: <point>}
            context.write(new DoubleWritable(nearest_center),
                    new DoubleWritable(point));


        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}