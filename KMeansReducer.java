import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ramyashenoy on 12/4/14.
 */
public class KMeansReducer extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text> {

    @Override
    protected void reduce(DoubleWritable key, java.lang.Iterable<DoubleWritable> values, org.apache.hadoop.mapreduce.Reducer<DoubleWritable,DoubleWritable,DoubleWritable,Text>.Context context) throws java.io.IOException, java.lang.InterruptedException {
        double newCenter;
        double sum = 0;
        int no_elements = 0;
        String points = "";
        Iterator<DoubleWritable> it= values.iterator();
        while (it.hasNext()) {
            double d = it.next().get();
            points = points + " " + Double.toString(d);
            sum = sum + d;
            ++no_elements;
        }

        //calculate anew centroid
        newCenter = sum / no_elements;
        context.write(new DoubleWritable(newCenter), new Text(points));
    }
}