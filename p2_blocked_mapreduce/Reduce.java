import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * IN: node id -> list of rank flows
 * OUT: node id -> pagerank
 */
public class Reduce extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

}
