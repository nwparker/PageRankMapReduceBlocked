package p2_mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/*
 * IN: node id -> list of rank flows
 * OUT: node id -> pagerank
 */
public class Reduce extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
    
    public void reduce(IntWritable id, Iterable<FloatWritable> rank_flows, Context context)
    throws IOException, InterruptedException {
        float pagerank = 0;
        for (FloatWritable flow : rank_flows) {
        	pagerank += flow.get();
        }
        context.write(id, new FloatWritable(pagerank));
    }
}