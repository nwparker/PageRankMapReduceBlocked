package p2_mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/*
 * IN: src node id -> node info
 * OUT: dest node id -> rank flow
 */
public class Map extends Mapper<IntWritable, Node, IntWritable, FloatWritable> {
        
    public void map(IntWritable id, Node node, Context context) throws IOException, InterruptedException {
    	FloatWritable rank_flow = new FloatWritable(node.rank / node.outgoing.size());
    	
    	for(int dest_id: node.outgoing) {
            context.write(new IntWritable(dest_id), rank_flow);
    	}
    }
}