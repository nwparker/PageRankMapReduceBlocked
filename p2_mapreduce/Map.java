package p2_mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/*
 * IN: src node id -> node info
 * OUT: dest node id -> rank flow
 */
public class Map extends Mapper<LongWritable, Text, IntWritable, Node> {
	private static final Log LOG = LogFactory.getLog(Map.class);

    public void map(LongWritable key, Text node, Context context) throws IOException, InterruptedException {    	
		String[] node_vals = node.toString().split("\\$");
		if (node_vals.length < 2) {
			System.out.println("Line format was mezzed uppp.");
			return;
		}
		
		// parse node input
		int id = Integer.parseInt(node_vals[0]);
		String[] outgoing_vals = node_vals[1].split(" ");
		int[] outgoing = new int[outgoing_vals.length];
		for(int k = 0; k < outgoing.length; k++) {
			outgoing[k] = Integer.parseInt(outgoing_vals[k]);
		}
		float rank = Float.parseFloat(node_vals[2]);
		
		// Create the node object
		Node node_obj = new Node(id, rank, outgoing, null);    	
    	
		// Emit the original node to keep track of its previous PR
		node_obj.is_original = true;
    	context.write(new IntWritable(id), node_obj);

    	for(int dest_id: node_obj.outgoing) {
    		// Emit the partial page ranks for the dest nodes
            context.write(new IntWritable(dest_id), new Node(id, rank / outgoing.length, null, null));
    	}
    }
}