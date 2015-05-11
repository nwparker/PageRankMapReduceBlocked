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
		// Create the node object
		Node node_obj = new Node(node.toString());
    	
		// Emit the original node to keep track of its previous PR
		node_obj.is_original = true;
    	context.write(new IntWritable(node_obj.Id), node_obj);

    	LOG.info("OUTGOING FOR " + node_obj.Id);
    	for(int i: node_obj.outgoing) {
    		LOG.info(i);
    	}
    	
    	for(int dest_id: node_obj.outgoing) {
    		// Emit the partial page ranks for the dest nodes
    		Node dest_node = new Node(dest_id, node_obj.rank / node_obj.outgoing.length, null, null);
    		dest_node.is_original = false;
    		LOG.info(dest_node.toString());
            context.write(new IntWritable(dest_id), dest_node);
    	}
    }
}