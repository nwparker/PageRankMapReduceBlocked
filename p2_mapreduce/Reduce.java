package p2_mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * IN: node id -> list of rank flows
 * OUT: node id -> pagerank
 */
public class Reduce extends Reducer<IntWritable, Node, IntWritable, Node> {
	private static final Log LOG = LogFactory.getLog(Reduce.class);

	private static final float SCALING = 1000;
	private static final float DAMPING_FACTOR = 0.75f;
	
    public void reduce(IntWritable node_id, Iterable<Node> incoming_nodes, Context context)
    throws IOException, InterruptedException {
    	
    	int num_nodes = context.getConfiguration().getInt("num_nodes", -1); // should never be the default of -1
    	float original_pagerank = 0;
        float new_pagerank = 0;
        Node original = null;
        
        // sum up flow from other nodes for this node
        for (Node node: incoming_nodes) {
        	if (node.is_original) {
        		original = node;
        		original_pagerank = node.rank;
        	}
        	else {
        		new_pagerank += node.rank;
        	}
        }
        
        // include the damping factor in new pagerank
        float damping_factor = (1 - DAMPING_FACTOR) / (float) num_nodes;
        new_pagerank = damping_factor + (DAMPING_FACTOR * new_pagerank);
        
        // emit node and new page rank
        if(original != null) {
	        original.rank = new_pagerank;
	        LOG.info("Outgoing " + original.toString());
	        context.write(node_id, original);
	        
	        // update the residuals
	        long delta = (long) Math.abs(SCALING*((original_pagerank - new_pagerank) / original_pagerank));
	        context.getCounter(SimpleMapReduce.Counter.RESIDUALS).increment(delta);
        }
    }
}