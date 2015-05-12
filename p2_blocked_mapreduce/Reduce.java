package p2_mapreduce;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * IN: node id -> list of rank flows
 * OUT: node id -> pagerank
 */
public class Reduce extends Reducer<IntWritable, Node, IntWritable, Node> {	
	// the precision we want when converting from float to long for the Hadoop counter
	private static final int PRECISION = 1000;
	private static final Log LOG = LogFactory.getLog(Reduce.class);
	
    public void reduce(IntWritable block_id, Iterable<Node> nodes, Context context)
    	throws IOException, InterruptedException {
    	
    	// get the page ranks of nodes before updating with page rank algorithm
    	HashMap<Integer, Float> old_node_ranks = new HashMap<Integer, Float>();
    	HashMap<Integer, Node> node_map = new HashMap<Integer, Node>();
    	HashMap<Integer, Node> boundary_nodes = new HashMap<Integer, Node>();
    	
    	Iterator<Node> iter = nodes.iterator();
    	while(iter.hasNext()) {
    		Node node = (Node) iter.next();
    		if(node.is_boundary) {
    			// add this node as a boundary node for this block
    			boundary_nodes.put(node.Id, new Node(node.toString()));
    		} else {
	    		// copy the node and ranks over to data structures
	    		old_node_ranks.put(node.Id, node.rank);
	    		node_map.put(node.Id, new Node(node.toString()));
    		}
    	}
    	
    	// calculate new page ranks for nodes in this block
    	HashMap<Integer, Float> new_node_ranks = PageRank.pagerankBoundaries(node_map, boundary_nodes);
    	
    	// for each node in the block, emit the node id and its new page rank and update residual errors counter
    	for(int node_id : new_node_ranks.keySet()) {
    		float new_rank = new_node_ranks.get(node_id);
    		float old_rank = old_node_ranks.get(node_id);
    		
    		// Set the new rank for this node
    		Node node = node_map.get(node_id);
    		node.rank = new_rank;
    		
    		// emit the node object with its new page rank
    	    context.write(new IntWritable(node_id), node);
    	    
    		// update the residual counter
    		long residual = (long) (PRECISION * Math.abs((old_rank - new_rank) / old_rank));
        	context.getCounter(BlockedPageRank.Counter.RESIDUALS).increment(residual);
    	}	
    }
}



