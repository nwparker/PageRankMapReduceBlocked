import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * IN: node id -> list of rank flows
 * OUT: node id -> pagerank
 */
public class Reduce extends Reducer<IntWritable, Node, IntWritable, FloatWritable> {
	
    public void reduce(IntWritable id, Iterable<Node> nodes, Context context)
    throws IOException, InterruptedException {
    	HashMap<Integer, Float> node_ranks = pagerankBoundaries(nodes);
    	
    	for(Map.Entry<Integer, Float> entry : node_ranks.entrySet()){
    		IntWritable nodeID = new IntWritable(entry.getKey());
    	    FloatWritable nodePageRank = new FloatWritable(entry.getValue());
    	    context.write(nodeID, nodePageRank);
    	}
    	
    }
}



