package p2_mapreduce;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Given a node object, emit 
 * IN: src node id -> node info
 * OUT: node's block id -> node info
 */
public class Map extends Mapper<LongWritable, Text, IntWritable, Node> {
	int[] block_ids = {10328,20373,30629,40645,50462,60841,70591,80118,90497,100501,110567,120945,130999,140574,150953,161332,171154,181514,191625,202004,212383,222762,232593,242878,252938,263149,273210,283473,293255,303043,313370,323522,333883,343663,353645,363929,374236,384554,394929,404712,414617,424747,434707,444489,454285,464398,474196,484050,493968,503752,514131,524510,534709,545088,555467,565846,576225,586604,596585,606367,616148,626448,636240,646022,655804,665666,675448,685230,1370460};

	public void map(LongWritable id, Text node_text, Context context) throws IOException, InterruptedException {
    	// parse the node from string
    	Node node = new Node(node_text.toString().split("\t")[1]);
    	
    	// emit this node to the reducer for its block id
    	int block_id = BlockIDFinder.BlockIdOfNode(block_ids, node.Id);
    	context.write(new IntWritable(block_id), node);
    	
    	for(int dest_id: node.outgoing) {
    		// if linking to different block, this node is a boundary node for that block
    		int dest_block_id = BlockIDFinder.BlockIdOfNode(block_ids, dest_id);
    		if(block_id != dest_block_id) {
    			Node boundary_node = new Node(node.Id, node.rank, node.outgoing, node.incoming);
    			boundary_node.is_boundary = true;
    			context.write(new IntWritable(dest_block_id), boundary_node);
    		}
    	}
    }
}
