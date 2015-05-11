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
    public void map(LongWritable id, Text node_text, Context context) throws IOException, InterruptedException {
    	Node node = new Node(node_text.toString().split("\t")[1]);
    	context.write(new IntWritable(node.getBlockID()), node);
    }
}
