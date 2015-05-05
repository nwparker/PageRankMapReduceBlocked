import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Given a node object, emit 
 * IN: src node id -> node info
 * OUT: node's block id -> node info
 */
public class Map extends Mapper<IntWritable, Node, IntWritable, Node> {
    public void map(IntWritable id, Node node, Context context) throws IOException, InterruptedException {
    	context.write(new IntWritable(node.getBlockID()), node);
    }
}
