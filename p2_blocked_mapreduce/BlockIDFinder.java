import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BlockIDFinder {
	static int[] block_ids = new int[68];
	
	public BlockIDFinder() {
		// initialize by reading blocks.txt and populating block ids array
		int sum = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader("blocks.txt"))) {
		    String line = null;
		    int index = 0;
		    while ((line = reader.readLine()) != null) {
		    	sum += Integer.parseInt(line);
		    	block_ids[index] = sum;
		    	index++;
		    }
		    block_ids[68] = block_ids[67] * 2;
		} catch (IOException x) {
		    System.err.format("IOException: %s%n", x);
		}
	}
	
	public static int BlockIdOfNode(int node_id) {
		int index = node_id/10000;
		if(block_ids[index] < node_id) {
			return index+1;
		} else if(index == 0 || block_ids[index-1] < node_id) {
			return index;
		} else {
			return index-1;
		}
	}
	
	public static void main(String[] args) {
		BlockIDFinder bf = new BlockIDFinder();
		
	}
}
