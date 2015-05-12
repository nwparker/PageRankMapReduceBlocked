package p2_mapreduce;

public class BlockIDFinder {
	public static int BlockIdOfNode(int[] block_ids, int node_id) {
		int index = node_id/10000;
		if(block_ids[index] < node_id) {
			return index+1;
		} else if(index == 0 || block_ids[index-1] < node_id) {
			return index;
		} else {
			return index-1;
		}
	}
}
