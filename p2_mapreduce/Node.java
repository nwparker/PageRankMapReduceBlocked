package p2_mapreduce;

import java.util.ArrayList;

public class Node {
	protected int Id;						// node id
	protected ArrayList<Integer> outgoing;	// all outgoing edges
	protected ArrayList<Integer> incoming;	// incoming from other blocks

	public Node(int Id, ArrayList<Integer> outgoing, ArrayList<Integer> incoming) {
		this.Id = Id;
		this.outgoing = outgoing;
		this.incoming = incoming;
	}
	
	public Node(int Id) {
		this(Id, new ArrayList<Integer>(), new ArrayList<Integer>());
	}
	
	public int getBlockID() {
		return BlockIDFinder.BlockIdOfNode(this.Id);
	}
}
