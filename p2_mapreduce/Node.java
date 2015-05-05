package p2_mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Node implements Writable {
	protected int Id;						// node id
	protected float rank;					// node's current pagerank
	protected ArrayList<Integer> outgoing;	// all outgoing edges
	protected ArrayList<Float> incoming;	// incoming pagerank VALUES from other blocks

	public Node(int Id, ArrayList<Integer> outgoing, ArrayList<Float> incoming) {
		this.Id = Id;
		if(outgoing != null) this.outgoing = outgoing;
		if(incoming != null) this.incoming = incoming;
	}
	
	public Node(int Id) {
		this(Id, new ArrayList<Integer>(), new ArrayList<Float>());
	}
	
	public int getBlockID() {
		return BlockIDFinder.BlockIdOfNode(this.Id);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Id = in.readInt();
		rank = in.readFloat();
		int outgoing_size = in.readInt();
		outgoing = new ArrayList<Integer>(outgoing_size);
		for(int i = 0; i < outgoing_size; i++) {
			outgoing.add(in.readInt());
		}
	
		int incoming_size = in.readInt();
		incoming = new ArrayList<Float>(incoming_size);
		for(int i = 0; i < incoming_size; i++) {
			incoming.add(in.readFloat());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(Id);
		out.writeFloat(rank);
		out.writeInt(outgoing.size());
		for(int i: outgoing) {
			out.writeInt(i);
		}
		out.writeInt(incoming.size());
		for(float f: incoming) {
			out.writeFloat(f);
		}
	}
}
