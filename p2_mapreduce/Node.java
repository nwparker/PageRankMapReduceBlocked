package p2_mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Node implements Writable {
	protected int Id;						// node id
	protected float rank;					// node's current pagerank
	protected int[] outgoing;	// all outgoing edges
	protected float[] incoming;	// incoming pagerank VALUES from other blocks

	public Node(int Id, float rank, int[] outgoing, float[] incoming) {
		this.Id = Id;
		this.rank = rank;
		if(outgoing != null) this.outgoing = outgoing;
		else this.outgoing = new int[] {};
		
		if(incoming != null) this.incoming = incoming;
		else this.incoming = new float[] {};
	}
	
	public Node(int Id) {
		this(Id, 0, new int[] {}, new float[] {});
	}
	
	public int getBlockID() {
		return BlockIDFinder.BlockIdOfNode(this.Id);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Id = in.readInt();
		rank = in.readFloat();
		int outgoing_size = in.readInt();
		outgoing = new int[outgoing_size];
		for(int i = 0; i < outgoing_size; i++) {
			outgoing[i] = in.readInt();
		}
	
		int incoming_size = in.readInt();
		incoming = new float[incoming_size];
		for(int i = 0; i < incoming_size; i++) {
			incoming[i] = in.readFloat();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(Id);
		out.writeFloat(rank);
		out.writeInt(outgoing.length);
		for(int i: outgoing) {
			out.writeInt(i);
		}
		System.out.println(incoming);

		out.writeInt(incoming.length);
		for(float f: incoming) {
			out.writeFloat(f);
		}
	}
}
