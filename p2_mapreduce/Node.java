package p2_mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class Node implements Writable {
	protected int Id;						// node id
	protected float rank;					// node's current pagerank
	protected int[] outgoing;	// all outgoing edges
	protected float[] incoming;	// incoming pagerank VALUES from other blocks
	protected boolean is_original;	// indicates the node is the original node
	
	public static final char DELIM = ',';
	public static final char DELIM2 = ' ';

	private static final Log LOG = LogFactory.getLog(Node.class);

	public Node(){}
	
	public Node(int Id, float rank, int[] outgoing, float[] incoming) {
		this.Id = Id;
		this.rank = rank;
		if(outgoing != null) this.outgoing = outgoing;
		else this.outgoing = new int[] {};
		
		if(incoming != null) this.incoming = incoming;
		else this.incoming = new float[] {};
		
		this.is_original = false;
	}
	
	public Node(int Id) {
		this(Id, 0, new int[] {}, new float[] {});
	}
	
	//Reconstruct from toString
	public Node(String nodeStr){		
		String[] mySplit = nodeStr.split(",");
		
		this.Id = Integer.parseInt(mySplit[0]);
		this.rank = Float.parseFloat(mySplit[1]);
		
		//get outgoing
		if(mySplit.length > 2) {
			String [] outgoing_str = mySplit[2].split(" ");
			int[] outgoing = new int[outgoing_str.length];
			for (int i=0; i<outgoing_str.length; i++){
				outgoing[i] = Integer.parseInt(outgoing_str[i]);
			}
			this.outgoing = outgoing;
		} else {
			this.outgoing = new int[] {};
		}
		
		//get incoming
		if (mySplit.length > 3) {
			String [] incoming_str = mySplit[3].split(" ");
			float[] incoming = new float[incoming_str.length];
			for (int i=0; i<incoming_str.length; i++){
				incoming[i] = Float.parseFloat(incoming_str[i]);
			}
			this.incoming = incoming;
		} else {
			this.incoming = new float[] {};
		}
		
		// constructing from file, original is true
		this.is_original = true;
	}
	
	
	@Override
	public String toString() {
		/*
		 * ID, rank, outgoing(space delim), incoming(space delim), is_original
		 */
		StringBuilder sb = new StringBuilder();
		sb.append(this.Id);
		sb.append(DELIM);
		sb.append(this.rank);
		sb.append(DELIM);
		//convert outgoing edges
		if (this.outgoing.length > 0){
			for(int edge : this.outgoing){
				sb.append(edge);
				sb.append(DELIM2);
			}
			sb.deleteCharAt(sb.length()-1);
		}
		sb.append(DELIM);
		//convert incoming values
		if (this.incoming.length > 0){
			for(float income: this.incoming){
				sb.append(income);
				sb.append(DELIM2);
			}
			sb.deleteCharAt(sb.length()-1);
		}
		return sb.toString();
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
		is_original = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(Id);
		out.writeFloat(rank);
		out.writeInt(outgoing.length);
		for(int i: outgoing) {
			out.writeInt(i);
		}

		out.writeInt(incoming.length);
		for(float f: incoming) {
			out.writeFloat(f);
		}
		out.writeBoolean(is_original);
	}
}
