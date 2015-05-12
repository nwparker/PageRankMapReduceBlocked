package p2_mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

public class PageRank {
	private static final int MAX_ITERATIONS = 100;
	private static final double DAMPING_FACTOR = 0.75;
	private static final double CONVERGENCE_THRESHOLD = 0.001;
	
	/*
	 * Compute pageranks of the nodes
	 */
	public static HashMap<Integer, Float> pagerankBoundaries(HashMap<Integer, Node> nodes, HashMap<Integer, Node> node_boundaries) {

		int num_nodes = nodes.size();
		HashMap<Integer, Float> cur_ranks = new HashMap<>();
		HashMap<Integer, Float> new_ranks = new HashMap<>();
		boolean converged = false;
		
		// get current page rank of each node
		for(Entry<Integer, Node> e: nodes.entrySet())
			cur_ranks.put(e.getKey(), e.getValue().rank);
		
		int num_iterations = 0;
		while(!converged && (num_iterations < MAX_ITERATIONS || MAX_ITERATIONS == -1)) {
			num_iterations++;
			float initRank = (float) ((1 - DAMPING_FACTOR)/num_nodes);
			
			for(Entry<Integer, Node> e: nodes.entrySet()) {
				new_ranks.put(e.getKey(), initRank); //must be done in this loop
			}
			
			// Iterate through nodes and update ranks
			for(Entry<Integer, Node> e: nodes.entrySet()) {
				Node n = e.getValue();
				float nodeRank = new_ranks.get(n.Id);
				
				// add rank values from incoming boundary edges
				for(int boundary_node_id: n.incoming){
					Node boundary_node = node_boundaries.get(boundary_node_id);
					float boundary_val = boundary_node.rank/boundary_node.outgoing.length;
					nodeRank += boundary_val * DAMPING_FACTOR;
				}
					
				//Put rank into node
				new_ranks.put(n.Id, nodeRank);
				
				// propagate rank of current node to its out going links
				float out_flow;
				int num_outgoing = n.outgoing.length;
				if (num_outgoing > 0) {
					for(int target_id: n.outgoing) {
						out_flow = (float)(DAMPING_FACTOR * cur_ranks.get(n.Id) / num_outgoing);
						new_ranks.put(target_id, new_ranks.get(target_id) + out_flow);
					}
				
				// if no outgoing edges, distribute evenly to all nodes in block
				} else {
					for(Entry<Integer, Node> e2: nodes.entrySet()) {
						out_flow = (float)(DAMPING_FACTOR * cur_ranks.get(n.Id) / num_nodes);
						new_ranks.put(e2.getKey(), new_ranks.get(e2.getKey()) + out_flow);
					}
				}
			}
			
			System.out.println("Iteration: " + num_iterations);
			System.out.println("Ranks: " + new_ranks);

			// check convergence
			float diffsum = 0;

			// add the percent change from the original rank
			for(Entry<Integer, Node> e: nodes.entrySet())
				diffsum += Math.abs((cur_ranks.get(e.getKey()) - new_ranks.get(e.getKey())) / cur_ranks.get(e.getKey()));
			
			System.out.println(diffsum + ", " + CONVERGENCE_THRESHOLD * num_nodes);
			if(diffsum < CONVERGENCE_THRESHOLD * num_nodes) {
				converged = true;
				System.out.println("Converged!");
			}
			
			// update page ranks for next iteration
			cur_ranks = new HashMap<>(new_ranks);
			new_ranks.clear();
		}
		
		return cur_ranks;
	}
	
	
	// Tests
	public static void main(String[] args) throws IOException {
		System.out.println("test");
		
		Node[] nodes = new Node[8];
		BufferedReader br = new BufferedReader(new FileReader("nodes_simple_test1.txt"));  
		String line = null;
		int idx = 0;
		while ((line = br.readLine()) != null){
			if (line != null && !line.equals("")){
				String[] mySplit = line.toString().split("\t");
				if (mySplit.length>=1){
					nodes[idx] = new Node(mySplit[1]);
					idx++;
				}
			}
		}
		br.close();
		
		HashMap<Integer, Node> node_map = new HashMap<Integer, Node>();
		for (Node n: nodes) {
			System.out.println(n.toString());
			node_map.put(n.Id, n);
		}
		HashMap<Integer, Float> ranks2 = pagerankBoundaries(node_map, node_map);
		System.out.println(ranks2);
		
		//try to sum
		float sum = 0;
    	for(Entry<Integer, Float> entry : ranks2.entrySet()) {
    		sum += entry.getValue();
    	}
    	System.out.println("sum= "+sum);

	}
	
}
