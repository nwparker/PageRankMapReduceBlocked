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
	public static HashMap<Integer, Float> pagerankBoundaries(Node[] nodes) {
		int num_nodes = nodes.length;
		HashMap<Integer, Float> cur_ranks = new HashMap<>();
		HashMap<Integer, Float> new_ranks = new HashMap<>();
		boolean converged = false;
		
		for(Node n: nodes) {
			cur_ranks.put(n.Id, n.rank);
		}
		
		int num_iterations = 0;
		while(!converged && (num_iterations < MAX_ITERATIONS || MAX_ITERATIONS == -1)) {
			num_iterations++;
			
			// initialize new ranks to damping/N
			for(Node n: nodes) {
				new_ranks.put(n.Id, (float) (1 - DAMPING_FACTOR)/num_nodes);
			}
			
			// Iterate through nodes and update ranks
			for(Node n: nodes) {
				int num_outgoing = n.outgoing.length;

				// add rank values from incoming boundary edges
				for(float val: n.incoming) {
					new_ranks.put(n.Id, new_ranks.get(n.Id) + val);
				}
				
				// propagate rank. if no outgoing edges, distribute evenly to all nodes
				float out_flow;
				if (num_outgoing > 0) {
					for(int target_id: n.outgoing) {
						out_flow = (float)(DAMPING_FACTOR * cur_ranks.get(n.Id) / num_outgoing);
						new_ranks.put(target_id, new_ranks.get(target_id) + out_flow);
					}
				} else {
					for(Node n2: nodes) {
						out_flow = (float)(DAMPING_FACTOR * cur_ranks.get(n.Id) / num_nodes);
						new_ranks.put(n2.Id, new_ranks.get(n2.Id) + out_flow);
					}
				}
			}
			
			System.out.println("Iteration: " + num_iterations);
			System.out.println("Ranks: " + new_ranks);

			// check convergence
			float diffsum = 0;

			for(Node n: nodes) {
				// add the percent change from the original rank
				diffsum += Math.abs((cur_ranks.get(n.Id) - new_ranks.get(n.Id)) / cur_ranks.get(n.Id));
			}
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
	
	public static Node[] initializeRanks(Node[] nodes) {
		for(Node n: nodes) {
			n.rank = 1.0f / nodes.length;
		}
		return nodes;
	}
	
	// Tests
	public static void main(String[] args) throws IOException {
		System.out.println("test");
		
		Node[] nodes = new Node[685013];
		BufferedReader br = new BufferedReader(new FileReader("nodes_simple.txt"));  
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
		
		HashMap<Integer, Float> ranks2 = pagerankBoundaries(nodes);
		System.out.println(ranks2);
		
		//try to sum
		float sum = 0;
    	for(Entry<Integer, Float> entry : ranks2.entrySet()) {
    		sum += entry.getValue();
    	}
    	System.out.println("sum= "+sum);

	}
	
}
