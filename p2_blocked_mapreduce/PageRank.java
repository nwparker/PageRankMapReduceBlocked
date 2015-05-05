import java.util.ArrayList;
import java.util.HashMap;

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
				int num_outgoing = n.outgoing.size();

				// add rank values from incoming boundary edges
				for(float val: n.incoming) {
					new_ranks.put(n.Id, new_ranks.get(n.Id) + val);
				}
				
				// propagate rank. if no outgoing edges, distribute evenly to all nodes
				if (num_outgoing > 0) {
					for(int target_id: n.outgoing) {
						new_ranks.put(target_id, new_ranks.get(target_id) + (float)(DAMPING_FACTOR * cur_ranks.get(n.Id) / num_outgoing));
					}
				} else {
					for(Node n2: nodes) {
						new_ranks.put(n2.Id, new_ranks.get(n2.Id) + (float)(DAMPING_FACTOR * cur_ranks.get(n.Id) / num_nodes));
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
	public static void main(String[] args) {
		Node[] nodes = new Node[5];
		for(int i = 1; i < 4; i++) {
			ArrayList<Integer> outgoing = new ArrayList<Integer>();
			outgoing.add(0);
			outgoing.add(i + 1);
			nodes[i] = new Node(i, outgoing, null);
		}
		ArrayList<Integer> outgoing = new ArrayList<Integer>();
		outgoing.add(4);
		nodes[0] = new Node(0, outgoing, null);
		nodes[4] = new Node(4);
		
		Node[] nodes2 = new Node[3];
		ArrayList<Integer> outgoing2 = new ArrayList<Integer>();
		ArrayList<Float> incoming2 = new ArrayList<Float>();
		outgoing2.add(0);
		incoming2.add(0.3f);
		nodes2[0] = new Node(0);
		nodes2[1] = new Node(100, outgoing2, incoming2);
		nodes2[2] = new Node(2, outgoing2, incoming2);
		
		nodes2 = initializeRanks(nodes2);
		
		HashMap<Integer, Float> ranks2 = pagerankBoundaries(nodes2);
		System.out.println(ranks2);
	}
	
}
