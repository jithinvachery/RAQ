import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

public class TraditionalQuery {
	/**
	 * Find all the results  with in the range 0 to rangeThreshold, we do a DFS search
	 * @param rangeThreshold : the range query value
	 * @param queryGraph
	 * @param beamWidth : a parameter used in beam search
	 * @param targetGraph
	 * @param excludeQuery : The search will exclude query graph 
	 * @param showProgress
	 * @return
	 */
	static <T1 extends NodeFeatures>
	ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> 
	GetTopSubgraphs (double rangeThreshold, Graph<T1> queryGraph,
			Graph<T1> targetGraph, boolean excludeQuery,
			boolean showProgress) {
		System.out.println("    ");
		
		return new TraditionalSearch <T1>(queryGraph,
				targetGraph, showProgress, excludeQuery).GetTopSubgraphs(rangeThreshold);
	}

	public static <T1 extends NodeFeatures>
	ArrayList<Helper.ObjectDoublePair<
	ArrayList<Graph<T1>.Node>>>
	GetTopKSubgraphsCGQHierarchicalExcludingQueryAndFilterNode(int k,
			Graph<T1> queryGraph, Graph<T1> targetGraph,
			boolean excludeQuery, boolean showProgress) {
		System.out.println("    ");
		
		return new TraditionalSearch <T1>(queryGraph,
				targetGraph, showProgress, excludeQuery).GetTopKSubgraphs(k);
	}
	
	/**
	 * The worker class
	 * @author jithin
	 *
	 * @param <T1>
	 * @param <T2>
	 */
	static class TraditionalSearch <T1 extends NodeFeatures> {
		final Graph<T1> targetGraph;
		final Graph<T1> queryGraph;
		final ArrayList <Graph<T1>.Edge> queryEdges; //order in which edges to be processed
		final int numQueryEdges;

		PriorityQueue<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> SolutionPQ;
		KPriorityQueue<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> SolutionPQK;
		
		final boolean diGraph;
		final boolean showProgress;

		//exclude the query nodes from target graph
		final boolean excludeQuery;
		HashSet<Graph<T1>.Node> excludeNodes;
		
		//The mapping
		final HashMap<Graph<T1>.Node, Graph<T1>.Node> qNodeToTNodeMap;

		//the distance of current mapping
		double distance;
		
		//current range threshold
		double rangeThreshold;
		//TOPK
		int K;
		boolean performTopK;
		double worstDistance;
		
		/**
		 * Constructor
		 * @param queryGraph
		 * @param targetGraph
		 * @param showProgress
		 * @param excludeQuery
		 * @param beamWidth
		 */
		public TraditionalSearch (Graph<T1> queryGraph,
				Graph<T1> targetGraph, boolean showProgress,
				boolean excludeQuery) {
			this.queryGraph 	= queryGraph;
			this.showProgress 	= showProgress;
			this.targetGraph	= targetGraph;
			this.excludeQuery 	= excludeQuery;
			
			queryEdges 			= Helper.BestEdgeForProcessing (queryGraph.edgeSet);
			numQueryEdges 		= queryEdges.size();
			
			diGraph				= queryGraph.DiGraph;
			
			qNodeToTNodeMap		= new HashMap<>();
			
			if (excludeQuery) {
				excludeNodes = new HashSet<>();
				
				//find the nodes in target graph to be excluded
				for (Graph<T1>.Node tNode : targetGraph.nodeList) {
					if (queryGraph.NodeTouched(tNode)) {
						excludeNodes.add(tNode);
					}
				}
			}
		}
		
		public ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>>
		GetTopKSubgraphs(int k) {
			this.rangeThreshold = 1.0;
			this.performTopK 	= true;
			this.K 				= k;
			this.worstDistance 	= 1.0;
			this.SolutionPQK	= new KPriorityQueue<>(k, false);
			
			return GetTopSubgraphsHelper();
		}

		/**
		 * Find all the results with in the range 0 to rangeThreshold
		 **/
		ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> GetTopSubgraphs (double rangeThreshold) {
			this.rangeThreshold = rangeThreshold;
			this.performTopK= false;
			this.K 			= 0;
			this.SolutionPQ	= new PriorityQueue<>();

			return GetTopSubgraphsHelper();
		}

		ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> GetTopSubgraphsHelper () {			
			if (showProgress)
				System.out.print("              ");

			//add all edges to the partial results
			Graph<T1>.Edge qEdge = queryEdges.get(0);
			
			int i=0;
			
			for (Graph<T1>.Edge tEdge : targetGraph.edgeSet){
				i++;
				
				ShowProgress.ShowLongSample(i);
				
				if (excludeQuery) {
					//see if the edge is overlapping with our query
					if (excludeNodes.contains(tEdge.node1) ||
							excludeNodes.contains(tEdge.node2)) {
						//we cannot use this edge
						continue;
					}
				}
				//at this point the mapping should be empty
				if (qNodeToTNodeMap.size() > 0) {
					System.err.println("The mapping is not empty : Traditional Query.java");
					System.exit(-1);
				}
				
				//update distance
				distance  = qEdge.node1.Distance(tEdge.node1);
				distance += qEdge.node2.Distance(tEdge.node2);
				
				if (distance <= rangeThreshold) {
					//this is a valid growth
					
					//add a straight map
					qNodeToTNodeMap.put(qEdge.node1, tEdge.node1);
					qNodeToTNodeMap.put(qEdge.node2, tEdge.node2);

					//Perform DFS
					if(DFS(1))
						break; // we can terminate the search
					
					//remove the mapping
					qNodeToTNodeMap.remove(qEdge.node1);
					qNodeToTNodeMap.remove(qEdge.node2);
				}
				
				//if undirected graph create a partial solution non-straight
				if (!diGraph) {
					//update distance
					distance  = qEdge.node1.Distance(tEdge.node2);
					distance += qEdge.node2.Distance(tEdge.node1);
					
					if (distance <= rangeThreshold) {
						//this is a valid growth

						//add a non-straight map
						qNodeToTNodeMap.put(qEdge.node1, tEdge.node2);
						qNodeToTNodeMap.put(qEdge.node2, tEdge.node1);

						//Perform DFS
						if(DFS(1))
							break; // we can terminate the search
						
						//remove the mapping
						qNodeToTNodeMap.remove(qEdge.node1);
						qNodeToTNodeMap.remove(qEdge.node2);
					}
				}
			}
			
			//return the solution
			ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> ret;
			
			if (performTopK) {
				ret = SolutionPQK.toArrayListElements();
			} else {
				ret = new ArrayList<>(SolutionPQ.size());
				Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>> sol;
				while ((sol = SolutionPQ.poll()) != null) {
					ret.add(sol);
				}
			}
			
			return ret;
		}

		/**
		 * Perform DFS
		 * @param numEdgesMapped
		 * @return true is we can terminate the search
		 */
		boolean DFS (int numEdgesMapped) {
			boolean ret = false;
			if (numEdgesMapped == numQueryEdges) {
				//we have a valid solution to be added to solution PQ
				ArrayList<Graph<T1>.Node> mapping = new ArrayList<>();
				for (Graph<T1>.Node qNode : queryGraph.nodeList) {
					Graph<T1>.Node tNode = qNodeToTNodeMap.get(qNode);
					mapping.add(tNode);
				}
				
				Helper.ObjectDoublePair	<ArrayList<Graph<T1>.Node>> map =
						new Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>(mapping, distance);
				
				if (performTopK) {
					SolutionPQK.add(map, distance);
					rangeThreshold = SolutionPQK.LeastValueK();
				} else {
					SolutionPQ.add(map);

					ShowProgress.ShowLong(SolutionPQ.size());

					//we are looking for a max of 10k results
					if(SolutionPQ.size() >= 10000)
						ret = true;
				}
			} else {
				//we need to perform the DFS
				Graph<T1>.Edge qEdge = queryEdges.get(numEdgesMapped);

				Graph<T1>.Node qNode1 = qEdge.node1;
				Graph<T1>.Node qNode2 = qEdge.node2;
				
				//which of its node is already mapped?
				// note, this is a valid question because of the ordering of edges we have adopted
				Graph<T1>.Node tNode1 = qNodeToTNodeMap.get(qNode1);
				Graph<T1>.Node tNode2 = qNodeToTNodeMap.get(qNode2);
				
				if (tNode1 != null) {
					//node1 of qEdge has been mapped
					if (tNode2 != null) {
						//node2 of qEdge is also mapped
						//this means we should have edge between tNode1 and tNode2
						Graph<T1>.Edge tEdge = tNode1.GetEdgeTo(tNode2);
						if (tEdge == null) {
							//this is not a valid subgraph growth
							//update distance
							distance += 1.0;
							ret = DFS (numEdgesMapped+1);
							distance -= 1.0;
						} else {
							//we can go deeper into DFS
							ret = DFS (numEdgesMapped+1);
						}
					} else {
						//we have an outgoing (in case of Digraph) edge to handle
						//expand all the out going edges
						if (diGraph) {
							for (Graph<T1>.Edge tEdge : tNode1.edges) {
								Graph<T1>.Node tNode = tEdge.destination;

								ret = DFSRecursor(numEdgesMapped, qNode2, tNode);
								if(ret)
									break;
							}
						} else {
							for (Graph<T1>.Edge tEdge : tNode1.edges) {
								//find the tnode to map to
								Graph<T1>.Node tNode;

								if (tEdge.node1 == tNode1)
									//we need to map to the other node
									tNode = tEdge.node2;
								else
									tNode = tEdge.node1;

								ret = DFSRecursor(numEdgesMapped, qNode2, tNode);
								if(ret)
									break;
							}
						}
					}
				} else {
					//now tNode2 != null
					//we have an incoming (in case of Digraph) edge to handle
					if (diGraph) {
						//process incoming edges
						//we will map the qEdge to each of the incoming edges
						for (Graph<T1>.Edge tEdge : tNode2.incomingEdges) {
							Graph<T1>.Node tNode = tEdge.source;

							ret = DFSRecursor(numEdgesMapped, qNode2, tNode);
							if(ret)
								break;
						}
					} else {
						for (Graph<T1>.Edge tEdge : tNode2.edges) {
							//find the tnode to map to
							Graph<T1>.Node tNode;

							if (tEdge.node1 == tNode2)
								//we need to map to the other node
								tNode = tEdge.node2;
							else
								tNode = tEdge.node1;
							
							ret = DFSRecursor(numEdgesMapped, qNode1, tNode);
							if(ret)
								break;
						}
					}
				}
			}
			
			return ret;
		}

		/**
		 * A helper function to perform recursion
		 * @param numEdgesMapped
		 * @param qNode
		 * @param tNode
		 * @return true is we can terminate the search
		 */
		private boolean DFSRecursor(int numEdgesMapped, Graph<T1>.Node qNode,
				Graph<T1>.Node tNode) {
			boolean ret 	= false;
			boolean addMap 	= true;

			if (excludeQuery)
				if (excludeNodes.contains(tNode))
					addMap = false;
			
			if (addMap) {
				double dist;
				dist = qNode.Distance(tNode);

				//update distance
				distance += dist;

				if (distance < rangeThreshold) {
					//add map
					qNodeToTNodeMap.put(qNode, tNode);

					//we can go deeper into DFS
					ret = DFS (numEdgesMapped+1);

					//remove map
					qNodeToTNodeMap.remove(qNode);
				} else {
					//discard the growth
				}

				//reduce distance
				distance -= dist;
			}
			
			return ret;
		}
	}	
}
