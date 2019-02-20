import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * All the get top-K code is here
 * @author jithin
 *
 */
public class IndexQuery extends Helper {
	
	/**
	 * Function to find the Top-K subgraphs in the Target graph 
	 * @param K
	 * @param queryGraph
	 * @param targetGraph
	 * @param mNodesOfGraph
	 * @return ArrayList of topK subgraphs
	 */
	static <T1 extends NodeFeatures>
	ArrayList<ObjectDoublePair<HashSet<Graph<T1>.Node>>> 
	GetAbsoluteTopKSubgraphs(int K,
			Graph<T1> queryGraph,
			Graph<T1> targetGraph,
			HashSet<HashSet<Graph<T1>.Node>> mNodesOfGraph) {
		
		KPriorityQueue<HashSet<Graph<T1>.Node>> kPQ = 
				new KPriorityQueue<>(K, false, 0.0);
		
		int a=1;
		int b=mNodesOfGraph.size();
		
		
		for (HashSet<Graph<T1>.Node> nodeSet : mNodesOfGraph) {
			kPQ.add(nodeSet, RAQ.CGQDistGraphAndSubgraph(queryGraph, targetGraph, nodeSet));
			if (kPQ.ThresholdAchieved()) {
				ShowProgress.ShowPercent(b,b);
				break;
			}
			ShowProgress.ShowPercent(a++, b);
		}
		System.out.println();
		
		ArrayList<HashSet<Graph<T1>.Node>> topK = kPQ.toArrayListElements();
		ArrayList<Double> 				 topKValues	= kPQ.toArrayListValues();
		//sanity
		//all should be different sets
		/*
		for (int j=0; j<K; j++){
			HashSet<Graph<T1>.Node> set1 = topK.get(j);
			for (int l=j+1; l<K; l++){
				HashSet<Graph<T1>.Node> set2 = topK.get(l);
				
				HashSet<Graph<T1>.Node> temp = new HashSet<>(set1);
				temp.addAll(set2);
				
				if (temp.size() == queryGraph.SizeNodes()){
					System.err.println("Fatal error : our two sets are the same");
					System.exit(-1);
				}
			}
		}
		 */
		ArrayList<ObjectDoublePair<HashSet<Graph<T1>.Node>>> ret = new ArrayList<>();
		for (int i=0; i<topK.size(); i++) {
			HashSet<Graph<T1>.Node> element = topK.get(i);
			Double value 						= topKValues.get(i);
			
			ObjectDoublePair<HashSet<Graph<T1>.Node>> o = 
					new ObjectDoublePair<HashSet<Graph<T1>.Node>>(element, value);
			
			ret.add(o);
		}
		return ret;
	}
}


class ModifiedTA <T1 extends NodeFeatures> {	
	Graph<T1> queryGraph;
	int K;
	ArrayList <Graph<T1>.Edge> 					queryEdges; //order in which edges to be processed
	KPriorityQueue<ArrayList<Graph<T1>.Node>>	kPQ;
	HashMap<Graph<T1>.Node, Graph<T1>.Node> qNodeToTNodeMap;
	Double 	distance;
	boolean useTa;
	boolean baseCase;
	boolean gatherStat;
	Helper.CallByReference<Long> numEdgesTouched;
	Helper.CallByReference<Long> numDFS;
	boolean fuzzy;

	private void init (int K,
			Graph<T1> queryGraph,
			boolean useTa,
			boolean baseCase, 
			Helper.CallByReference<Integer> depth,
			boolean fuzzy) {
		this.K = K;
		this.queryGraph = queryGraph;
		
		kPQ = new KPriorityQueue<>(K, false, 0);

		//Best edge from which to start processing
		queryEdges = Helper.BestEdgeForProcessing (queryGraph.edgeSet);
		
		qNodeToTNodeMap = new HashMap<>();
		this.useTa 		= useTa;
		this.baseCase 	= baseCase;
		
		if (depth != null) {
			gatherStat = true;
		} else {
			gatherStat = false;
		}
		
		this.fuzzy = fuzzy;
	}
	
	
	/* Function to find the Top-K subgraphs in the Target graph Using Threshold Algorithm
	 * @param K
	 * @param queryGraph
	 * @param taIndex
	 * @return the top -k isomorphic subgraphs and their distances
	 */
	public ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>>
	GetTopKSubgraphs (int K,
			Graph<T1> queryGraph,
			Graph<T1> targetGraph,
			boolean showProgress,
			Helper.CallByReference<Integer>	depth,
			Helper.CallByReference<Long>   	numDFS,
			Helper.CallByReference<Long> 	numEdgesTouched,
			boolean useTA,
			boolean baseCase,
			boolean fuzzy) {

		int a = 1;
		int b = targetGraph.edgeSet.size();
		init (K, queryGraph, useTA, baseCase, depth, fuzzy);

		for (int numEdgesMapped = 0; (fuzzy && (numEdgesMapped < queryGraph.SizeEdges())); numEdgesMapped++) {
			double dist = numEdgesMapped * RAQ.penaltyDist;
			if (dist >= kPQ.LeastValueK()) {
				// we have found the topK
				break;
			}
			//initiate global taIndex based on this
			Graph<T1>.Edge qEdge = queryEdges.get(numEdgesMapped);
			Iterator<Graph<T1>.Edge> it;
			it = targetGraph.edgeSet.iterator();

			if (gatherStat) {
				numEdgesTouched.element = 0L;
				this.numEdgesTouched 	= numEdgesTouched;
				numDFS.element 			= 0L;
				this.numDFS				= numDFS;
			}

			while (it.hasNext()) {
				Graph<T1>.Edge tEdge = it.next();

				if (StartDFSForSubgraph (qEdge, tEdge, queryGraph.DiGraph, numEdgesMapped)) {
					//we have achieved threshold of KPQ
					break;
				}

				a++;
				a++;
				if (showProgress)
					//ShowProgress.ShowPercentFlush(a++, b);
					ShowProgress.ShowLong(a, b);
			}

		}
		//if (showProgress)
			//ShowProgress.ShowPercentFlush(b, b);

		if (showProgress)
			System.out.println();
		
		if (gatherStat)
			depth.element = a;
		return kPQ.toArrayListObjectDoublePair();
	}

	/**
	 * Function to populate kPQ
	 * @param tEdge
	 * @param diGraph 
	 * @return true if top-K found
	 */

	private boolean StartDFSForSubgraph (Graph<T1>.Edge qEdge, Graph<T1>.Edge tEdge,
				boolean diGraph, int numEdgesMapped) {
		boolean ret;
				
		if (diGraph) {
			ret = StartDirectedDFSForSubgraph (qEdge, tEdge, numEdgesMapped+1);
			
		} else {
			//undirected graph, the edges can be mapped in two ways
			ret = StartUnDirectedDFSForSubgraph (qEdge, tEdge, true, numEdgesMapped+1);
			if (!ret) {
				//we have not yet found the top K
				ret = StartUnDirectedDFSForSubgraph (qEdge, tEdge, false, numEdgesMapped+1);
			}
		}
		return ret;
	}

	private boolean StartDirectedDFSForSubgraph (Graph<T1>.Edge qEdge, Graph<T1>.Edge tEdge,
			int numEdgesMapped) {
		if (gatherStat)
			numDFS.element++;
		qNodeToTNodeMap.put(qEdge.source, 		tEdge.source);
		qNodeToTNodeMap.put(qEdge.destination, 	tEdge.destination);

		distance = qEdge.CGQDistEdges(tEdge);

		boolean ret =  DFSForSubgraph (numEdgesMapped, true);

		qNodeToTNodeMap.remove(qEdge.source);
		qNodeToTNodeMap.remove(qEdge.destination);
		
		return ret;
	}

	private boolean StartUnDirectedDFSForSubgraph (Graph<T1>.Edge qEdge, Graph<T1>.Edge tEdge, boolean straight,
			int numEdgesMapped) {
		if (straight) {
			if (gatherStat)
				numDFS.element++;
			qNodeToTNodeMap.put(qEdge.node1, tEdge.node1);
			qNodeToTNodeMap.put(qEdge.node2, tEdge.node2);
		} else {
			if (gatherStat)
				numDFS.element++;
			qNodeToTNodeMap.put(qEdge.node1, tEdge.node2);
			qNodeToTNodeMap.put(qEdge.node2, tEdge.node1);
		}
		distance = qEdge.CGQDistEdges(tEdge);

		boolean ret =  DFSForSubgraph (numEdgesMapped, false);

		qNodeToTNodeMap.remove(qEdge.node1);
		qNodeToTNodeMap.remove(qEdge.node2);
		
		return ret;
	}

	private boolean DFSForSubgraph (int numEdgesMapped, boolean diGraph) {
		if (RAQ.interruptable)
			if (InterruptSearchSignalHandler.Interrupt())
				return true;
		
		boolean ret = false;
		//ShowProgress.ShowInt(numEdgesMapped, 10000000);
		//counting how many time we go into searching 
		if (gatherStat)
			numEdgesTouched.element++;
		
		if (distance >= kPQ.LeastValueK()) {
			//we can prune this growth
		} else {
			if (numEdgesMapped == queryEdges.size()) {
				if (fuzzy) {
					// we need to do a second pass through all the query edges
					for (Graph<T1>.Edge qEdge : queryEdges) {
						//this is useless statement to avoid warning
						if(qEdge == null)
							break;
						//which of its node is already mapped?
						//TODO
						//We will get a runtime any way
					}
				}
				//we have a valid mapping
				//add the mapping to result.
				ArrayList<Graph<T1>.Node> mapping = new ArrayList<>();
				for (Graph<T1>.Node qNode : queryGraph.nodeList) {
					Graph<T1>.Node tNode = qNodeToTNodeMap.get(qNode);
					if (!fuzzy) {
						if (tNode == null) {
							System.err.println("DFSForSubgraph : tNode = null? mapping is missing");
							System.exit(-1);
						}
					} else {
						//do nothing
					}
					mapping.add(tNode);
				}
				double dist = distance;
				
				if (RAQ.slowAndSafe)
					if (dist > numEdgesMapped) {
						//How can this be ?????
						System.err.println("INdexQuery:DFSForSubgraph : dist > numEdgesMapped");
						System.exit(-1);
					}
					
					
				kPQ.add(mapping, dist);
				ret =  kPQ.ThresholdAchieved();
			} else {
				//the query edge to be mapped is
				Graph<T1>.Edge qEdge = queryEdges.get(numEdgesMapped);

				//which of its node is already mapped?
				// note, this is a valid question because of the ordering of edges we have adopted
				Graph<T1>.Node tNode1 = qNodeToTNodeMap.get(qEdge.node1);
				Graph<T1>.Node tNode2 = qNodeToTNodeMap.get(qEdge.node2);

				if (tNode1 != null) {
					//node1 of qEdge has been mapped
					if (tNode2 != null) {
						//node2 of qEdge is also mapped
						//this means we should have edge between tNode1 and tNode2
						Graph<T1>.Edge tEdge = tNode1.GetEdgeTo(tNode2);
						if (tEdge == null) {
							if (fuzzy) {
								distance += RAQ.penaltyDist;
								ret = DFSForSubgraph (numEdgesMapped + 1, diGraph);
								distance -= RAQ.penaltyDist;
							} else {
								//this is not a valid subgraph growth
								return false;
							}
						} else {
							Double dist = qEdge.CGQDistEdges( tEdge);
							distance += dist;
							ret = DFSForSubgraph (numEdgesMapped + 1, diGraph);
							distance -= dist;
						}
					} else {
						//we have an outgoing (in case of Digraph) edge to handle
						Iterator<Graph<T1>.Edge> it;
						it = tNode1.edges.iterator();

						boolean flag = false;
						while (it.hasNext()) {
							flag = true;
							if (gatherStat)
								numDFS.element++;
			
							Graph<T1>.Edge tEdge = it.next();
							
							ret = Recursor(qEdge.node2, tEdge.node2, qEdge, tEdge, numEdgesMapped, diGraph);

							if (ret)
								break;
						}
						//we have counted one extra
						if (flag)
							if (gatherStat)
								numDFS.element--;
					}
				} else if (tNode2 != null) {
					// now we can be sure (tNode2 != null)
					//we have an incoming (in case of Digraph) edge to handle
					Iterator<Graph<T1>.Edge> it;
					if (diGraph) {
						it = tNode2.incomingEdges.iterator();
					} else {
						it = tNode2.edges.iterator();
					}
					boolean flag = false;
					while (it.hasNext()) {
						flag = true;
						if (gatherStat)
							numDFS.element++;
						
						Graph<T1>.Edge tEdge = it.next();
					
						ret = Recursor (qEdge.node1, tEdge.node1, qEdge, tEdge, numEdgesMapped, diGraph);
						
						if (ret)
							break;
					}
					//we have counted one extra
					if (flag)
						if (gatherStat)
							numDFS.element--;	
				} else if (fuzzy){
					//this edge need to be processed in the second pass
				}
			}
		}
		
		return ret;
	}

	private boolean Recursor (Graph<T1>.Node qNode,
			Graph<T1>.Node tNode,
			Graph<T1>.Edge qEdge,
			Graph<T1>.Edge tEdge,
			int numEdgesMapped,
			boolean diGraph) {
		boolean ret = false;
		boolean condition = false;
		
		if (baseCase) {
			condition = (qNode.Degree() <= tNode.Degree());
			if (diGraph)
				condition = condition && (qNode.InDegree() <= tNode.InDegree());
		} else {
			condition = qNode.ValidDegreeDist(tNode);
		}
		
		if (condition && (!qNodeToTNodeMap.containsKey(qNode))) {
			qNodeToTNodeMap.put(qNode, tNode);
			Double dist = qEdge.CGQDistEdges( tEdge);
			distance += dist;
			ret = DFSForSubgraph (numEdgesMapped + 1, diGraph);
			distance -= dist;
			qNodeToTNodeMap.remove(qNode);
		}
		
		return ret;
	}
}

class CGQHierarchicalIndexQuery <T1 extends NodeFeatures> {
	Graph<T1> queryGraph;
	ArrayList <Graph<T1>.Edge> 	queryEdges; //order in which edges to be processed
	KPriorityQueue<ArrayList<Graph<T1>.Node>>	kPQ;
	HashMap<Graph<T1>.Node, Graph<T1>.Node> qNodeToTNodeMap;

	int K;
	Double 	distance;
	Helper.CallByReference<Integer> depth;
	Helper.CallByReference<Long> numDFS;
	Helper.CallByReference<Long> numEdgesTouched;

	boolean showProgress;
	boolean diGraph;
	int targetGraphNumEdges;
	boolean fuzzy;
	// edges which are to be reprocessed
	ArrayList <Graph<T1>.Edge> 	queryEdgesFuzzy; 
	/**
	 * Function to find the Top-K subgraphs in the Target graph Using CGQIndex
	 * @param K
	 * @param queryGraph
	 * @param targetGraph
	 * @param cgqIndex
	 * @param showProgress
	 * @param depth
	 * @param numDFS
	 * @param numEdgesTouched
	 * @param fuzzy 
	 * @return
	 */
	public ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>>
	GetTopKSubgraphs (int K,
			Graph<T1> queryGraph,
			Graph<T1> targetGraph,
			CGQHierarchicalIndex<T1> cgqIndex,
			boolean showProgress,
			Helper.CallByReference<Integer>depth,
			Helper.CallByReference<Long>   numDFS,
			Helper.CallByReference<Long>   numEdgesTouched,
			boolean fuzzy) {
		
		this.K = K;
		this.queryGraph = queryGraph;
		this.showProgress = showProgress;
		this.fuzzy = fuzzy;				
		
		kPQ = new KPriorityQueue<>(K, false, 0);

		//Best edge from which to start processing
		queryEdges = Helper.BestEdgeForProcessing (queryGraph.edgeSet);
		
		qNodeToTNodeMap = new HashMap<>();
				
		numEdgesTouched.element = 0L;
		this.numEdgesTouched 	= numEdgesTouched;
		numDFS.element 			= 0L;
		this.numDFS				= numDFS;
		this.depth				= depth;

		diGraph	= queryGraph.DiGraph;
			
		targetGraphNumEdges = targetGraph.edgeSet.size();
		distance = 0.0;
		StartDFS (cgqIndex, queryGraph.weights);

		if (showProgress)
			System.out.println();
		
		return kPQ.toArrayListObjectDoublePair();
	}
	

	/**
	 * lets begin it
	 * @param cgqIndex
	 * @param diGraph
	 * @param showProgess
	 */
	private void StartDFS (CGQHierarchicalIndex<T1> cgqIndex, double[] weights) {
		int size;
		
		if (fuzzy) {
			size = queryEdges.size();
			queryEdgesFuzzy = new ArrayList<>();
		} else
			size = 1;
		
		//flag to see if we have found the top-K
		boolean ret = false;

		//in case of fuzzy it is possible that the first 
		//edge is not mapped
		for (int i=0; i<size; i++) {
			Graph<T1>.Edge qEdge = queryEdges.get(i);
			
			//first i edges where not mapped hence 
			distance = (double) i*RAQ.penaltyDist;
			
			//the indices of the top weights in the edge
			Integer[] indexList = Helper.GetSortedIndex(weights, 0.75);

			//sort the bins in the main index
			ArrayList<Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>> sortedBins = 
					cgqIndex.GetSortedBins(qEdge);

			for (Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> odBin : sortedBins) {
				//the values are similarities
				if ((distance + 1-odBin.value) > kPQ.LeastValueK()) {
					//we have found the top-K
					//we can break, since the bins are sorted
					break;
				}

				CGQHierarchicalIndex<T1>.Bin bin = odBin.element;

				ret = DFS (i, bin, qEdge, indexList, null, false, true);
				if (ret)
					break;
			}
			if (ret)
				break;
		}
	}


	/**
	 * 
	 * @param numEdgesMapped
	 * @param secondPass : true if we are reprocessing old edges 
	 * @return
	 */
	private boolean StartDFSIntermediate (int numEdgesMapped) {
		boolean ret = false;
		boolean secondPass = false;
		
		if (distance >= kPQ.LeastValueK()) {
			//we can prune this growth
		} else {
			boolean foundAMatch=false;
			
			if (fuzzy) {
				if (numEdgesMapped == (queryEdges.size() + queryEdgesFuzzy.size()))
					foundAMatch = true;
				else if ((numEdgesMapped >= queryEdges.size()) &&
						(queryEdgesFuzzy.size() > 0)) {
					//we need to do a second pass
					secondPass = true;
				}
			} else {
				if (numEdgesMapped == queryEdges.size())
					foundAMatch = true;
			}

			if  (foundAMatch) {
				//we have a valid mapping
				//add the mapping to result.
				ArrayList<Graph<T1>.Node> mapping = new ArrayList<>();
				for (Graph<T1>.Node qNode : queryGraph.nodeList) {
					mapping.add(qNodeToTNodeMap.get(qNode));
				}
				double dist = distance;
				
				if (RAQ.slowAndSafe)
					if (dist > numEdgesMapped) {
						//How can this be ?????
						System.err.println("INdexQuery:StartDFSIntermediate : dist > numEdgesMapped");
						System.exit(-1);
					}					
					
				kPQ.add(mapping, dist);
				ret =  kPQ.ThresholdAchieved();
			} else {
				//the query edge to be mapped is
				Graph<T1>.Edge qEdge;
				if (secondPass)
					qEdge = queryEdges.get(numEdgesMapped - queryEdges.size());
				else
					qEdge = queryEdges.get(numEdgesMapped);

				//which of its node is already mapped?
				// note, this is a valid question because of the ordering of edges we have adopted
				Graph<T1>.Node tNode1 = qNodeToTNodeMap.get(qEdge.node1);
				Graph<T1>.Node tNode2 = qNodeToTNodeMap.get(qEdge.node2);

				if (tNode1 != null) {
					//node1 of qEdge has been mapped
					if (tNode2 != null) {
						//node2 of qEdge is also mapped
						//this means we should have edge between tNode1 and tNode2
						Graph<T1>.Edge tEdge = tNode1.GetEdgeTo(tNode2);
						if (tEdge == null) {
							//this is not a valid subgraph growth
							 if (fuzzy) {
									double dBkp = distance;
									distance += RAQ.penaltyDist;
									ret = StartDFSIntermediate (numEdgesMapped+1);
									distance = dBkp;
								}
						} else {
							numEdgesTouched.element++;
							Double dist = distance;
							distance += qEdge.CGQDistEdges( tEdge);
							ret = StartDFSIntermediate (numEdgesMapped+1);
							distance = dist;
						}
					} else {
						//we have an outgoing (in case of Digraph) edge to handle
						//we need work on the bins
						ret = DFSIntoBin (numEdgesMapped, qEdge, qEdge.node2,
								true, tNode1.cgqHierarchicalIndex, queryGraph.weights);
					}
				} else if (tNode2 != null) {
					//we have an incoming (in case of Digraph) edge to handle
					if (diGraph) {
						ret = DFSIntoBin (numEdgesMapped, qEdge, qEdge.node1,
								false, tNode2.incomingCGQHierarchicalIndex, queryGraph.weights);
					} else {
						ret = DFSIntoBin (numEdgesMapped, qEdge, qEdge.node1,
								false, tNode2.cgqHierarchicalIndex, queryGraph.weights);
					}
				} else if (fuzzy && !secondPass){
					queryEdgesFuzzy.add(qEdge);
					ret = StartDFSIntermediate (numEdgesMapped+1);
				}
			}
		}

		return ret;
	}
	
	boolean DFSIntoBin (int numEdgesMapped, Graph<T1>.Edge qEdge,
			Graph<T1>.Node qNode, boolean secondNode,
			CGQHierarchicalIndex<T1> cgqHierarchicalIndex,
			double[] weights) {
		//the indices of the top weights in the edge
		Integer[] indexList = Helper.GetSortedIndex(weights, 0.75);

		//sort the bins in the main index
		ArrayList<Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>> sortedBins = 
				cgqHierarchicalIndex.GetSortedBins(qEdge);

		//flag to see if we have found the top-K
		boolean ret = false;

		//FIXME we do not need this sorting
		for (Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> odBin : sortedBins) {
			
			CGQHierarchicalIndex<T1>.Bin bin = odBin.element;
			ret = DFS (numEdgesMapped, bin, qEdge, indexList, qNode, secondNode, false);
			
			if (ret) {
				//we have found the top-k
				break;
			}
		}
		
		return ret;
	}
	
	private boolean DFS (int numEdgesMapped, CGQHierarchicalIndex<T1>.Bin bin,
			Graph<T1>.Edge qEdge, Integer []indexList,
			Graph<T1>.Node qNode, boolean secondNode,
			boolean starting) {
		boolean ret = false;
		if (bin.leaf) {
			//we have found a leaf
			if (starting) {
				//this is the first edge to be mapped so
				//needs to be treated differently
				Iterator<Graph<T1>.Edge> it = bin.allEdges.iterator();
				while (it.hasNext() && (!ret)) {
					Graph<T1>.Edge tEdge = it.next();
					double d = qEdge.CGQDistEdges( tEdge);
					depth.element++;
					numDFS.element++;
					numEdgesTouched.element++;
					if (showProgress)
						ShowProgress.ShowLong(depth.element, targetGraphNumEdges);
					
					if (diGraph) {
						//directed graph
						qNodeToTNodeMap.put(qEdge.source, 		tEdge.source);
						qNodeToTNodeMap.put(qEdge.destination, 	tEdge.destination);
						
						double dBkp = distance;
						distance += d;
						ret = StartDFSIntermediate (numEdgesMapped+1);
						distance = dBkp;

						qNodeToTNodeMap.remove(qEdge.source);
						qNodeToTNodeMap.remove(qEdge.destination);
					} else {
						qNodeToTNodeMap.put(qEdge.node1, tEdge.node1);
						qNodeToTNodeMap.put(qEdge.node2, tEdge.node2);
						
						double dBkp = distance;
						distance += d;
						ret = StartDFSIntermediate (numEdgesMapped+1);
						distance = dBkp;

						qNodeToTNodeMap.remove(qEdge.node1);
						qNodeToTNodeMap.remove(qEdge.node2);

						if (!ret) {
							numEdgesTouched.element++;
							depth.element++;
							numDFS.element++;
							if (showProgress)
								ShowProgress.ShowLong(depth.element, targetGraphNumEdges);
							
							qNodeToTNodeMap.put(qEdge.node1, tEdge.node2);
							qNodeToTNodeMap.put(qEdge.node2, tEdge.node1);
							
							dBkp = distance;
							distance += d;
							ret = StartDFSIntermediate (numEdgesMapped+1);
							distance = dBkp;

							qNodeToTNodeMap.remove(qEdge.node1);
							qNodeToTNodeMap.remove(qEdge.node2);
						}
					}
				}
			} else {
				Iterator<Graph<T1>.Edge> it = bin.allEdges.iterator();
				while (it.hasNext() && (!ret)) {
					Graph<T1>.Edge tEdge = it.next();
					numDFS.element++;
					if (secondNode) {
						if ((fuzzy) || (qNode.ValidDegreeDist(tEdge.node2))) {
							numEdgesTouched.element++;
							double d = distance;
							distance += qEdge.CGQDistEdges( tEdge);
							
							qNodeToTNodeMap.put(qNode, tEdge.node2);
							ret = StartDFSIntermediate(numEdgesMapped+1);						
							qNodeToTNodeMap.remove(qNode);
							
							distance = d;
						}
					} else {
						if ((fuzzy) || (qNode.ValidDegreeDist(tEdge.node1))) {
							numEdgesTouched.element++;
							double d = distance;
							distance += qEdge.CGQDistEdges( tEdge);
							
							qNodeToTNodeMap.put(qNode, tEdge.node1);
							ret = StartDFSIntermediate (numEdgesMapped+1);						
							qNodeToTNodeMap.remove(qNode);

							distance = d;
						}
					}
				}
			}
		} else {
			//go deep
			Iterator<CGQHierarchicalIndex<T1>.Bin> it = bin.iterator(qEdge, false);
			
			while (it.hasNext() && (!ret)) {
				CGQHierarchicalIndex<T1>.Bin branch = it.next();
				ret = DFS (numEdgesMapped, branch, qEdge, indexList, qNode, secondNode, starting);
				numDFS.element++;
			}
		}
		
		return ret;
	}
}

