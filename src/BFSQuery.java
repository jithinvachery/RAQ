import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;
import java.util.Stack;

class PartialSolution <T1 extends NodeFeatures>
 implements Comparator<PartialSolution<T1>>, Comparable<PartialSolution<T1>> {
	private double distance;
	private int numEdgesMapped;
	private final LinkedList<Graph<T1>.Edge> queryEdgesFuzzy; //edges to be reprocessed in the case of fuzzy query
	private final HashMap<Graph<T1>.Node, Graph<T1>.Node> qNodeToTNodeMap;
	private final HashSet<Graph<T1>.Node> previouslyMappedTNodes;
	
	PartialSolution (Graph<T1>.Edge qEdge, Graph<T1>.Edge tEdge,
			boolean fuzzy, boolean diGraph, boolean straight, double distance,
			int numEdgesMapped) {
		BFSQuery.CheckDist(distance, numEdgesMapped);
		this.distance 		= distance;
		this.numEdgesMapped = numEdgesMapped;
		qNodeToTNodeMap 	= new HashMap<>();
		if (fuzzy)
			queryEdgesFuzzy = new LinkedList<>();
		else
			queryEdgesFuzzy = new LinkedList<>(); //Initially we were setting it to null, I think is not required.
		
		//tEdge is the first edge we are mapping
		if (diGraph) {
			qNodeToTNodeMap.put(qEdge.source, tEdge.source);
			qNodeToTNodeMap.put(qEdge.destination, tEdge.destination);
		} else {
			if (straight) {
				qNodeToTNodeMap.put(qEdge.node1, tEdge.node1);
				qNodeToTNodeMap.put(qEdge.node2, tEdge.node2);
			} else {
				qNodeToTNodeMap.put(qEdge.node1, tEdge.node2);
				qNodeToTNodeMap.put(qEdge.node2, tEdge.node1);
			}
		}
		
		previouslyMappedTNodes = new HashSet<>();
		previouslyMappedTNodes.add(tEdge.node1);
		previouslyMappedTNodes.add(tEdge.node2);
	}

	/**
	 * check if the maps are same
	 * @param ps
	 * @return
	 */
	public boolean IsSameAs (PartialSolution<T1> ps) {
		boolean ret = true;
		
		if (qNodeToTNodeMap.size() == ps.qNodeToTNodeMap.size()) {
			for (Graph<T1>.Node qNode : qNodeToTNodeMap.keySet()) {
				Graph<T1>.Node tNode1 = qNodeToTNodeMap.get(qNode);
				Graph<T1>.Node tNode2 = ps.qNodeToTNodeMap.get(qNode);
				
				if (tNode1 != tNode2) {
					ret = false;
					break;
				}
			}
		} else {
			ret = false;
		}
		
		return ret;
	}
	
	/**
	 * copy constructor
	 * @param ps
	 */
	public PartialSolution(PartialSolution<T1> ps) {
		BFSQuery.CheckDist(distance, numEdgesMapped);
		distance 		= ps.distance;
		numEdgesMapped 	= ps.numEdgesMapped;
		queryEdgesFuzzy = new LinkedList<>(ps.queryEdgesFuzzy);
		qNodeToTNodeMap = new HashMap<>(ps.qNodeToTNodeMap);
		previouslyMappedTNodes = new HashSet<>(ps.previouslyMappedTNodes);
	}

	/**
	 * find the target node which is mapped to this query node
	 * @param qNode
	 * @return
	 */
	Graph<T1>.Node GetMap (Graph<T1>.Node qNode) {
		return qNodeToTNodeMap.get(qNode);
	}
	
	int NumEdgesMapped () {
		return numEdgesMapped;
	}
	
	@Override
	public int compare(PartialSolution<T1> o1, PartialSolution<T1> o2) {
		int ret = Double.compare(o1.distance, o2.distance);
		
		//larger mappings are preferred
		if (ret == 0)
			ret = -1* Integer.compare(o1.qNodeToTNodeMap.size(), o2.qNodeToTNodeMap.size());
		
		return ret;
	}

	/*
	 * in the mapping we could not find a valid edge so 
	 * increment the distance
	 */
	public void OneEdgeMissed() {
		IncrementDistance(RAQ.penaltyDist);
	}

	/**
	 * An increase in distance and the number of edges mapped
	 * @param dist
	 */
	public void IncrementDistance(Double dist) {
		BFSQuery.CheckDist(dist, 1);
		distance += dist;
		numEdgesMapped++;
	}

	/**
	 * Add a map and increment the distance
	 * @param qNode
	 * @param tNode
	 * @param dist
	 */
	public boolean AddMap(Graph<T1>.Node qNode, Graph<T1>.Node tNode, Double dist) {
		//we need to check if this tNode is used previously
		//if not we can use it
		boolean ret = false;
		
		if (!previouslyMappedTNodes.contains(tNode)) {
			ret = true;
			qNodeToTNodeMap.put(qNode, tNode);
			IncrementDistance (dist);
			previouslyMappedTNodes.add(tNode);
		}
		
		return ret;
	}

	public Double Distance() {
		return distance;
	}

	/**
	 * This edge needs to be processed in the second pass
	 * @param qEdge
	 */
	public void ProcessEdgeLater(Graph<T1>.Edge qEdge) {
		queryEdgesFuzzy.add(qEdge);
		numEdgesMapped++;
	}

	/**
	 * Get the mapping
	 * @param qNode
	 * @return
	 */
	public Graph<T1>.Node GetqNodeToTNodeMap(Graph<T1>.Node qNode) {
		return qNodeToTNodeMap.get(qNode);
	}

	/**
	 * size of queryEdgesFuzzy
	 * @return size of queryEdgesFuzzy
	 */
	public int NumReprocessEdges() {
		return queryEdgesFuzzy.size();
	}

	/**
	 * Get one edge to be reprocessed
	 * @return
	 */
	public Graph<T1>.Edge GetReprocessEdge() {
		return queryEdgesFuzzy.poll();
	}

	@Override
	public int compareTo(PartialSolution<T1> o) {
		return compare(this, o);
	}
	
	/**
	 * How many nodes have been mapped
	 * @return
	 */
	public int Size() {
		return qNodeToTNodeMap.size();
	}
}

/**
 * Partial solution are kept in separate priority queues based on the
 * number of edges matched so far
 * @author jithin
 *
 * @param <T1>
 */
class PartialSolutionPriorityQueue <T1 extends NodeFeatures> {
	final int beamWidth;
	/** list of priority queues */
	private ArrayList<PriorityQueue<PartialSolution<T1>>> pqList;
	
	/**
	 * @param size : number of edges to be mapped in the query graph
	 */
	public PartialSolutionPriorityQueue (int size, int beamWidth) {
		this.beamWidth = beamWidth;
		
		pqList = new ArrayList<>(size-1); //size-1 because the full solution should go to other priority queue
		
		for (int i=0; i<size; i++) {
			PriorityQueue<PartialSolution<T1>> pq = new PriorityQueue<>();
			pqList.add(pq);
		}
	}

	/**
	 * Add the partial solution to the appropriate priority queue
	 * @param ps
	 */
	public void add(PartialSolution<T1> ps) {
		int size = ps.NumEdgesMapped();
		int index = size-1;
		
		PriorityQueue<PartialSolution<T1>> pq = pqList.get(index);
		
		pq.add(ps);
	}

	/**
	 * clear the memory
	 */
	public void clear() {
		for (PriorityQueue<PartialSolution<T1>> pq : pqList) {
			pq.clear();
		}
	}

	/**
	 * is the list empty
	 * @return
	 */
	public boolean isEmpty() {
		boolean ret = true;
		
		for (PriorityQueue<PartialSolution<T1>> pq : pqList) {
			if (pq.isEmpty() == false) {
				ret = false;
				break;
			}
		}
		
		return ret;
	}

	/**
	 * Get the biggest solution and the best from it
	 * @param currentWorstDistance , clear all partial solutions having more than this distance
	 * @return
	 */
	public Stack<PartialSolution<T1>> poll(double currentWorstDistance) {
		PartialSolution<T1> ps = null;
		Stack<PartialSolution<T1>> stack = new Stack<>();
		
		// Generate an iterator. Start just after the last element.
		ListIterator<PriorityQueue<PartialSolution<T1>>> li = pqList.listIterator(pqList.size());

		// Iterate in reverse.
		while(li.hasPrevious() && stack.size() < beamWidth) {
			PriorityQueue<PartialSolution<T1>> pq = li.previous();
			
			while ((!pq.isEmpty()) &&
					(stack.size() < beamWidth)){
				ps = pq.poll();
				
				if (ps.Distance() >= currentWorstDistance) {
					//we can clear this priority queue
					pq.clear();
					ps = null;
				} else {
					stack.add(ps);
				}
			}
		}
		
		return stack;
	}

	public ArrayList<PartialSolution<T1>> getArrayList() {
		ArrayList<PartialSolution<T1>> arr = new ArrayList<>();
		
		// Generate an iterator. Start just after the last element.
		ListIterator<PriorityQueue<PartialSolution<T1>>> li = pqList.listIterator(pqList.size());

		// Iterate in reverse.
		while(li.hasPrevious()) {
			PriorityQueue<PartialSolution<T1>> pq = li.previous();

			arr.addAll(pq);
		}
		
		return arr;
	}
}

public class BFSQuery {
	static final boolean gatherStat = RAQ.gatherStat;
	public static final int HeuristicsNumBin = 10; //the number of bins in which each ve value is divided into
	
	/**
	 * Function uses best first search, in beam format with DFS augmentation
	 * The index used is CGQ hierarchical
	 * @param K
	 * @param queryGraph
	 * @param neGraph
	 * @param neCGQhierarchicalIndex
	 * @param showProgress
	 * @param depth
	 * @param numDFS
	 * @param numEdgesTouched
	 * @param fuzzy
	 * @return
	 */
	static <T1 extends NodeFeatures>
	ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> 
	GetTopKSubgraphsCGQHierarchical (int K, Graph<T1> queryGraph,
			Graph<T1> neGraph, 		CGQHierarchicalIndex<T1> neCGQhierarchicalIndex,
			boolean showProgress, 			Helper.CallByReference<Integer> depth,
			Helper.CallByReference<Long> numDFS,
			Helper.CallByReference<Long> numEdgesTouched, boolean fuzzy,
			boolean avoidTAHeuristics, int beamWidth, int widthThreshold,
			boolean doNotUseIndex) {
		boolean excludeQuery = false;	
		boolean filterNode   = false;
		return GetTopKSubgraphsCGQHierarchicalHelper(K, queryGraph, neGraph,
				neCGQhierarchicalIndex, showProgress, depth, numDFS, numEdgesTouched,
				fuzzy, avoidTAHeuristics, beamWidth, widthThreshold, doNotUseIndex,
				excludeQuery, filterNode);
	}
	
	/**
	 * Function uses best first search, in beam format with DFS augmentation
	 * The index used is CGQ hierarchical
	 * The search will exclude query graph 
	 * @param K
	 * @param queryGraph
	 * @param neGraph
	 * @param neCGQhierarchicalIndex
	 * @param showProgress
	 * @param depth
	 * @param numDFS
	 * @param numEdgesTouched
	 * @param fuzzy
	 * @return
	 */
	static <T1 extends NodeFeatures>
	ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> 
	GetTopKSubgraphsCGQHierarchicalExcludingQuery (int K, Graph<T1> queryGraph,
			Graph<T1> neGraph, 		CGQHierarchicalIndex<T1> neCGQhierarchicalIndex,
			boolean showProgress, 			Helper.CallByReference<Integer> depth,
			Helper.CallByReference<Long> numDFS,
			Helper.CallByReference<Long> numEdgesTouched, boolean fuzzy,
			boolean avoidTAHeuristics, int beamWidth, int widthThreshold,
			boolean doNotUseIndex) {
		boolean excludeQuery = true;
		boolean filterNode   = false;
		return GetTopKSubgraphsCGQHierarchicalHelper(K, queryGraph, neGraph,
				neCGQhierarchicalIndex, showProgress, depth, numDFS, numEdgesTouched,
				fuzzy, avoidTAHeuristics, beamWidth, widthThreshold, doNotUseIndex,
				excludeQuery, filterNode);
	}
	
	/**
	 * Function uses best first search, in beam format with DFS augmentation
	 * The index used is CGQ hierarchical
	 * The search will exclude query graph 
	 * @param K
	 * @param queryGraph
	 * @param neGraph
	 * @param neCGQhierarchicalIndex
	 * @param showProgress
	 * @param depth
	 * @param numDFS
	 * @param numEdgesTouched
	 * @param fuzzy
	 * @return
	 */
	static <T1 extends NodeFeatures>
	ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> 
	GetTopKSubgraphsCGQHierarchicalExcludingQueryAndFilterNode (int K, Graph<T1> queryGraph,
			Graph<T1> neGraph, 		CGQHierarchicalIndex<T1> neCGQhierarchicalIndex,
			boolean showProgress, 			Helper.CallByReference<Integer> depth,
			Helper.CallByReference<Long> numDFS,
			Helper.CallByReference<Long> numEdgesTouched, boolean fuzzy,
			boolean avoidTAHeuristics, int beamWidth, int widthThreshold,
			boolean doNotUseIndex) {
		boolean excludeQuery = true;
		boolean filterNode   = true;
		return GetTopKSubgraphsCGQHierarchicalHelper(K, queryGraph, neGraph,
				neCGQhierarchicalIndex, showProgress, depth, numDFS, numEdgesTouched,
				fuzzy, avoidTAHeuristics, beamWidth, widthThreshold, doNotUseIndex,
				excludeQuery, filterNode);
	}
		
	/**
	 * Function uses best first search, in beam format with DFS augmentation
	 * The index used is CGQ hierarchical
	 * @param K
	 * @param queryGraph
	 * @param neGraph
	 * @param neCGQhierarchicalIndex
	 * @param showProgress
	 * @param depth
	 * @param numDFS
	 * @param numEdgesTouched
	 * @param fuzzy
	 * @return
	 */
	private static <T1 extends NodeFeatures>
	ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> 
	GetTopKSubgraphsCGQHierarchicalHelper (int K, Graph<T1> queryGraph,
			Graph<T1> neGraph, 		CGQHierarchicalIndex<T1> neCGQhierarchicalIndex,
			boolean showProgress, 			Helper.CallByReference<Integer> depth,
			Helper.CallByReference<Long> numDFS,
			Helper.CallByReference<Long> numEdgesTouched, boolean fuzzy,
			boolean avoidTAHeuristics, int beamWidth, int widthThreshold,
			boolean doNotUseIndex, boolean excludeQuery, boolean filterNode) {

		//Defining a class to do the work
		class CGQHierarchical {
			final Graph<T1> targetGraph;
			final Graph<T1> queryGraph;
			final ArrayList <Graph<T1>.Edge> queryEdges; //order in which edges to be processed
			KPriorityQueue<ArrayList<Graph<T1>.Node>> kPQ;
			
			int depthProgress = 0;
			
			Helper.CallByReference<Integer> depth;
			Helper.CallByReference<Long> numDFS;
			Helper.CallByReference<Long> numEdgesTouched;
			
			final boolean showProgress;
			final boolean diGraph;
			final boolean fuzzy;
			
			final CGQHierarchicalIndex<T1> cgqIndex;
			
			/** we would prioritize size of partial solution */
			PartialSolutionPriorityQueue<T1> 	partialSolutionPQList;
			//PriorityQueue<PartialSolution<T1>> 	partialSolutionPQ;
			
			PriorityQueue<PartialSolution<T1>> 		fullSolutionPQ;
			
			//nodes with very high degree to be expanded will be added to this
			PriorityQueue<Helper.ObjectDoublePair<PartialSolution<T1>>> veryWidePS_PQ;
			
			final int beamWidth;
			final int widthThreshold;
			
			//exclude the query nodes from target graph
			final boolean excludeQuery;
			//filter nodes based on their features
			final boolean filterNode;
			//avoid the use of heuristics
			final boolean avoidTAHeuristics;
			
			/**
			 * Constructor
			 * @param K
			 * @param queryGraph
			 * @param targetGraph
			 * @param cgqIndex
			 * @param showProgress
			 * @param depth
			 * @param numDFS
			 * @param numEdgesTouched
			 * @param fuzzy
			 * @param excludeQuery 
			 * @param filterNode 
			 */
			CGQHierarchical (int K, Graph<T1> queryGraph, Graph<T1> targetGraph,
			CGQHierarchicalIndex<T1> cgqIndex, 			  boolean showProgress,
			Helper.CallByReference<Integer>depth, Helper.CallByReference<Long> numDFS,
			Helper.CallByReference<Long> numEdgesTouched, 	  boolean fuzzy, 
			boolean avoidTAHeuristics, int beamWidth, int widthThreshold, boolean excludeQuery,
			boolean filterNode) {
				this.fuzzy 				= fuzzy;
				this.cgqIndex 			= cgqIndex;
				this.queryGraph 		= queryGraph;
				this.showProgress 		= showProgress;
				this.beamWidth			= beamWidth;
				this.widthThreshold		= widthThreshold;
				this.targetGraph		= targetGraph;
				this.excludeQuery 		= excludeQuery;
				this.filterNode 		= filterNode;
				this.avoidTAHeuristics	= avoidTAHeuristics;
				
				kPQ = new KPriorityQueue<>(K, false, 0);

				//Best edge from which to start processing
				//FIXME
				queryEdges = Helper.BestEdgeForProcessing (queryGraph.edgeSet);
				//queryEdges = cgqIndex.BestEdgeForProcessing (queryGraph.edgeSet);
				
				if(gatherStat) {
					numEdgesTouched.element = 0L;
					this.numEdgesTouched 	= numEdgesTouched;
					numDFS.element 			= 0L;
					this.numDFS				= numDFS;
					this.depth				= depth;
				}

				diGraph					= queryGraph.DiGraph;								
				//partialSolutionPQ 		= new PriorityQueue<>();
				fullSolutionPQ			= new PriorityQueue<>();
				veryWidePS_PQ			= new PriorityQueue<>();
				partialSolutionPQList 	= new PartialSolutionPriorityQueue<T1>(queryGraph.SizeEdges(), beamWidth);
			}
			
			/**
			 * 
			 * @param doNotUseIndex : do not use the weight index
			 * @return
			 */
			ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>>
			GetTopKSubgraphs (boolean doNotUseIndex) {
				if (showProgress)
					System.out.print("              ");

				if (doNotUseIndex) {
					StartBFSWithoutindex();
				} else {
					StartBFS ();
				}
				if (showProgress)
					System.out.println();
				
				return kPQ.toArrayListObjectDoublePair();
			}
			
			private void StartBFSWithoutindex() {
				int size;
				
				if (fuzzy) {
					size = queryEdges.size();
				} else
					size = 1;
				
				boolean ret = false;
				//in case of fuzzy it is possible that the first 
				//edge is not mapped
				for (int i=0; i<(size-1); i++) {
					Graph<T1>.Edge qEdge = queryEdges.get(i);
					
					//first i edges where not mapped hence 
					double distance = (double) i*RAQ.penaltyDist;
					if ((distance) >= kPQ.LeastValueK()) {
						//we have found the top-K
						//we can break, since the bins are sorted
						break;
					}
					
					Iterator<Graph<T1>.Edge> it = targetGraph.edgeSet.iterator();
					while (!ret && it.hasNext()) {
						for (int j=0; (!ret && j<beamWidth && it.hasNext()); j++) {
							Graph<T1>.Edge tEdge = it.next();
							
							PartialSolution <T1> ps;

							boolean addFlag = true;
							
							if (filterNode) {
								addFlag = !(queryGraph.FilterEdge (qEdge, tEdge));
							}
							
							if (addFlag && excludeQuery) {
								//Are any of the nodes in tEdge in query graph?
								//if yes, do not do anything
								addFlag = !(queryGraph.EdgeTouched(tEdge));
							}
							
							if (addFlag) {
								ps = new PartialSolution<> (qEdge, tEdge,
										fuzzy, diGraph, false, 
										distance+qEdge.CGQDistEdges(tEdge), i+1);
								AddTopartialSolutionPQ(ps);
								depthProgress++;

								if (!diGraph) {
									ps = new PartialSolution<> (qEdge, tEdge, fuzzy,
											diGraph, true, 
											distance+qEdge.CGQDistEdges(tEdge), i+1);
									AddTopartialSolutionPQ(ps);
								}
								depthProgress++;
							}
						}
					}
					if (showProgress)
						ShowProgress.ShowLong(depthProgress);
					
					if(Beam (false, 0)) {
						ret = kPQ.ThresholdAchieved();
					}
				}
			}

			/**
			 * lets begin it
			 */
			private void StartBFS () {
				int size;
				
				if (fuzzy) {
					size = queryEdges.size();
				} else
					size = 1;
				
				//in case of fuzzy it is possible that the first 
				//edge is not mapped
				for (int i=0; i<=(size-1); i++) {
					Graph<T1>.Edge qEdge = queryEdges.get(i);
					
					//first i edges where not mapped hence 
					double distance = (double) i*RAQ.penaltyDist;
					if ((distance) >= kPQ.LeastValueK()) {
						//we have found the top-K
						//we can break, since the bins are sorted
						break;
					}
					
					//free unnecessary data, if any
					//partialSolutionPQ.clear();
					partialSolutionPQList.clear();
					fullSolutionPQ.clear();
					veryWidePS_PQ.clear();
					
					//sort the bins in the main index
					ArrayList<Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>> sortedBins = 
							cgqIndex.GetSortedBins(qEdge);

					for (Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> odBin : sortedBins) {
						//the values are similarities
						if (kPQ.UpdateThreshold (distance + 1-odBin.value))
							break;
						
						if ((distance + 1-odBin.value) >= kPQ.LeastValueK()) {
							//we have found the top-K
							//we can break, since the bins are sorted
							break;
						}

						//creating a stack for processing of bin, so that we can reach till the leaf
						PriorityQueue<Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>> binPQ = new PriorityQueue<>();
						
						CGQHierarchicalIndex<T1>.Bin bin = odBin.element;
						
						{
						double leastDistance = qEdge.leastDistance(bin);
						Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> entry = 
								new Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>(bin, leastDistance);
						binPQ.add(entry);
						}
						//flag to see if we have found the top-K
						boolean ret = false;
						
						while (!ret && (!binPQ.isEmpty())) {
							Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> entry = binPQ.poll();
							
							bin = entry.element;
							{
							double dist = entry.value;
							ret = kPQ.UpdateThreshold (dist);
							if (ret)
								break;
							}
							
							if (bin.leaf) {
								Iterator<Graph<T1>.Edge> it;

								//is this leaf containing only one type of neighbourhood vector
								if (bin.containsOnlyOneVEvector()) {
									boolean oneTimeOperation=true;
									
									//set to null to cause an error, since heuristics has shown
									//to be of negative effect in query graph of smaller size
									
									//june-3-2018 : I do not know why should this be made to null
									/*
									if (avoidTAHeuristics)
										it = null; //bin.allEdges.iterator();
									 */
									if (avoidTAHeuristics)
										it = bin.allEdges.iterator();
									else
										//it = bin.allEdges.iterator();
										//FIXME
										it = bin.heuristicsIterator(qEdge);
									
									//lets add all the elements in the bin to the priority queue
									double dist=0;
									if (it != null)
									while (!ret && it.hasNext()) {

										for (int j=0; (!ret && j<beamWidth && it.hasNext()); j++) {
											Graph<T1>.Edge tEdge = it.next();
											if (oneTimeOperation) {
												oneTimeOperation = false;
												dist = distance+qEdge.CGQDistEdges(tEdge);
												//ret = kPQ.UpdateThreshold (dist);
												//if (ret)
													//break;
											}
											

											if (kPQ.LeastValueK() > dist) {
												boolean addFlag = true;
												
												if (filterNode) {
													addFlag = !(queryGraph.FilterEdge (qEdge, tEdge));
												}
												
												if (addFlag && excludeQuery) {
													//Are any of the nodes in tEdge in query graph?
													//if yes, do not do anything
													addFlag = !(queryGraph.EdgeTouched(tEdge));
												}
												
												if (addFlag) {

													PartialSolution <T1> ps;

													ps = new PartialSolution<> (qEdge, tEdge,
															fuzzy, diGraph, false, dist, i+1);

													//adding partial solution to the priority Queue
													AddTopartialSolutionPQ(ps);
													Helper.GatherStatIncrementLong (numDFS);
													Helper.GatherStatIncrementLong (numEdgesTouched);
													depthProgress++;

													if (!diGraph) {
														ps = new PartialSolution<> (qEdge, tEdge, fuzzy,
																diGraph, true, dist, i+1);
														//adding partial solution to the priority Queue
														AddTopartialSolutionPQ(ps);

														if (gatherStat) {
															Helper.GatherStatIncrementInt  (depth);
															Helper.GatherStatIncrementLong (numDFS);
															Helper.GatherStatIncrementLong (numEdgesTouched);
														}
														depthProgress++;
													}
												}
											}
										}

										if (showProgress)
											ShowProgress.ShowLong(depthProgress);
											//ShowProgress.ShowDouble(kPQ.LeastValueK(), 0.11111111111111);
										
										//having populated enough partial solutions lets beam search them
										ret = Beam (true, dist);
										//ret = Beam (false, dist);
									}
								} else {
									//we shall sort the edges wrt to the distance to query edge
									ArrayList <Helper.ObjectDoublePair<Graph<T1>.Edge>> binEdges = new ArrayList<>();
									
									for (Graph<T1>.Edge tEdge : bin.allEdges) {
										double dist = qEdge.CGQDistEdges(tEdge);
										binEdges.add(new Helper.ObjectDoublePair<Graph<T1>.Edge>(tEdge, dist));
									}
									
									//sort the tedges
									Collections.sort(binEdges);
									
									for (Helper.ObjectDoublePair<Graph<T1>.Edge> od : binEdges) {
										//no subsequent edges can give a better match than this
										double dist = distance+od.value;
										Graph<T1>.Edge tEdge = od.element;
										
										//we can update the threshold of KPQ
										/*
										ret = kPQ.UpdateThreshold (dist);
										if (ret)
											break;
										
										if (kPQ.LeastValueK() <= dist) {
											//we have achieved the top K
											ret = true;
										} else {
										*/
										if (kPQ.LeastValueK() > dist) {
											boolean addFlag = true;
											
											if (filterNode) {
												addFlag = !(queryGraph.FilterEdge (qEdge, tEdge));
											}
											
											if (addFlag && excludeQuery) {
												//Are any of the nodes in tEdge in query graph?
												//if yes, do not do anything
												addFlag = !(queryGraph.EdgeTouched(tEdge));
											}

											if (addFlag) {
												PartialSolution <T1> ps;

												ps = new PartialSolution<> (qEdge, tEdge,
														fuzzy, diGraph, false, dist, i+1);

												//adding partial solution to the priority Queue
												AddTopartialSolutionPQ(ps);
												Helper.GatherStatIncrementLong (numDFS);
												Helper.GatherStatIncrementLong (numEdgesTouched);
												depthProgress++;

												if (!diGraph) {
													ps = new PartialSolution<> (qEdge, tEdge, fuzzy,
															diGraph, true, dist, i+1);
													//adding partial solution to the priority Queue
													AddTopartialSolutionPQ(ps);
													if (gatherStat) {
														Helper.GatherStatIncrementInt  (depth);
														Helper.GatherStatIncrementLong (numDFS);
														Helper.GatherStatIncrementLong (numEdgesTouched);
													}
													depthProgress++;
												}
												
												if (showProgress)
													ShowProgress.ShowLong(depthProgress);
													//ShowProgress.ShowDouble(kPQ.LeastValueK(), 0.111111111111);

												ret = Beam (true, dist);
												//ret = Beam (false, dist);
											}
										}
										if (ret)
											break;
									}
								}

								//since we are aggressively doing breadth first search
								//we need to do this
								if (!ret)
									ret = Beam (false, 0);
							} else {
								//since we are not at the leaf we need to go deeper
								Iterator<CGQHierarchicalIndex<T1>.Bin> it = bin.iterator(qEdge, true);
								
								while (it.hasNext()) {
									CGQHierarchicalIndex<T1>.Bin branch = it.next();
									{
										double leastDistance = qEdge.leastDistance(branch);
										Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> odp = 
												new Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>
										(branch, leastDistance);
										binPQ.add(odp);
									}
								}
							}
							if (RAQ.interruptable)
								if (InterruptSearchSignalHandler.Interrupt())
									return;
						}
						
						/*
						 * we cannot break here because other bin could have better value
						 * and as it is we are now updating thresholds
						 */
						//if (ret)
							//break;
					}
				}
			}
			
			/**
			 * We will add the ps to stack to populate the kPQ asap 
			 * to get effective pruning
			 * @param ps
			 */
			private void AddTopartialSolutionPQ(PartialSolution<T1> ps) {
				//the worst distance to which ps can grow
				Double worstDistancekPQ = kPQ.LeastValueK();

				if (ps.Distance() >= worstDistancekPQ) {
					//there is no point in growing any further
					//discard ps
				} else {
					if (ps.NumEdgesMapped() >= queryEdges.size()) {
						fullSolutionPQ.add(ps);
					} else {
						if ((partialSolutionPQList.isEmpty()) || 
								(WorstDistance (ps) < worstDistancekPQ)) {
							//we need to do DFS
							//we need to populate the kPQ asap
							//to get effective pruning
							partialSolutionPQList.add(ps);
						} else {
							partialSolutionPQList.add(ps);
							//partialSolutionPQ.add(ps);
						}
					}
				}
			}

			/**
			 * What is the worst possible distance growth of this
			 * partial solution can give 
			 * @param ps
			 * @return
			 */
			private Double WorstDistance(PartialSolution<T1> ps) {
				Double dist = ps.Distance();
				//edges to be reprocessed
				dist += ps.NumReprocessEdges();
				
				//edges to be freshly processed
				dist += queryEdges.size() - ps.NumEdgesMapped();
				
				return dist;
			}

			/**
			 * do a Beam search
			 * @param i 
			 * @param b 
			 * @return true if we find top-k 
			 */
			boolean Beam (boolean aggressive, double prevousDist) {
				boolean ret = false;
				PartialSolution <T1> ps=null;
				Stack<PartialSolution<T1>> psStack = new Stack<>();
				
				while (!ret) {
					Graph<T1>.Edge qEdge=null;
					
					boolean foundAMatch = false;
					boolean secondPass 	= false;

					boolean considerWidth = true;
					
					double currentWorstDistance = kPQ.LeastValueK();

					ps=null;
					
					if (!fullSolutionPQ.isEmpty()) {
						ps = fullSolutionPQ.poll();

						if (ps.Distance() >= currentWorstDistance) {
							//we need not process any further
							fullSolutionPQ.clear();
							ps=null;
						} else {
							if (!fuzzy)
								foundAMatch = true;
							else {
								if (ps.NumReprocessEdges () == 0)
									foundAMatch = true;
							}
						}
					}
					if (ps == null) {
						if (psStack.isEmpty()) {
							//we do not have any thing to beam
							psStack = partialSolutionPQList.poll(currentWorstDistance);
						}
						
						if (!psStack.isEmpty()) {
							ps = psStack.pop();
							
							if (RAQ.interruptable)
								if (InterruptSearchSignalHandler.Interrupt()) {
									//CheckStack();
									return true;
								}
							//considerWidth = false;
						} else if (!veryWidePS_PQ.isEmpty()) {

							if (RAQ.interruptable)
								if (InterruptSearchSignalHandler.Interrupt()) {
									return true;
								}
							ps = veryWidePS_PQ.poll().element;
							considerWidth = false;
						} /*else {
							if (RAQ.interruptable)
								if (InterruptSearchSignalHandler.Interrupt())
									return true;

							ps = partialSolutionPQ.poll();

							if (ps == null)
								break;

							if (ps.Distance() >= kPQ.LeastValueK()) {
								//we have found the top-k
								ret = true;
								partialSolutionPQ.clear();
								break;
							}

							if (aggressive) {
								if (ps.Distance() > prevousDist) {
									//we have a sub optimal growth
									AddTopartialSolutionPQ(ps);
									break;
								}
							}
							keepTab = 4;
						}*/
					}
					if (ps == null)
						break;
					
					if (foundAMatch) {
						//we have a valid mapping
						//add the mapping to result.
						ArrayList<Graph<T1>.Node> mapping = new ArrayList<>();
						for (Graph<T1>.Node qNode : queryGraph.nodeList) {
							Graph<T1>.Node tNode = ps.GetqNodeToTNodeMap(qNode);
							if (!fuzzy) {
								if (tNode == null) {
									System.err.println("Beam in hierarchical : tNode = null? mapping is missing");
									System.exit(-1);
								}
							} else {
								//do nothing
							}
							Graph<T1>.Node mNode = ps.GetqNodeToTNodeMap(qNode);
							if (!fuzzy && mNode == null) {
								System.err.println("The result is wrong : BFSQuery.Beam");
							}
							mapping.add(mNode);
						}
						double dist = ps.Distance();

						CheckDist(dist, ps.NumEdgesMapped());

						if (kPQ.add(mapping, dist)) {
							ret =  kPQ.ThresholdAchieved();
						} else {
							//we can empty the fullSolutionPQ
							fullSolutionPQ.clear();
						}
						//FIXME XYZ
						//ShowProgress.ShowDouble(kPQ.LeastValueK(), ps.Distance());
					} else {

						//lets grow the partial solution
						int numEdgesMapped = ps.NumEdgesMapped();

						if (numEdgesMapped < queryEdges.size()) {
							qEdge = queryEdges.get(numEdgesMapped);
						} else {
							//we need to re-process some edges
							secondPass = true;
							qEdge = ps.GetReprocessEdge ();
						}								
						
						//which of its node is already mapped?
						// note, this is a valid question because of the ordering of edges we have adopted
						Graph<T1>.Node tNode1 = ps.GetMap(qEdge.node1);
						Graph<T1>.Node tNode2 = ps.GetMap(qEdge.node2);

						if (tNode1 != null) {
							//node1 of qEdge has been mapped
							if (tNode2 != null) {
								//node2 of qEdge is also mapped
								//this means we should have edge between tNode1 and tNode2
								Graph<T1>.Edge tEdge = tNode1.GetEdgeTo(tNode2);
								if (tEdge == null) {
									//this is not a valid subgraph growth
									if (fuzzy && 
											(currentWorstDistance > 1.0) //a edge miss will add at the least 1.0
											) {
										ps.OneEdgeMissed();
										//add back the priority queue
										AddTopartialSolutionPQ(ps);
									} else {
										//discard ps
										ps=null;
									}
								} else {
									boolean addFlag = true;
									
									if (filterNode) {
										addFlag = !(queryGraph.FilterEdge (qEdge, tEdge));
									}
									
									if (addFlag) {
										Helper.GatherStatIncrementLong (numEdgesTouched);
										Double dist = qEdge.CGQDistEdges(tEdge);
										ps.IncrementDistance (dist);
										//add back the priority queue
										if (dist == 0) {
											//we have a optimal growth so process this first
											AddTopartialSolutionStack(ps);
										} else {
											AddTopartialSolutionPQ(ps);
										}
									} else {
										ps = null;
									}
								}
							} else {
								//we have an outgoing (in case of Digraph) edge to handle
								boolean process = true;
								if (considerWidth) {
									int w = tNode1.edges.size(); 
									if (w > widthThreshold) {
										Helper.ObjectDoublePair<PartialSolution<T1>> od = 
												new Helper.ObjectDoublePair<PartialSolution<T1>>(ps, (double)w);

										veryWidePS_PQ.add(od);
										process = false; // we will proccesss this later
									}
								} 

								if (process){
									//expand all the out going edges into respective partial solutions
									if (diGraph) {
										//process out going edges
										//we will map the qEdge to each of the out going edges
										for (Graph<T1>.Edge tEdge : tNode1.edges) {
											Double dist = qEdge.CGQDistEdges(tEdge);
											AddMap(ps, qEdge.node2, tEdge.destination, dist, qEdge, tEdge);
										}
										//discard ps
										ps=null;
									} else {
										//process all edges
										//we will map the qEdge to each of the edges
										for (Graph<T1>.Edge tEdge : tNode1.edges) {
											Double dist = qEdge.CGQDistEdges(tEdge);

											//find the tnode to map to
											Graph<T1>.Node tNode;

											if (tEdge.node1 == tNode1)
												//we need to map to the other node
												tNode = tEdge.node2;
											else
												tNode = tEdge.node1;

											AddMap(ps, qEdge.node2, tNode, dist, qEdge, tEdge);
										}
										//discard ps
										ps=null;
									}
								}
							}
						} else if (tNode2 != null) {
							boolean process = true;
							
							if (considerWidth) {
								//int w = tNode2.incomingEdges.size();
								int w = tNode2.edges.size();
								if (w > widthThreshold) {
									Helper.ObjectDoublePair<PartialSolution<T1>> od = 
											new Helper.ObjectDoublePair<PartialSolution<T1>>(ps, (double)w);

									veryWidePS_PQ.add(od);
									process = false;
								}
							} 

							if (process){
								//we have an incoming (in case of Digraph) edge to handle
								if (diGraph) {
									//process incoming edges
									//we will map the qEdge to each of the incoming edges
									for (Graph<T1>.Edge tEdge : tNode2.incomingEdges) {
										Double dist = qEdge.CGQDistEdges(tEdge);
										AddMap(ps, qEdge.node1, tEdge.source, dist, qEdge, tEdge);
									}
									//discard ps
									ps=null;
								} else {
									//process all edges
									//we will map the qEdge to each of the edges
									for (Graph<T1>.Edge tEdge : tNode2.edges) {
										Double dist = qEdge.CGQDistEdges(tEdge);
										//find the tnode to map to
										Graph<T1>.Node tNode;

										if (tEdge.node1 == tNode2)
											//we need to map to the other node
											tNode = tEdge.node2;
										else
											tNode = tEdge.node1;

										AddMap(ps, qEdge.node1, tNode, dist, qEdge, tEdge);
									}
									//discard ps
									ps=null;							
								}
							}
						} else if (fuzzy && !secondPass){
							//this edge need to be processed in the second pass
							ps.ProcessEdgeLater (qEdge);
							AddTopartialSolutionPQ (ps);
						}
					}
				}
				
				return ret;
			}

			/**
			 * function to see all elements are unique in the stack
			 */
			@SuppressWarnings("unused")
			private void CheckStack() {
				class ComparePS implements Comparator<PartialSolution<T1>> {

					@Override
					public int compare(PartialSolution<T1> o1, PartialSolution<T1> o2) {
						return Integer.compare(o1.Size(), o2.Size());
					}
				}
				
				ArrayList<PartialSolution<T1>> list = partialSolutionPQList.getArrayList();
				
				Collections.sort(list, new ComparePS());
				
				int dublicates = 0;
				//now we shall compare the list elements
				int start=0;
				do {
					int end;
					int size = list.get(start).Size();
					
					for (end=start+1; end<list.size(); end++) {
						int s = list.get(end).Size();
						if (s > size)
							break;
					}
					
					List<PartialSolution<T1>> subList = list.subList(start, end);
					
					for (int i=0; i<subList.size(); i++) {
						PartialSolution<T1> ps1 = subList.get(i);
						for (int j=i+1; j<subList.size(); j++) {
							PartialSolution<T1> ps2 = subList.get(j);
							
							if (ps1.IsSameAs(ps2))
								dublicates++;
						}
					}
					start = end;
				} while (start < list.size());
				
				System.out.println("We have "+dublicates+" duplicates in the stack");
			}

			private void AddTopartialSolutionStack(PartialSolution<T1> ps) {
				if (ps.NumEdgesMapped() < queryEdges.size())
					partialSolutionPQList.add(ps);
				else
					fullSolutionPQ.add(ps);
			}

			private void AddMap(PartialSolution<T1> ps,
					Graph<T1>.Node qNode, Graph<T1>.Node tNode, Double dist,
					Graph<T1>.Edge qEdge, Graph<T1>.Edge tEdge) {
				boolean addFlag = true;
				
				if (filterNode) {
					addFlag = !(queryGraph.FilterEdge (qEdge, tEdge));
				}
				
				if (addFlag && excludeQuery) {
					addFlag = !(queryGraph.NodeTouched(tNode));
				}
				
				if(addFlag) {
					//make a copy of ps
					PartialSolution <T1> psNew = new PartialSolution<>(ps);
					if (psNew.AddMap (qNode, tNode, dist)) {
						//this is a valid map
						if (dist == 0) {
							//we have a optimal growth so process this first
							AddTopartialSolutionStack(psNew);
						} else {
							AddTopartialSolutionPQ(psNew);
						}
						Helper.GatherStatIncrementLong (numDFS);
					}
				}
			}
		}

		return new CGQHierarchical(K, queryGraph, neGraph,
				neCGQhierarchicalIndex, showProgress,
				depth, numDFS, numEdgesTouched, fuzzy,
				avoidTAHeuristics, beamWidth, widthThreshold,
				excludeQuery, filterNode).GetTopKSubgraphs(doNotUseIndex);
	}
	
	static void CheckDist (double dist, int numEdgesMapped) {
		if (RAQ.slowAndSafe) {
			if (dist > numEdgesMapped) {
				//How can this be ?????
				System.err.println("dist > numEdgesMapped");
				System.exit(-1);
			}
		}
	}
}
