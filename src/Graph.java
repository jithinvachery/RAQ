import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Random;
import java.util.Stack;
import java.util.TreeMap;


public class Graph <T1 extends NodeFeatures>
{
	private static ArrayList<Integer> RandomSubgraphArrayNode;

	static Random random = new Random(System.currentTimeMillis());
	final int DegreeRefinement = 1;
	boolean DiGraph; //true if the graph is directed
	/**
	 * class for a node in the graph
	 */
	class Node  {
		int  nodeID;
		T1   features;
		
		
		// we shall maintain neighbours in both edges and neighbours this allows ease of access
		HashSet<Edge>    edges;
		HashSet<Edge>    incomingEdges;
		HashSet<Node>    neighbours;
		HashSet<Node>    iAmNeighbourTo;

		//the degree distribution up to 3-hops are saved as an index
		private int[] degreeDist;
		private int[] inDegreeDist;
		//the max degree of any node of in the 2nd and 3rd hop
		private int[] degreeMax;
		private int[] inDegreeMax;
				
		//local CGQHierarchicalIndex
		CGQHierarchicalIndex <T1> cgqHierarchicalIndex;
		CGQHierarchicalIndex <T1> incomingCGQHierarchicalIndex;
		
		
		Node(int n, T1 f) {
			nodeID 		= n;
			features 	= f;
						
			edges 	   	 	= new HashSet<>();
			neighbours 	 	= new HashSet<>();
			degreeDist   = new int[DegreeRefinement];
			degreeMax    = new int[DegreeRefinement-1];

			if (DiGraph) {
				incomingEdges	= new HashSet<>();
				iAmNeighbourTo 	= new HashSet<>();
				inDegreeDist = new int[DegreeRefinement];
				inDegreeMax  = new int[DegreeRefinement-1];
			}
		}
				
		Edge AddNeighbour (Node node){
			Edge edge = new Edge(this, node);
			
			edges.add       (edge);
			neighbours.add  (node);
			
			if (!DiGraph) {
				node.edges.add     (edge);
				node.neighbours.add(this);
			}
			
			if (DiGraph) {
				node.AddToIAmNeighbourTo(this);
				node.incomingEdges.add(edge);
			}
			
			return edge;
		}
		
		void AddToIAmNeighbourTo (Node node) {
			iAmNeighbourTo.add(node);
		}

		/**
		 * function to get all the features in the node
		 * This arraylist should not be modified.
		 * @return
		 */
		public String[] AllFeatures() {
			return features.AllFeatures();
		}
		
		int NumFeatures(){
			return features.NumFeatures();
		}
		
		int NumCategoricalFeatures(){
			return features.NumCategoricalFeatures();
		}
		
		/**
		 * Function to find the VE vector for the edge identified by 
		 * the node to which current node connects to
		 * @return
		 */
		
		private double[] GetVE(Node destination) {
			return features.GetVE (destination.features);
		}
		
		public int Degree(){
			return neighbours.size();
		}
				
		/**
		 * Function to update the degree distribution index structure
		 */
		public void UpdateDegreeDist () {
			HashSet<Node> nodes = new HashSet<>();
			
			nodes.add(this);
			
			for (int i=0; i<DegreeRefinement; i++) {
				int d =0;				
				int max 	= 0;
				
				HashSet<Node> tempSet = new HashSet<>();
				
				for (Node node : nodes) {
					//FIXME: this sort of counting will do double counting but intentionally kept so
					d += node.Degree();
					tempSet.addAll(node.neighbours);
					
					if (d>max)
						max=d;
				}
				
				degreeDist	[i]	= d;				
				if (i > 0) {
					//we are only interested in 2nd hop and higher
					degreeMax	[i-1]	= max;
				}
				
				nodes = tempSet;
			}
			
			if (DiGraph) {
				nodes = new HashSet<>();
				
				nodes.add(this);
				
				for (int i=0; i<DegreeRefinement; i++) {
					int in=0;
					int maxIn 	= 0;
					
					HashSet<Node> tempSet = new HashSet<>();
					
					for (Node node : nodes) {
						//FIXME: this sort of counting will do double counting but intentionally kept so
						in+= node.InDegree();
						tempSet.addAll(node.neighbours);
						
						if (in>maxIn)
							maxIn=in;
					}
					
					inDegreeDist[i]	= in;
					
					if (i > 0) {
						//we are only interested in 2nd hop and higher
						inDegreeMax	[i-1]	= maxIn;
					}
					
					nodes = tempSet;
				}
			}
		}

		int InDegree() {
			return iAmNeighbourTo.size();
		}

		/**
		 * Function to check if the degree distribution of tNode matched that of this edge
		 * @param tNode
		 * @return
		 */
		public boolean ValidDegreeDist(Graph<T1>.Node tNode) {
			boolean ret = true;
			
			for(int i=0; i<DegreeRefinement-1; i++) {
				if ((degreeMax[i] > tNode.degreeMax[i]) ||
						((DiGraph) && (inDegreeMax[i] > tNode.inDegreeMax[i]))) {
					ret = false;
					break;
				}
			}

			for(int i=0; i<DegreeRefinement; i++) {
				if ((degreeDist[i] > tNode.degreeDist[i]) ||
						((DiGraph) && (inDegreeDist[i] > tNode.inDegreeDist[i]))) {
					ret = false;
					break;
				}
			}
				
			return ret;
		}

		public int TotalDegree() {
			int ret=0;
			
			for(int i=0; i<DegreeRefinement; i++)
				ret += degreeDist[i];
			
			return ret;
		}

		/**
		 * get the edge going to the node
		 * @param qNode
		 * @return
		 */
		public Edge GetEdgeTo(Node node) {
			Edge ret = null;
			
			if (DiGraph) {
				for (Edge edge : edges) {
					if (edge.destination == node) {
						ret = edge;
					}
				}
			} else {
				for (Edge edge : edges) {
					if ((edge.node1 == node) ||
							(edge.node2 == node))	{
						ret = edge;
					}
				}
			}
			return ret;
		}

		/**
		 * Return the set containing both incoming and outgoing edges.
		 * @return
		 */
		public HashSet<Edge> AllEdges() {
			HashSet<Edge> ret = new HashSet<>(edges);
			
			if (incomingEdges != null)
				ret.addAll(incomingEdges);
			
			return ret;
		}

		/**
		 * Find the traditional distance between two nodes based on their feature values
		 * @param tNode
		 * @return
		 */
		public double Distance(Graph<T1>.Node tNode) {
			return features.Distance(tNode.features);
		}

		/**
		 * the nodes are compared based on a unique identifier which need not be its feature
		 * @param tNode
		 * @return
		 */
		public boolean IsSameAs(Graph<T1>.Node tNode) {
			return features.NodeIsSameAs(tNode.features);
		}

		/**
		 * get the summary of the nodes fetaure
		 * @param ve 
		 * @return
		 */
		public int[][] getSummaryVector(double[] ve) {			
			return features.getSummaryVector(ve);
		}
	}

	/**
	 * class for a edge in the graph
	 * @author jithin
	 *
	 */
	class Edge   {		
		Node source;				// source of the edge; valid only for Digraph
		Node destination;		    // the node this edge connects to; valid only for Digraph
		
		Node node1;
		Node node2; //valid only for both directed and undirected graph

		//the similarity vector of similarity between the two nodes
		private double[] ve;
		private String   veString;
		private String   veStringCategorical;
		
		//the summary of the neighbourhood of edge is maintained here
		private int[][] neighbourhoodSummary;
		void updateNeighbourhoodSummary (){
			//allocating memory
			neighbourhoodSummary = new int[NumFeatures()][BFSQuery.HeuristicsNumBin];
			
			//summarize each of the neighbours ve vector			
			for (Edge edge : IncidentEdges()) {
				int [][]summaryVector = node1.getSummaryVector(edge.ve);
				
				for (int i=0; i<summaryVector.length; i++) {
					for (int j=0; j<summaryVector[i].length; j++) {
						neighbourhoodSummary[i][j] += summaryVector[i][j];
					}
				}
			}
		}

		Edge (Node source, Node neighbour){
			if (DiGraph) {
				this.destination	 = neighbour;
				this.source			 = source;
			}

			node1 = source;
			node2 = neighbour;

			ve = GetVEPrivate();
			
			//converting the ve vector into a string
			veStringCategorical	 	= "";
			int numCategorical 		= source.NumCategoricalFeatures(); 
			DecimalFormat dFormat 	= new DecimalFormat("#.00");
			
			for (int i=0; i<numCategorical; i++) {
				veStringCategorical += dFormat.format(ve[i])+":";
			}
			
			veString = ""+veStringCategorical;			
			for (int i=numCategorical; i<ve.length; i++) {
				veString += dFormat.format(ve[i])+":";
			}
			//removing the last ":"
			veString = veString.substring(0, veString.length()-1);
			try {
				veStringCategorical = veStringCategorical.substring(0, veStringCategorical.length()-1);
			} catch (IndexOutOfBoundsException e) {
				//we have no categorical features
				veStringCategorical	= "";
			}
		}
		
		String[] GetAllFeatures () {
			return node1.AllFeatures();
		}
		
		int NumFeatures () {
			return node1.NumFeatures();
		}
		
		int NumCategoricalFeatures () {
			return node1.NumCategoricalFeatures();
		}
		
		void UpdateVE () {
			ve = GetVEPrivate();
		}
		
		/**
		 * Function to find the VE vector for the edge
		 * @return
		 */
		private double[] GetVEPrivate() {			
			return node1.GetVE(node2);
		}
		
		double GetVe(int i) {
			return ve[i];
		}
		
		double[] GetVe(){
			return ve;
		}
		
		String GetVeString(){
			return veString;
		}
		
		String GetVeStringCategorical(){
			return veStringCategorical;
		}
						
		/**
		 * Given a query graph, extend this edge to form all possible subgraphs in the target graph,
		 * which is isomorphic to give graph
		 * @param qGraph
		 * @param qEdge : the edge in qGraph which is isomorphic to this  edge 
		 * @return set of "set of nodes" which classify the subgraph
		 */
		HashSet<ArrayList<Node>> ExtendEdgeToGraph (Graph<T1> qGraph, 
				Edge qEdge,
				Graph<T1> targetGraph){
			if (!DiGraph) {
				//this function is obsolete and will be removed soon
				throw new UnsupportedOperationException();
			}
			HashSet<ArrayList<Node>> mapSet = new HashSet<>();
			//to find a subgraph we basically find a mapping
			//we can start our mapping in two directions
			//	1) source-source dest-dest
			//	2) source-dest dest-source
			
			//check if (1) is possible
			// i.e. degree_query(source) <= degree(source)
			//  &   degree_query(dest)   <= degree(dest)
			if (qEdge.source.ValidDegreeDist(source) &&
					qEdge.destination.ValidDegreeDist(destination)){
				StartUpdate_mapSet(qEdge.source, qEdge.destination,
						mapSet, qEdge, qGraph, targetGraph);
			}
			
			if (qEdge.source.ValidDegreeDist(destination) &&
					qEdge.destination.ValidDegreeDist(source)){
				StartUpdate_mapSet(qEdge.destination, qEdge.source,
						mapSet, qEdge, qGraph, targetGraph);
			}
			
			return mapSet;
		}

		private void StartUpdate_mapSet(Node qSource, Node qDest,
				HashSet<ArrayList<Node>> mapSet, 
				Edge qEdge, Graph<T1> qGraph,
				Graph<T1> targetGraph) {
			
			//class to compare nodes based on node ID
			class nodeIDComp implements Comparator<Node> {
				@Override
				public int compare(Node o1, Node o2) {
					return Integer.compare(o1.nodeID, o2.nodeID);
				}
			}
			
			//Map from query graph to Target graph
			TreeMap<Node, Node> mapQT =
					new TreeMap<>(new nodeIDComp());			
			mapQT.put(qSource, source);
			mapQT.put(qDest,   destination);
			
			//Map from Target graph to querygraph
			TreeMap<Node, Node> mapTQ =
					new TreeMap<>(new nodeIDComp());
			mapTQ.put(source, qSource);
			mapTQ.put(destination, qDest);
			
			Stack<Edge> edgesMapped = new Stack<>();
			edgesMapped.add(qEdge);
			
			Update_mapSet (mapSet, mapQT, mapTQ, edgesMapped, qGraph, targetGraph);
		}

		private void Update_mapSet(HashSet<ArrayList<Node>> mapSet,
				TreeMap<Node, Node> mapQT,
				TreeMap<Node, Node> mapTQ, 
				Stack<Edge> edgesMapped,
				Graph<T1> qGraph,
				Graph<T1> targetGraph) {
			//System.out.println("edgesMapped : "+edgesMapped.size() + " qGraph : " + qGraph.SizeEdges());
			if (edgesMapped.size() == qGraph.SizeEdges()){
				//we have a valid map
				//convert this map into arraylist
				ArrayList<Node> map = new ArrayList<>(mapQT.values());
				//we only extended based on our edges in query graph, but this need not mean a valid map,
				//since we might have an edge in the target graph which does not have a valid pre-map in query graph
				if (RAQ.GraphToNodeMapValid(qGraph, targetGraph, map))
					mapSet.add(map);
				
				//for (Node node : map) 
				//	System.out.print(node.nodeID+",");
				//System.out.println();
			} else {
				HashSet<Edge> edgesToBeConsidered = new HashSet<>();
				
				// iterate through all edges connected to
				// the current set of mapped edges
				for (Edge edge : edgesMapped) {
					edgesToBeConsidered.addAll(edge.source.edges);
					edgesToBeConsidered.addAll(edge.destination.edges);
				}
				edgesToBeConsidered.removeAll(edgesMapped);
				//System.out.println("edgesToBeConsidered : "+edgesToBeConsidered.size());

				for (Edge edge : edgesToBeConsidered) {
					//extend the mapping by trying to map this edge
					Node source   = edge.source;
					Node mSource  = mapQT.get(source);
					Node dest     = edge.destination;
					
					if (mapQT.keySet().contains(dest)){
						//we have mapped the nodes of this edge already
						edgesMapped.push(edge);
						Update_mapSet (mapSet, mapQT, mapTQ, edgesMapped, qGraph, targetGraph);
						edgesMapped.pop();
					} else {
						//we can map to any one of the edges moving out of mSource
						for (Edge mEdge : mSource.edges) {
							Node mDest = mEdge.destination;
							if (mapTQ.keySet().contains(mDest)) {
								//this node is already taken so we cannot map to this
							} else {
								//check if we can map this node
								if (dest.ValidDegreeDist(mDest)) {
								//if (mDest.Degree() >= dest.Degree()){
									//now check if the edge labels match
									//if (RAQ.CGQDistEdges(edge, mEdge) == 0){
										//we have a valid map
										mapQT.put(dest, mDest);
										mapTQ.put(mDest, dest);
										edgesMapped.push(edge);
										
										Update_mapSet (mapSet, mapQT, mapTQ, edgesMapped, qGraph, targetGraph);
										
										edgesMapped.pop();
										mapTQ.remove(mDest);
										mapQT.remove(dest);										
									//}
								}
							}
						}
					}
				}
			}
		}

		public int TotalDegree() {
			return node1.TotalDegree()+node2.TotalDegree();
		}

		/**
		 * Function to get the set of all edges (incoming and outgoing)
		 * incident on an edge (i.e. on both its end points)
		 * @return
		 */
		public HashSet<Edge> IncidentEdges() {
			HashSet<Edge> incidentEdges = new HashSet<>();

			incidentEdges.addAll(node1.edges);
			incidentEdges.addAll(node2.edges);

			if (DiGraph) {
				incidentEdges.addAll(node1.incomingEdges);
				incidentEdges.addAll(node2.incomingEdges);
			}
			
			return incidentEdges;
		}

		/**
		 * return a list of all feature value pairs taken all features
		 * @return
		 */
		public ArrayList<ArrayList<String>> AllFeatureValues() {
			return node1.features.AllFeatureValues(node2.features);
		}

		/*************************************
		 * CGQ Similarities
		 *************************************/
		
		/**
		 * this is query edge
		 * @param edge : target edge
		 * @return returns 1 if any parameter is null
		 */
		double CGQDistEdges (Edge edge) {
			double dist;
		
			if (edge==null)
				dist = 1.0;
			else {
				double[] ve_2 = edge.GetVe();
			
				dist = CGQDistEdges (ve_2);
			}
			
			return dist;
		}
		
		double CGQDistEdges (double[] ve_2) {
			double   sim 	= 0.0;
			double[] ve_1 	= ve;
			
			for (int i=0; i < NumFeatures(); i++) {
				double ve1 = ve_1[i];
				double ve2 = ve_2[i];

				double min, max;
				if (ve1 < ve2) {
					min = ve1;
					max = ve2;
				} else {
					min = ve2;
					max = ve1;
				}

				if (max != 0)
					sim += weights[i]*min/max;
				else
					sim += weights[i];
			}
					
			//sanity
			if (RAQ.slowAndSafe) {
				if (sim > 1.0000000001){
					System.err.println("Fatal error CGQDistEdges: similarity between edges > 1");
					double weight = 0.0;
					for (int i=0; i<NumFeatures(); i++) {
						weight += weights[i];
					}
					System.err.println("Weights add up to : "+weight);
					System.exit(-1);
				}
			} else if (sim > 1){
				sim = 1.0;
			}
			
			double dist = (1-sim);
			
			//rounding to 3 decimal places
			int n = 1000;
			dist  = dist * n;
			dist  = Math.round(dist);
			dist /= n;
			
			return dist;
		}
		

		double CGQSimilarityEdgesNonCategorical (Graph<T1>.Edge tEdge) {
			return CGQSimEdgesHelper (NumCategoricalFeatures(), NumFeatures(), tEdge);
		}

		double CGQSimilarityEdgesCategorical (Graph<T1>.Edge tEdge) {
			return CGQSimEdgesHelper (0, NumCategoricalFeatures(), tEdge);
		}

		/**
		 * What is the maximum similarity possible if we allow
		 * full freedom to non-categorical features
		 * @return
		 */
		double CGQSimilarityEdgesMaxPOssibleConsideringCategorical (Graph<T1>.Edge tEdge) {
			int numCat = NumCategoricalFeatures();
			double sim = CGQSimEdgesHelper (0, numCat, tEdge);
			
			for (int i=numCat; i< NumFeatures(); i++)
				sim += weights[i];
			
			return sim;
		}
		
		private double CGQSimEdgesHelper (int start, int end, Graph<T1>.Edge e2) {
			double sim = 0.0;
		
			Graph<T1>.Edge e1 = this;
			
			double[] ve_2  		= e2.GetVe();
			double[] ve_1 		= e1.GetVe();

			for (int i=start; i<end; i++) {
				double ve1 = ve_1[i];
				double ve2 = ve_2[i];

				double min, max;
				if (ve1 < ve2) {
					min = ve1;
					max = ve2;
				} else {
					min = ve2;
					max = ve1;
				}

				if (max != 0)
					sim += weights[i]*min/max;
				else
					sim += weights[i];
			}
			
			//sanity
			if (RAQ.slowAndSafe) {
				if (sim > 1.0000000001){
					System.err.println("Fatal error CGQDistEdgesHelper: similarity between edges > 1");
					double weight = 0.0;
					for (int i=0; i<e1.NumFeatures(); i++) {
						weight += weights[i];
					}
					System.err.println("Weights add up to : "+weight);
					System.exit(-1);
				}
			} else if (sim > 1){
				sim = 1.0;
			}

			return sim;
		}
		
		
		/**
		 * Find the minimum distance possible for the query edge (this) to any edge in the bin
		 * based only on the non-categorical features
		 * @param qEdge
		 * @return
		 */
		double MaxNonCategoricalsimilarity (double[] veUpper, double[] veLower) {
			double sim = 0.0;

			Graph<T1>.Edge qEdge= this;
			double []qVe 		= qEdge.GetVe();

			for (int i=0; i<veLower.length; i++) {
				int q = i+qEdge.NumCategoricalFeatures();
				
				double vQ = qVe[q];
				double vU = veUpper[i];
				double vL = veLower[i];
				
				double min, max;

				if (vQ <= vL) {
					min = vQ;
					max = vL;
				} else if (vU <= vQ) {
					min = vU;
					max = vQ;
				} else {
					//we are with in the window
					min = 0;
					max = 0;
				}
				
				if (max != 0)
					sim += weights[i]*min/max;
				else
					sim += weights[i];
			}
			
			//sanity
			if (RAQ.slowAndSafe) {
				if (sim > 1.0000000001){
					System.err.println("Fatal error MinDistance: similarity between edges > 1; sim = "+sim);
					double weight = 0.0;
					for (int i=0; i<qEdge.NumFeatures(); i++) {
						weight += weights[i];
					}
					System.err.println("Weights add up to : "+weight);
					System.exit(-1);
				}
			} else if (sim > 1){
				sim = 1.0;
			}

			return sim;
		}

		/**
		 * Heuristics is used to compare edges, "this" edge should be from the query graph
		 * @param edge
		 * @return
		 */
		public Double heuristicsDistance(Graph<T1>.Edge edge) {
			int [][] neighbourhoodSummary1 = this.neighbourhoodSummary;
			int [][] neighbourhoodSummary2 = edge.neighbourhoodSummary;
			
			Double distance = 0.0;
			
			for (int i=0; i< weights.length; i++) {
				int [] summary1 = neighbourhoodSummary1[i];
				int [] summary2 = neighbourhoodSummary2[i];
				
				int length = summary1.length;
				double c=0;
				
				for (int j=0; j<length; j++) {	
					//is summary1 contained in summary2?
					if (summary1[j] != 0)
						if (summary1[j] <= summary2[j])
							c++;
				}
				
				//similarity
				c /= length;
				
				distance += weights[i]*(1-c);
			}
			
			return distance;
		}

		/**
		 * Find the least distance possible to any entry in the bin
		 * @param bin
		 * @return
		 */
		public double leastDistance(CGQHierarchicalIndex<T1>.Bin bin) {
			double categoricalSimilarity 	= CGQSimEdgesHelper (0, NumCategoricalFeatures(), bin.canonicalEdge);
			double nonCategoricalSimilarity	= MaxNonCategoricalsimilarity (bin.veUpper, bin.veLower);
			
			return (1-(categoricalSimilarity + nonCategoricalSimilarity));
		}
	}

	/**
	 * class used to find the h-hop neighbourhood
	 *
	 */
	class NodeHopCount   {
		Node node;
		int hopCount;
		
		public NodeHopCount(Node n, int h) {
			node     = n;
			hopCount = h;
		}
	}
	
	//weight of each feature
	//this is relevant only in query graph
	double [] weights;
	
	//variables
	ArrayList<Node> nodeList;	// list of nodes in the graph
	HashSet  <Edge> edgeSet; 	// list of edges in the graph
	
	//methods
	/**
	 * 
	 * @param n number of node (This can be approximate value)
	 * @param directed true if the graph is directed
	 */
	Graph (int n, boolean directed) {
		nodeList = new ArrayList<>(n);
		edgeSet  = new HashSet<>(n);
		
		DiGraph  = directed;
	}

	/**
	 * Find the value of weights but hard set the value of one feature
	 * @param probability
	 * @param index
	 * @param weightVal
	 */
	void UpdateWeightsWithHardSetting(Probability probability, 
			int index, double weightVal) {
		UpdateWeights (probability);
		
		if (weightVal >= 1.0) {
			System.err.println("Invalid weight rewuested");
		}
		
		//set the weight to new value
		double w = 1-weights[index]; //weight of all the features but index
		double n = weightVal/w;
		
		for (int i=0; i<probability.numFeatures; i++) {
			weights [i] *= n;
		}
		
		weights[index] = weightVal;
	}
	
	/**
	 * find the weight associated with each feature
	 * @param probability
	 */
	void UpdateWeights (Probability probability) {
		int numFeatures = probability.numFeatures;
		weights = new double [numFeatures];
		
		double []chiSquared = new double [numFeatures];
		
		int numEdges = edgeSet.size();
		
		//store the stat
		ArrayList<HashMap<String, Integer>> featurePairMaps = new ArrayList<>(numFeatures);
		for (int i=0; i<numFeatures; i++) {
			HashMap<String, Integer> featurePairMap = new HashMap<>();
			featurePairMaps.add(featurePairMap);
		}
		
		//for each edge and each feature
		for (Edge edge : edgeSet) {
			ArrayList<ArrayList<String>> featureValuePairList = edge.AllFeatureValues();
			
			int i=-1; //index of the fature
			for (ArrayList<String> featureValuePairs : featureValuePairList) {
				i++;
				HashMap<String, Integer> map = featurePairMaps.get(i);
				
				for (String featureValuePair : featureValuePairs) {
					Integer count = map.get(featureValuePair);
					
					if (count == null)
						count = 1;
					else
						count++;
					
					map.put(featureValuePair, count);
				}
			}
		}
		
		//all feature pairs have been counted
		//calculate the chi-squared value
		double chiSum=0;
		for (int featureIndex=0; featureIndex<numFeatures; featureIndex++) {
			HashMap<String, Integer> featurePairMap = featurePairMaps.get(featureIndex);
			double chi=0;
			
			for (Map.Entry<String, Integer> entry : featurePairMap.entrySet()) {
				String featurePair  = entry.getKey();
				double count		= (double)entry.getValue();
				
				double prob 		= probability.getProbability (featureIndex, featurePair);
				
				double expectedCount= prob*numEdges;
				
				double deviation	= (count - expectedCount)*(count - expectedCount)/expectedCount;
				chi += deviation;
			}
			
			chiSquared[featureIndex] = chi;
			chiSum += chi;
		}
		
		for (int featureIndex=0; featureIndex<numFeatures; featureIndex++) {
			weights[featureIndex] = chiSquared[featureIndex]/chiSum;
		}
	}
	
	/**
	 * get a array of all features in the graph
	 * @return
	 */
	public String[] AllFeatures () {
		return nodeList.get(0).AllFeatures();
	}
	public int NumFeatures () {
		return nodeList.get(0).NumFeatures();
	}
	
	/*
	 * return the number of bidirectional edges in a graph
	 */
	int NumBidirectionalEdges () {
		int n = 0;
		
		if (DiGraph) {
			for (Node node:nodeList){
				n += NumBidirectionalEdgesInNode (node);
			}
			n /= 2;
		} else {
			n = edgeSet.size();
		}
		return n;
	}

	/*
	 * return the number of bidirectional edges in a node
	 */
	private int NumBidirectionalEdgesInNode(Node node) {
		int n = 0;
		
		if (DiGraph) {
			for (Node neighbour:node.neighbours) {
				if (neighbour.neighbours.contains(node))
					n++;
			}
		} else {
			n = node.edges.size();
		}
		return n;
	}
	/*
	 * count the number of non bidirectional edges
	 */

	public int NumNonBidirectionalEdges() {
		int n = 0;
		
		for (Node node:nodeList){
			n += NumNonBidirectionalEdgesInNode (node);
		}
		return n;
	}

	/*
	 * return the number of bidirectional edges in a node
	 */
	private int NumNonBidirectionalEdgesInNode(Node node) {
		return node.edges.size()-NumBidirectionalEdgesInNode(node);
	}
	
	/*
	 * return the number of nodes in the graph
	 */
	int SizeNodes () {
		return nodeList.size();
	}

	/*
	 * return the number of nodes in the graph with non zero degree
	 */
	int SizeNodesNonZero () {
		int z=0;
		for (Node node : nodeList) {
			if (node.Degree() == 0)
				z++;
		}
		
		return (nodeList.size()-z);
	}

	/*
	 * the edges are considered to be directed
	 */
	int SizeEdges () {
		return edgeSet.size();
	}

	/*
	 * Do some basic analysis of the graph
	 */
	void Analyse () throws IOException	{
		System.out.println();
		//AnalyseComponents ();
		//AnalyseTriangles  ();
		System.out.println ("Number of nodes    : "+SizeNodes());
		System.out.println ("Number of edges    : "+SizeEdges());
		System.out.println ("Number of features : "+NumFeatures());
		System.out.println ("Average degree     : "+(double)SizeEdges()/SizeNodes());
		
		//System.out.println("Press enter");
		//System.in.read();
	}
	
	/*
	 * counts the number of triangles for undirected graph
	 */
	void AnalyseTriangles() {
		int numTriangles=0;
		
		for  (Node node:nodeList){
			HashSet<Node> neighboursConsidered = new HashSet<>();
			
			for (Node neighbour1:node.neighbours){
				neighboursConsidered.add(neighbour1);
				for (Node neighbour2:node.neighbours){
					if (neighboursConsidered.contains(neighbour2)){
						//this is to avoid multiple counting of a triangle
						//ie to get nC2 pairs
						continue;
					}
					//see if a triangle
					if (neighbour1.neighbours.contains(neighbour2)){
						numTriangles++;
					}
				}
			}
		}
		
		//we have counted each triangle three times
		System.out.println(numTriangles);
		numTriangles /= 3;
		System.out.println("#triangles : "+numTriangles);
	}

	/*
	 * count the number of connected components and there histogram.
	 */
	void AnalyseComponents () {
		//number of connected components
		int numComponents = 0;
		HashSet<Integer> countedNodes = new HashSet<>();
		TreeMap<Integer, Integer> componentSizeMap = new TreeMap<>();
		
		for  (Node node:nodeList){
			if (!countedNodes.contains(node.nodeID)){
				numComponents++;
				countedNodes.add(node.nodeID);
				//calculate the size of the component
				int componentSize=1;
				LinkedList<Node> neighbourList = new LinkedList<>();
				
				neighbourList.add(node);
				Node currentNode;
				
				while((currentNode = neighbourList.poll()) != null) {
					for (Node neighbour:currentNode.neighbours){
						if(countedNodes.contains(neighbour.nodeID)){
							continue;
						}
						componentSize++;
						neighbourList.add(neighbour);
						countedNodes.add(neighbour.nodeID);
					}
				}
				
				//update the componentSizeMap histogram
				int n = 1;
				if (componentSizeMap.containsKey(componentSize)){
					n = componentSizeMap.get(componentSize);
					n++;
				}
				componentSizeMap.put(componentSize, n);
			}
		}
		System.out.println("#connected componets   : "+numComponents);
		System.out.println("Average component size : "+(float)nodeList.size()/numComponents);
		//Histogram of component size 
		System.out.println("Size   :   num");
		for (Entry<Integer, Integer> size:componentSizeMap.entrySet()){
			System.out.println(size.getKey()+","+size.getValue()+",");
		}		
	}
	
	/**
	 * Generate an array of random graphs, such that each "m" size graph is
	 * subgraph of "m+1" subgraph. The size of each graph is number of nodes
	 * @param mStart number of nodes in smallest graph
	 * @param mEnd number of nodes in largest graph
	 * @return
	 */
	ArrayList<Graph<T1>> GetRandomSubgraphArrayNode (double p, int mStart,
			int mEnd, boolean debug) {
		ArrayList<Graph<T1>> subgraphArray;
		
		while (true) {
			subgraphArray = new ArrayList<>();
			
			ArrayList<Node> graphNodes = GetRandomConnectedNodes (p, mEnd);
			
			if (debug) {
				System.out.print("Node Ids : ");
				for (Node node : graphNodes)
					System.out.print(node.nodeID+",");
				System.out.println();
				
				//store the nodes in a static variable to be accessed later
				RandomSubgraphArrayNode = new ArrayList<>(graphNodes.size());
				for (Node node : graphNodes)
					RandomSubgraphArrayNode.add(node.nodeID);
			}
			
			for (int m=mStart; m<=mEnd; m++) {
				List<Node> arrNodes = graphNodes.subList(0, m);
				subgraphArray.add(ConvertNodesToGraph(arrNodes));
			}
			
			if (subgraphArray.size() == (mEnd-mStart+1))
				break;
			else
				subgraphArray = null;
		}
		
		return subgraphArray;
	}
	/**
	 * Display the target graph nodes used to generate the random subgraph
	 */
	void DisplayPreviousRandomSubgraphArrayNode () {
		System.out.print("The node in target graph used to generate reusult : ");
		System.out.println(RandomSubgraphArrayNode);
		RandomSubgraphArrayNode=null;
	}
	
	ArrayList<Graph<T1>> GetRandomSubgraphArrayNodeFromFile (String fName, int mStart,
			int mEnd) throws IOException {
		ArrayList<Graph<T1>> subgraphArray;
		
		subgraphArray = new ArrayList<>();

		ArrayList<Node> graphNodes = GetConnectedNodesFromFile (fName);

		for (int m=mStart; m<=mEnd; m++) {
			List<Node> arrNodes = graphNodes.subList(0, m);
			subgraphArray.add(ConvertNodesToGraph(arrNodes));
		}

		return subgraphArray;
	}
	
	private ArrayList<Node> GetConnectedNodesFromFile (String fName) throws IOException {
		ArrayList<Node> arr = new ArrayList<>();
		
		//start reading the file
		String l;
		BufferedReader br = new BufferedReader(new FileReader(fName));
		
		l = br.readLine();

		String S[] = l.split(",");
		for (String s: S) {
			int id = Integer.parseInt(s);
			arr.add(nodeList.get(id));
		}
		
		br.close();
		
		return arr;
	}
	
	/**
	 * generate a random List of connected Nodes, the list is ordered
	 * such that each sublist 0..k is also connected
	 * @param p probability of selecting an Node, is used internally
	 * @param size number of edges returned
	 * @return
	 */
	ArrayList<Node> GetRandomConnectedNodes (double p, int size){
		ArrayList<Node> nodes;
		
		while (true) { //loop to ensure that some non empty subgraph is returned
			nodes = new ArrayList<>();
			
			//pick one random nodes.
			Node e = nodeList.get(random.nextInt(nodeList.size()));
			
			//Nodes to be considered
			LinkedList<Node> nodesToBeConsidered = new LinkedList<>();
			nodesToBeConsidered.add(e);
			
			//FIXME SERIOUS should I use Permutaion?
			//SERIOUS
			while ((nodesToBeConsidered.size() > 0) &&
					(nodes.size() != size)) {
				int r = random.nextInt(nodesToBeConsidered.size());
				e = nodesToBeConsidered.remove(r);

				nodes.add(e);
				
				//add its neighbouring edges
				HashSet<Node> set = new HashSet<>();
				
				set.addAll(e.neighbours);
				if (DiGraph)
					set.addAll(e.iAmNeighbourTo);
				
				set.removeAll(nodesToBeConsidered); //to avoid duplicates
				set.removeAll(nodes);
				
				nodesToBeConsidered.addAll(set);
				set = null;
			}			
			
			nodesToBeConsidered = null;
			if (nodes.size() == size)
				break;
			else
				nodes = null;
		}
		
		return nodes;
	}
	
	private Graph<T1> ConvertNodesToGraph (List<Node> arrNodes) {
		//count the number of nodes in the graph
		HashSet<Node> nodes = new HashSet<>(arrNodes);
		HashSet<Edge> edges = new HashSet<>();
		
		for (Node node:nodes) {
			for (Edge e : node.edges) {
				if (nodes.contains(e.node1) &&
						nodes.contains(e.node2)) {
					edges.add(e);
				}
			}
		}
		
		Graph<T1> graph = new Graph<>(arrNodes.size(), DiGraph);
		
		int nextNodeId = 0;
		HashMap<Integer, Integer> nodeIDMaps = new HashMap<>(); //map to find the new node id from old node id
		
		//adding node to graph
		for (Node node:arrNodes) {
			graph.AddNode (nextNodeId, node.features);
			nodeIDMaps.put(node.nodeID, nextNodeId);
			
			nextNodeId++;
		}
		//adding edge to the graph
		for(Edge e: edges) {
			graph.AddEdge (nodeIDMaps.get(e.node1.nodeID),
					nodeIDMaps.get(e.node2.nodeID));
		}
		
		return graph;
	}
	
	/**
	 * Generate an array of random graphs, such that each "m" size graph is
	 * subgraph of "m+1" subgraph. The size of each graph is number of edges
	 * @param mStart number of edges in smallest graph
	 * @param mEnd number of edges in largest graph
	 * @return
	 */
	ArrayList<Graph<T1>> GetRandomSubgraphArrayEdge (double p, int mStart, int mEnd) {
		ArrayList<Graph<T1>> subgraphArray;
		
		while (true) {
			subgraphArray = new ArrayList<>();
			
			ArrayList<Edge> graphEdges = GetRandomConnectedEdges (p, mEnd);
			
			for (int m=mStart; m<=mEnd; m++) {
				List<Edge> arrEdges = graphEdges.subList(0, m);
				subgraphArray.add(ConvertEdgesToGraph(arrEdges));
			}
			
			if (subgraphArray.size() == (mEnd-mStart+1))
				break;
			else
				subgraphArray = null;
		}
		
		return subgraphArray;
	}

	/**
	 * generate a random List of connected Edges, the list is ordered
	 * such that each sublist 0..k is also connected
	 * @param p probability of selecting an edge, is used internally
	 * @param size number of edges returned
	 * @return
	 */
	ArrayList<Edge> GetRandomConnectedEdges (double p, int size){
		ArrayList<Edge> edges;
		
		while (true) { //loop to ensure that some non empty subgraph is returned
			edges = new ArrayList<>();
			
			//pick one random Edge.
			Edge e = GetARandomEdge();
			
			//edges to be considered
			LinkedList<Edge> edgesToBeConsidered = new LinkedList<>();
			edgesToBeConsidered.add(e);
			
			//FIXME SERIOUS should I use Permutaion?
			//SERIOUS
			while ((edgesToBeConsidered.size() > 0) &&
					(edges.size() != size)) {
				int r = random.nextInt(edgesToBeConsidered.size());
				//e = edgesToBeConsidered.poll();
				e = edgesToBeConsidered.remove(r);
				
				//randomly choose it
				//if (random.nextDouble() > p)
				//	continue;
				
				edges.add(e);
				
				//add its neighbouring edges
				HashSet<Edge> set = new HashSet<>();
				
				set.addAll(e.IncidentEdges());
				
				set.removeAll(edgesToBeConsidered); //to avoid duplicates
				set.removeAll(edges);
				
				edgesToBeConsidered.addAll(set);
				set = null;
			}			
			
			edgesToBeConsidered = null;
			if (edges.size() == size)
				break;
			else
				edges = null;
		}
		
		return edges;
	}


	/*
	 * generate a random subgraph with "size" number of nodes
	 * the probability of growing the subgraph is taken as parameter
	 */
	Graph<T1> GetARandomSubgraph (double p, int size){		
		HashSet<Node>    nodesOfGraph;

		if (size == 0)
			return null;

		while (true) { //loop to ensure that some non empty subgraph is returned
			LinkedList<Node> nodesToBeConsidered = new LinkedList<>();

			nodesOfGraph = new HashSet<>();

			int r = random.nextInt(nodeList.size());
			nodesToBeConsidered.add(nodeList.get(r));

			//we shall now do a random walk and grow the graph
			Node node;

			while ((node = nodesToBeConsidered.poll()) != null){

				if (random.nextDouble() > p)
					//we discard the node
					continue;

				nodesOfGraph.add(node);
				if (nodesOfGraph.size() == size)
					break;

				for (Node neighbour:node.neighbours){
					nodesToBeConsidered.add(neighbour);
				}

				if (DiGraph) {
					for (Node neighbour:node.iAmNeighbourTo){
						nodesToBeConsidered.add(neighbour);
					}
				}
				
				if (nodesOfGraph.size() == size)
					break;
			}

			// condition that we have the graph of required size.
			if (nodesOfGraph.size() == size)
				break;
			
			nodesOfGraph 	    = null;
			nodesToBeConsidered = null;

		}

		return (ConvertNodeSetToGraph(nodesOfGraph));
	}

	/**
	 * Add an edge between source and destination
	 * @param source
	 * @param destination
	 */
	void AddEdge (int source, int destination) {
		Node s = nodeList.get(source);
		Node d = nodeList.get(destination);
		Edge e = null;

		if (s!=null && d!=null){
			if (s.neighbours.contains(d)) {
				//we shall not add duplicate edges
			} else {
				e = s.AddNeighbour(d);
				edgeSet.add(e);
			}
		}
	}
	
	/**
	 * class to add edges only beyond a threshold
	 * @author jithin
	 *
	 */
	class EdgeThreshold {
		final int threshold;
		//map from source to destination
		private HashMap<Integer, HashMap<Integer, EdgeThresholdEntry>> edgeMapMap = new HashMap<>();
		
		class EdgeThresholdEntry {
			private int count=0;
		}
		
		EdgeThreshold (int t) {
			threshold = t;
		}
		
		/**
		 * @param source
		 * @param destination
		 */
		void AddEdgeThreshold (Integer source, Integer destination) {
			Integer node1, node2;
			
			if (DiGraph) {
				node1 = source;
				node2 = destination;
			} else {
				if (source > destination) {
					node2 = source;
					node1 = destination;
				} else {
					node1 = source;
					node2 = destination;
				}
			}
			
			//add to the map
			HashMap<Integer, EdgeThresholdEntry> edgeMap = edgeMapMap.get(node1);
			if (edgeMap == null) {
				edgeMap = new HashMap<>();
				edgeMapMap.put(node1, edgeMap);
			}
			
			EdgeThresholdEntry entry = edgeMap.get(node2);
			
			if(entry == null) {
				entry = new EdgeThresholdEntry();
				edgeMap.put (node2, entry);
			}
			
			entry.count++;
		}
		
		void AddEdgesToGraph (int maxID) {
			for (Map.Entry<Integer, HashMap<Integer, EdgeThresholdEntry>> mapMap : edgeMapMap.entrySet()){
				Integer node1 = mapMap.getKey();
				if (node1 >= maxID)
					continue;
				HashMap<Integer, EdgeThresholdEntry> edgeMap = mapMap.getValue();

				for (Map.Entry<Integer, EdgeThresholdEntry> map : edgeMap.entrySet()){
					Integer node2 = map.getKey();
					if (node2 >= maxID)
						continue;
					EdgeThresholdEntry entry = map.getValue();

					if (entry.count >= threshold) {
						//we can add the edge now
						AddEdge(node1, node2);
					}
				}
			}
			//free memory
			edgeMapMap = null;
		}
	}
	
	Node AddNode (int nodeID, T1 features) {
		Node node = new Node (nodeID, features);
		
		//we expect nodes to be added in numeric order
		// and hence the nodeID s should be same as the current size of arraylist.
		if (nodeID != nodeList.size()) {
			System.err.println("Fatal error nodes added to graph are out of order");
			System.err.println("nodeID = "+nodeID+" ||| nodeList.size() = "+nodeList.size());
			System.exit(-1);
		}
		nodeList.add(node);
		return node;
	}
	

	/* 
	 * function returns Neighbours within H hops of the egde
	 */
	public HashSet<NodeHopCount> GetHhopNeighboursFromEdge(Node u1, Node u2, int H) {
		HashSet<NodeHopCount> neighbours = new HashSet<>();
		HashSet<Node> nodesConsidered	 = new HashSet<>();
		
		
		LinkedList<NodeHopCount> nodesToBeConsidered = new LinkedList<>();
		
		//adding the two end points to be processed
		NodeHopCount nhc;

		nhc = new NodeHopCount(u1,0);
		nodesToBeConsidered.add(nhc);
		nhc = new NodeHopCount(u2,0);
		nodesToBeConsidered.add(nhc);
		
		//we are using a queue so will always have the smallest hop count for a node
		while ((nhc = nodesToBeConsidered.poll()) != null) {
			if (nodesConsidered.contains(nhc.node)) {
				// the node has already been considered
				continue;
			}
			nodesConsidered.add(nhc.node);
			neighbours.add(nhc);
			
			if (nhc.hopCount <= H){
				//the neighbours are with in H hops
				for (Node node : nhc.node.neighbours){
					NodeHopCount nhcLocal = new NodeHopCount(node, nhc.hopCount+1);
					//a node will be added with multiple hopCounts but
					//since nodesToBeConsidered is an linked list its ok, I hope.
					nodesToBeConsidered.add(nhcLocal);
				}
				if (nhc.node.iAmNeighbourTo != null)
					for (Node node : nhc.node.iAmNeighbourTo){
						NodeHopCount nhcLocal = new NodeHopCount(node, nhc.hopCount+1);
						//a node will be added with multiple hopCounts but
						//since nodesToBeConsidered is an linked list its ok, I hope.
						nodesToBeConsidered.add(nhcLocal);
					}
			}
		}
		
		return neighbours;
	}

	/**
	 * Function to extract all subgraphs of size "n",
	 * these subgraphs are then converted into new graphs themselves
	 * @param n
	 * @return
	 */
	public ArrayList<Graph<T1>> getAllNNodeGraphs(int n) {
		ArrayList<Graph<T1>> graphList = new ArrayList<>();
		
		HashSet<HashSet<Node>> connectedNodesSet = getAllConnectedNodes (n);

		for (HashSet<Node> set: connectedNodesSet)
			graphList.add(ConvertNodeSetToGraph (set));

		return graphList;
	}

	/**
	 * Function to extract all connected components of size "n",
	 * @param n
	 * @return
	 */
	public HashSet<HashSet<Node>> getAllConnectedNodes (int n) {
		HashSet<HashSet<Node>> connectedNodesSet = new HashSet<>();
		int b = nodeList.size();
		int a = 1;
		
		//we shall traverse through all the nodes
		//and traverse only its neighbours of higher nodeid
		for(Node node:nodeList){
			ShowProgress.ShowPercent(a++, b);

			//get n-1 nodes connected to this node
			ArrayList<HashSet<Node>> connectedNodesList = GetNConnectedNodes (node, n);
			
			//sanity check
			for (HashSet<Node> s : connectedNodesList) {
				if (s.size() != n) {
					System.err.println("getAllConnectedNodes n = "+n+" and our size is : "+s.size());
					System.exit(-1);
				}
			}
			connectedNodesSet.addAll(connectedNodesList);
		}
		return connectedNodesSet;
	}

	/**
	 * 
	 * @param node : the node from which we shall do a traversal
	 * @param n : number of nodes connected to the given "node", "n" should be >= 1
	 * @return connectedNodesList : returns  a list of all sets containing the n-nodes which are part of a
	 * connected component involving "node",
	 * NOTE : all nodes which have id greater than that of "node", 
	 * and nodes which do not have edge to this node but has a lesser ID
	 */
	private ArrayList<HashSet<Node>> GetNConnectedNodes(Node node, int n) {
		ArrayList<HashSet<Node>> connectedNodesList = new ArrayList<HashSet<Node>>();
		
		if (n==1){
			//we need to return this node itself
			HashSet<Node> set = new HashSet<>();
			set.add(node);
			
			connectedNodesList.add(set);
		} else {
			//we need to recurse
			for (Node neighbour : node.neighbours) {
				if (neighbour.nodeID < node.nodeID) {
					//we are only interested in nodes which have id greater than that of "node"
					// or nodes which do not have edge to this node but has a lesser ID (valid only for Digraph)
					if (DiGraph && neighbour.neighbours.contains(node))
						continue;
				}
				
				ArrayList<HashSet<Node>> connectedNodesList_n_1 = GetNConnectedNodes (neighbour, n-1);
				for (HashSet<Node> set:connectedNodesList_n_1) {
					set.add(node);
					//now the size is "n", there are cases when
					//it is not true so we add the if condition
					if (set.size() == n)
						connectedNodesList.add(set);
				}
			}
		}

		return connectedNodesList;
	}
	
	
	/**
	 * Given a set of nodes in the graph, generate the induced subgraph
	 * @param set
	 * @return
	 */
	private Graph<T1> ConvertNodeSetToGraph(HashSet<Node> set) {
		Graph<T1> graph 	= new Graph<>(set.size(), DiGraph);
		int nextNodeId 	= 0;
		HashMap<Integer, Integer> nodeIDMaps = new HashMap<>(); //map to find the new node id from old node id
		
		//adding node to graph
		for (Node node:set) {
			graph.AddNode (nextNodeId, node.features);
			nodeIDMaps.put(node.nodeID, nextNodeId);
			
			nextNodeId++;
		}
		
		//adding edge to the graph
		for (Node node:set) {
			HashSet<Node> intersection = new HashSet<>(set);
			intersection.retainAll(node.neighbours);
			
			for (Node neighbour : intersection) {
				graph.AddEdge (nodeIDMaps.get(node.nodeID), nodeIDMaps.get(neighbour.nodeID));
			}
		}
		
		return graph;
	}

	Graph<T1> ConvertEdgesToGraph(List<Edge> arrEdges) {
		//count the number of nodes in the graph
		HashSet<Node> nodes = new HashSet<>();
		
		for(Edge e: arrEdges) {
			nodes.add(e.node1);
			nodes.add(e.node2);
		}
		
		Graph<T1> graph = new Graph<>(nodes.size(), DiGraph);
		
		int nextNodeId = 0;
		HashMap<Integer, Integer> nodeIDMaps = new HashMap<>(); //map to find the new node id from old node id
		
		//adding node to graph
		for (Node node:nodes) {
			graph.AddNode (nextNodeId, node.features);
			nodeIDMaps.put(node.nodeID, nextNodeId);
			
			nextNodeId++;
		}
		//adding edge to the graph
		for(Edge e: arrEdges) {
			graph.AddEdge (nodeIDMaps.get(e.node1.nodeID),
					nodeIDMaps.get(e.node2.nodeID));
		}
		
		return graph;
	}

	/**
	 * Returns the edge connecting source/node1 to destination/node2,
	 * returns null if no such edge exists
	 * @param source
	 * @param destination
	 * @return
	 */
	Edge GetEdge (Node node1, Node node2) {
		Edge edge = null;
		
		if (node1.neighbours.contains(node2)) {
			for (Edge e: node1.edges){
				if ((e.node1 == node2) ||
						(e.node2 == node2)){
					edge = e;
					break;
				}
			}
		}
		
		return edge;
	}

	/**
	 * Function return a random edge by first picking a random node and
	 * then a random edge going out of the node 
	 * @return
	 */
	public Graph<T1>.Edge GetARandomEdge() {
		Graph<T1>.Edge rEdge = null;
		
		while (rEdge == null){
			//pick a random node
			int r = random.nextInt(nodeList.size());
			
			Graph<T1>.Node node = nodeList.get(r);
			
			if (node.edges.size() > 0) {
				r = random.nextInt(node.edges.size());
				
				int i=0;
				for (Graph<T1>.Edge edge : node.edges) {
					if (i == r) {
						rEdge = edge;
						break;
					}
					
					i++;
				}
			}
		}
		
		return rEdge;
	}

	/**
	 * function to update the index structure saving the degree distribution of each node
	 */
	public void UpdateDegreeDist() {
		for (Node node: nodeList)
			node.UpdateDegreeDist();
	}

	/**
	 * Display the graph as a edge list
	 */
	public void Print() {
		String connector;
		if (DiGraph) {
			connector = "-->";
		} else {
			connector = "---";
		}
		
		DecimalFormat dFormat = new DecimalFormat("0.00");
		
		//printing weights
		System.out.println();
		System.out.print("Weights : ");
		for (double w : weights) {
			System.out.print(w+",");
		}
		System.out.println();
		
		//printing edges
		System.out.println();
		for (Edge e : edgeSet) {
			System.out.print (e.node1.nodeID+connector+e.node2.nodeID);
			//print ve
			System.out.print ("\t VE : ");
			for (int i=0; i<e.NumFeatures(); i++)
				System.out.print (dFormat.format(e.GetVe(i))+",");
			
			System.out.println();
		}
		
		//printing edges Nodes
		for (Node n : nodeList) {
			NodeFeatures features = n.features;
			
			features.Print();
		}
	}
	
	public void PrintCSV() {
		String connector;
		if (DiGraph) {
			System.out.print("Directed Graph");
			connector = "-->";
		} else {
			System.out.print("Undirected Graph");
			connector = "---";
		}
		
		DecimalFormat dFormat = new DecimalFormat("0.00");
		
		//printing edges
		System.out.println(",");
		System.out.println("Edge , VE, Weights");
		for (Edge e : edgeSet) {
			System.out.print (e.node1.nodeID+connector+e.node2.nodeID+",");
			//print ve
			for (int i=0; i<e.NumFeatures(); i++)
				System.out.print (dFormat.format(e.GetVe(i))+RAQ.Seperator);
			System.out.print(",");

			System.out.println();
		}
		
		//printing edges Nodes
		System.out.println(",");
		System.out.print("Node ID,");

		for (Node n : nodeList) {		
			NodeFeatures features = n.features;
			features.PrintCSVHeader();
			break;
		}

		int id = 0;
		for (Node n : nodeList) {
			NodeFeatures features = n.features;
			
			System.out.print(id++ + ",");
			features.PrintCSV();
		}
		System.out.println(",");
		System.out.println(",");
	}

	public void PrintCSVToFile(FileWriter fooWriter) throws IOException  {
		boolean printExtraComma = false;
		PrintCSVToFile(fooWriter, printExtraComma);
	}
	
	public void PrintCSVToFile(FileWriter fooWriter, boolean printExtraComma) throws IOException  {
		String connector;
		if (DiGraph) {
			fooWriter.write("Directed Graph");
			connector = "-->";
		} else {
			fooWriter.write("Undirected Graph");
			connector = "---";
		}
		
		DecimalFormat dFormat = new DecimalFormat("0.00");
		
		//printing edges
		fooWriter.write(",\n");
		fooWriter.write("Weights,");
		if (printExtraComma)
			fooWriter.write(",");
		
		for (int i=0; i<weights.length; i++) {
			fooWriter.write(weights[i]+", ");
		}
		fooWriter.write("\n");
		
		//fooWriter.write("Edge , VE, Weights,\n");
		fooWriter.write("Edge , VE,\n");
		for (Edge e : edgeSet) {
			fooWriter.write (e.node1.nodeID+connector+e.node2.nodeID+",");
			//print ve
			for (int i=0; i<e.NumFeatures(); i++)
				fooWriter.write (dFormat.format(e.GetVe(i))+RAQ.Seperator);
			fooWriter.write(",");
			//print weight
			/*
			for (int i=0; i<e.NumFeatures(); i++)
				fooWriter.write (dFormat.format(e.GetWeight(i))+RAQ.Seperator);
			fooWriter.write(",");
			*/
			fooWriter.write("\n");
		}
		
		//printing edges Nodes
		fooWriter.write(",\n");
		fooWriter.write("Node ID,");

		for (Node n : nodeList) {		
			NodeFeatures features = n.features;
			features.PrintCSVHeaderToFile(fooWriter);
			break;
		}

		int id = 0;
		for (Node n : nodeList) {
			NodeFeatures features = n.features;
			
			fooWriter.write(id++ + ",");
			features.PrintCSVToFile(fooWriter);
		}
		fooWriter.write(",\n");
		fooWriter.write(",\n");
	}

	/**
	 * Find the edge connecting node1 to node2
	 * @param node1
	 * @param node2
	 * @return
	 */
	Graph<T1>.Edge GetEdge(int n1, int n2) {
		Node node1 = nodeList.get(n1);
		Node node2 = nodeList.get(n2);
		
		Edge edge = node1.GetEdgeTo(node2);
		
		return edge;
	}

	
	/**
	 * Display the stat about the degree of nodes in the graph
	 */
	public void DegreeStat() {
		ArrayList<Integer> degrees = new ArrayList<>();
		ArrayList<Integer> inDegrees = new ArrayList<>();

		for (Node node : nodeList) {
			degrees.add(node.Degree());
			inDegrees.add(node.InDegree());
		}

		System.out.println("Degree");
		Helper.SortAndPrintStat(degrees);
		System.out.println("InDegree");
		Helper.SortAndPrintStat(inDegrees);
	}

	/**
	 * Get a set of list of edges which are connected and
	 * sublist 0-k is a subgraph of sublist 0-k+1
	 * The Edges picked are always trying to minimize the number of nodes.
	 * @param setSize
	 * @param listSize
	 * @return
	 */
	public ArrayList<ArrayList<Edge>> GetSetOfRandomListOfConnectedEdges (int setSize, int listSize) {
		ArrayList<ArrayList<Edge>> ret = new ArrayList<>(setSize);
		//we use this to avoid duplicate entries
		ArrayList<HashSet<Edge>> edgesConsidered = new ArrayList<>();
		
		while (ret.size() < setSize) {
			ArrayList<Edge> arr = GetRandomListOfConnectedEdges(listSize);
			
			HashSet<Edge> edges = new HashSet<>(arr);
			
			boolean duplicate = false;
			
			//check if this has been used already 
			for (HashSet<Edge> oldSet : edgesConsidered) {
				if (oldSet.containsAll(edges)) {
					//this is a duplicate entry
					duplicate = true;
					break;
				}
			}
			
			if (!duplicate) {
				ret.add(arr);
				edgesConsidered.add(edges);
			}
		}
		
		//printing the results
		/*
		for (ArrayList<Edge> arr : ret) {
			HashMap<Integer, Integer> nTonMap = new HashMap<>();
			Integer i=0;
			for (Edge e : arr) {
				Integer n1 = e.node1.nodeID;
				Integer n2 = e.node2.nodeID;

				Integer a1 = nTonMap.get(n1);
				Integer a2 = nTonMap.get(n2);
				if (a1 == null) {
					a1 = i++;
					nTonMap.put(n1, a1);
				}
				if (a2 == null) {
					a2 = i++;
					nTonMap.put(n2, a2);
				}
				
				System.out.print(a1+","+a2+",");
			}
			System.out.println();
		}
		*/
		//System.out.println("******************************************");
		//printing the results
		for (ArrayList<Edge> arr : ret) {
			for (Edge e : arr) {
				Integer n1 = e.node1.nodeID;
				Integer n2 = e.node2.nodeID;
				System.out.print(n1+","+n2+",");
			}
			System.out.println();
		}
	
		
		return ret;
	}
	
	/**
	 * Returns a list of edges which are connected and
	 * sublist 0-k is a subgraph of sublist 0-k+1
	 * The Edges picked are always trying to minimize the number of nodes.
	 * @param size
	 * @return
	 */
	public ArrayList<Edge> GetRandomListOfConnectedEdges (int size) {
		ArrayList<Edge> arr;
		Edge e;

		do {
			//pick a random edge from the entire graph
			e   = GetARandomEdge();
			arr = new ArrayList<>(size);

			HashSet<Edge> edgesToConsider = new HashSet<>();
			HashSet<Edge> edgesConsidered = new HashSet<>();
			//these are edges which have priority over other edges to be picked
			HashSet<Edge> specialedges	  = new HashSet<>();

			HashSet<Node> nodesConsidered = new HashSet<>();

			boolean picked;
			do {
				arr.add(e);
				edgesConsidered.add(e);
				nodesConsidered.add(e.node1);
				nodesConsidered.add(e.node2);

				HashSet<Edge> incidentEdges = e.IncidentEdges();

				for (Edge edge : incidentEdges) {
					if (!edgesConsidered.contains(edge)) {
						if (nodesConsidered.contains(edge.node1) &&
								nodesConsidered.contains(edge.node2)) {
							//this is a special edge
							specialedges.add(edge);
						} else {
							edgesToConsider.add(edge);
						}
					}
				}

				//pick a random edge from
				specialedges.removeAll   (edgesConsidered);
				edgesToConsider.removeAll(edgesConsidered);
				
				picked = false;
				if (!specialedges.isEmpty()) {
					picked = true;
					e = getRandomEdgeFromSet(specialedges);
				} else if (!edgesToConsider.isEmpty()) {
					picked = true;
					e = getRandomEdgeFromSet(edgesToConsider);
				}
			} while ((picked == true) &&
					(arr.size() < size));
		} while (arr.size() < size);
		
		return arr;
	}
	
	/**
	 * Returns a edges picked uniformly at random from the parameter
	 * if the set is empty return null 
	 * @param edgeSet
	 * @return
	 */
	private Edge getRandomEdgeFromSet (HashSet<Edge> edgeSet) {
		Edge ret = null;
		int size = edgeSet.size();
		if (size > 0) {
			int n = random.nextInt(size);
			int i=0;
			for (Edge edge : edgeSet) {
				if (i == n) {
					ret = edge;
					break;
				}
				i++;
			}
		}
		return ret;
	}

	/**
	 * Load the query graphs from file to main memory, as a list of edges
	 * @param queryfname
	 * @return
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public ArrayList<ArrayList<Edge>> LoadQueryGraphEdgeList(String queryfname)
			throws NumberFormatException, IOException {
		ArrayList<ArrayList<Edge>> queryGraphList = new ArrayList<>();

		BufferedReader br = new BufferedReader(new FileReader(queryfname));
		//read each line and convert it into graph
		String  l;
		while ((l = br.readLine())!=null) {
			String []s = l.split(",");

			ArrayList<Edge> arr = new ArrayList<>();
			
			int i=0;
			while (i<s.length-1) {
				int node1 = Integer.parseInt(s[i++]);
				int node2 = Integer.parseInt(s[i++]);

				//get edge connecting node1 to node2
				Edge e = GetEdge(node1, node2);
				
				arr.add(e);
			}
			
			queryGraphList.add(arr);
		}
		br.close();
		
		return queryGraphList;
	}

	/**
	 * Return a array of graphs of size between mStart and mEnd
	 * @param mStart
	 * @param mEnd
	 * @param arrayList
	 * @return
	 */
	public ArrayList<Graph<T1>> GetRandomSubgraphArrayFromEdgeList
	(int mStart, int mEnd, ArrayList<Edge> arr) {
		ArrayList<Graph<T1>> ret = new ArrayList<>(mEnd-mStart+1);
		
		for (int size=mStart; size<=mEnd; size++) {
			List<Edge> egdes = arr.subList(0, size);
			
			ret.add(ConvertEdgesToGraph(egdes));
		}
		
		return ret;
	}

	public Node GetNode(Integer nodeID) {
		return nodeList.get(nodeID);
	}

	/**
	 * See if the nodes associated with the edge have any node in the graph
	 * The nodes are compared based on a unique identifier which need not be its feature
	 * @param tEdge
	 * @return
	 */
	public boolean EdgeTouched(Graph<T1>.Edge tEdge) {
		return (NodeTouched (tEdge.node1) || NodeTouched (tEdge.node2));
	}

	/**
	 * See if the node has similarity of 1
	 * the nodes are compared based on a unique identifier which need not be its feature
	 * @param node
	 * @return
	 */
	public boolean NodeTouched(Graph<T1>.Node tNode) {
		boolean ret = false;
		
		for (Node node : nodeList) {
			if (node.IsSameAs (tNode)) {
				ret = true;
				break;
			}
		}
		return ret;
	}

	
	/**
	 * Find the traditional distance between the graph and a corresponding mapping
	 * @param mapping
	 * @return
	 */
	public double TraditionalDistance(ArrayList<Node> mapping) {
		double distance=0;
		
		if(mapping.size() < SizeNodes()) {
			System.err.println("TraditionalDistance : The mapping is incomplete");
		}
		
		//find the distance between nodes
		for (int i=0; i<mapping.size(); i++) {
			Node mNode = mapping.get(i);
			
			if (mNode == null)
				distance += 1.0;
			else
				distance += nodeList.get(i).Distance(mapping.get(i));
		}

		//consider the missing nodes
		distance += nodeList.size() - mapping.size();
		
		//find the distance between edges
		//add 1 for each missing edge
		
		//find the nodes mapped to
		for (Edge edge : edgeSet) {
			int i1 = nodeList.indexOf(edge.node1);
			int i2 = nodeList.indexOf(edge.node2);
			
			if (i1 < mapping.size() && i2<mapping.size()) {
				Node tNode1 = mapping.get(i1);
				Node tNode2 = mapping.get(i2);
			
				if (tNode1 == null || tNode2 == null)
					distance += 1.0;
				else {
					//handles both directed and undirected graphs
					if(!tNode1.neighbours.contains(tNode2))
						distance += 1.0;
				}
			} else {
				//mapping is missing
				distance += 1.0;
			}
		}
		
		return distance;
	}

	
	/**
	 * filter a edge based on the features of the nodes
	 * @param tEdge
	 * @return true if the node is to filtered out
	 */
	public boolean FilterEdge(Edge qEdge, Edge tEdge) {
		boolean filterOut;
		
		//filter the edge based on exact match
		filterOut = qEdge.node1.features.Filter (qEdge, tEdge);
		
		if (! filterOut) {
			//filter the nodes based on some criterion
			filterOut = (FilterNode(tEdge.node1) || FilterNode(tEdge.node2));
		}
		
		return filterOut;
	}

	/**
	 * filter node on the features
	 * @param tNode
	 * @return true if the node is to filtered out
	 */
	boolean FilterNode(Node tNode) {
		return tNode.features.Filter();
	}


	public void PrintNodesToFile(String nodefname) throws IOException {
		File myFoo = new File(nodefname);
		FileWriter fooWriter = new FileWriter(myFoo, false); 
		
		for (Node node : nodeList) {
			fooWriter.write("nodeID,");
			node.features.PrintCSVHeaderToFile(fooWriter);
			break;
		}
		
		for (Node node : nodeList) {
			fooWriter.write(node.nodeID+",");
			node.features.PrintCSVToFile(fooWriter);
		}
		
		fooWriter.close();
	}

	public void PrintEdgesToFile(String edgefname) throws IOException {
		File myFoo = new File(edgefname);
		FileWriter fooWriter = new FileWriter(myFoo, false);
		
		fooWriter.write("nodeID1, nodeID2\n");
		
		for (Edge edge : edgeSet) {
			fooWriter.write(edge.node1.nodeID+","+edge.node2.nodeID+"\n");
		}
		
		fooWriter.close();
	}

	/**
	 * Make the weights uniform
	 */
	public void UpdateWeightsUniformly() {
		for (Edge edge : edgeSet) {
			int numFeatures = edge.NumFeatures(); 
			double w        = 1.0/numFeatures;
			
			weights = new double [numFeatures];

			for (int i=0; i<numFeatures; i++)
				weights[i] = w;
			
			break;
		}
	}
	
	/**
	 * We do not want one feature to dominate the show, hence we cap the weights
	 * @param probability
	 */
	public void UpdateWeightsWithCap(Probability probability) {
		final double cap = 0.4;
		
		UpdateWeights(probability);
		
		//now cap the weights
		boolean loop;

		do {
			loop = false;
			int numAdjustments = 0;
			double w = 0;
			//lets put a cap on the weights
			for (int i=0; i<weights.length; i++) {
				if (weights[i] >= cap) {
					if (weights[i] > cap)
						loop = true;

					weights[i] = cap;
					numAdjustments++;	
				} else {
					w += weights[i];
				}
			}

			// adjust other weights
			for (int i=0; i<weights.length; i++) {
				if (weights[i] < cap) {
					//we need to adjust it
					weights[i] = (weights[i]/w)*(1.0-(cap*numAdjustments));
				}
			}

		}while(loop); //loop to ensure no one is > cap

		//check if we did right job
		double w=0;
		for (int i=0; i<weights.length; i++) {
			w += weights[i];
		}
		//System.out.println(weightsArray);
		if (w < 0.99) {
			System.err.println("w : is not correct DBLPAlternateQ.UpdateWeights");
			System.exit(-1);
		}
	}

	
	/**
	 * Update the summary vector of all the edges
	 */
	public void updateNeighbourhoodSummary(boolean showProgress) {
		TicToc.Tic();
		final int size = edgeSet.size();
		/*
		int i=1;
		for (Edge edge : edgeSet) {
			ShowProgress.ShowPercent(i++, size);
			edge.updateNeighbourhoodSummary();
		}
		*/
		
		ShowProgress.ShowIntAndIncrementReset();
		final class Worker implements Runnable {
			Edge edge;
			Worker (Edge edge) {
				this.edge = edge;
			}
			
			@Override
			public void run() {
				edge.updateNeighbourhoodSummary();
				synchronized (edgeSet) {
					ShowProgress.ShowIntAndIncrement(size);
				}
			}
		}
		
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		
		for (Edge edge : edgeSet) {
			Runnable worker = new Worker(edge);
			
			executor.submit(worker);
			//worker.run();
		}
		
		executor.shutdown();
		
        while (!executor.isTerminated()) {   }
		TicToc.Toc();
	}
}
