import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

public class RAQ extends Helper{
	public static final int    H				= 1;
	public static final int    BeamWidth		= 50;
	public static final int    WidthThreshold	= 100;
	public static final double Delta			= 0.2;			//decay factor
	public static final double penaltyDist 		= 1.0;
	public static final double weightThreshold 	= 0.75;
	public static final boolean gatherStat 		= false;
	public static final boolean test	 		= false;
	
	//FIXME set this to false to make things fast
	public static final boolean slowAndSafe		= false;
	public static final boolean interruptable	= true;

	public static boolean edgeGraph = false; //used to generate random graphs
	public static int BranchingFactor = 22;
		
	public static String Seperator="[:]";
	
	enum IndexingScheme {
		BaseCase,
		CGQHierarchicalindex,
		All, CGQHierarchicalindexFuzzy,
		CGQHierarchicalindexFuzzyBFS,
		BFSWOIndex, 
		BFSWOHeuristics, //run without using the heuristics
	}
	
	enum DataSet {
		exit, DBLP,
		DBLPAlternate,
		Pokec, IMDB;

		static int maxLen = getMaxlen();

		public static DataSet ConvertIntToDataSet(int input) {
			DataSet ret = exit;
			
			for (DataSet e : DataSet.values()) {
				if (e.ordinal() == input) {
					ret = e;
					break;
				}
			}
			return ret;
		}
		
		private static int getMaxlen() {
			int ret = -1;
			for (DataSet experiments : DataSet.values()) {
				ret = Math.max(ret, experiments.toString().length());
			}
			return ret;
		}

		public String Padding() {
			int l = this.toString().length();
			String ret = "";
			
			for (int i=0; i<maxLen-l; i++)
				ret+=" ";
			
			return ret;
		}
	}

	enum DataSetSigmod {
		exit, DBLP, coAuthor, Pokec;

		static int maxLen = getMaxlen();

		public static DataSetSigmod ConvertIntToDataSet(int input) {
			DataSetSigmod ret = exit;
			
			for (DataSetSigmod e : DataSetSigmod.values()) {
				if (e.ordinal() == input) {
					ret = e;
					break;
				}
			}
			return ret;
		}
		
		private static int getMaxlen() {
			int ret = -1;
			for (DataSetSigmod experiments : DataSetSigmod.values()) {
				ret = Math.max(ret, experiments.toString().length());
			}
			return ret;
		}

		public String Padding() {
			int l = this.toString().length();
			String ret = "";
			
			for (int i=0; i<maxLen-l; i++)
				ret+=" ";
			
			return ret;
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		if (interruptable)
			InterruptSearchSignalHandler.listenTo("INT");
		if (args.length < 10) {
			/*
			System.out.println("Insufficient number of arguments");
			System.out.print  ("Usage : java CGQ \"Data file\" \"Num of nodes in Smallest Query graphs\" ");
			System.out.print  ("\"Num of nodes in Biggest Query graphs\" ");
			System.out.print  ("\"Num of Query graphs\" ");
			System.out.print  ("\"Indexing Scheme(-1:Base case 0:TA 1:Degree 2:TA modified 3:TA modified_chi 4:CGQINDEX 6:run all(except TA))\" ");
			System.out.println("\"KStart \" ");
			System.out.println("\"KEnd \" ");
			System.out.println("\"KStep \" ");
			System.out.print  ("\"Data set type (0:Amazon 1:NetworkX 2:NorthEast DBLP\" ");
			System.out.println("\"TestFileName \" ");
			System.out.println();
			
			System.out.println("The program also can be used to add features to the random graph generated by networkX");
			System.out.println("Usage : java CGQ \"Data file\" ");
			System.out.println();

			//System.out.println("By PRESS ENTER to see Old results : ");
			
			/*
			ResultLogger.ShowResultsAll	(50, true, "NetworkX_50000_100000");
			System.out.println("By PRESS ENTER to see Old results : ");
			System.in.read();
			
			//ResultLogger.ShowResultsAll	(50, true, "NetworkX_50000_200000");
			//System.out.println("By PRESS ENTER to see Old results : ");
			//System.in.read();
			
			ResultLogger.ShowResultsAll	(50, true, "NetworkX_50000_2000000");
			*?
			/*
			for (int i=100; i<101; i++) {
				//ResultLogger.ShowResults(i, false);
				ResultLogger.ShowResultsAll	(K, true, "Amazon");
			}
			for (int i=1; i<101; i++) {
				//ResultLogger.ShowResults(i, true);
				//ResultLogger.ShowResultsAll	(K, true);
			}
			*/
			//String dataSet = "NorthEast5";
			/*
			TicToc.Tic();
			String dataSet = "DBLP";
			System.out.println(dataSet+"5");
			System.in.read();
			ResultLogger.ShowResultsAll	(5, edgeGraph, dataSet);
			System.out.println("By PRESS ENTER to see Old results : ");
			System.out.println(dataSet+"50");
			System.in.read();
			ResultLogger.ShowResultsAll	(50, edgeGraph, dataSet);
			*/
			
			Regular ();
			//SIGMOD  ();
			//Sayan ();
		} else {
			int i=1;
			int mStart	 		= Integer.parseInt(args[i++]);
			int mEnd	 		= Integer.parseInt(args[i++]);
			int numQuery 		= Integer.parseInt(args[i++]);
			
			IndexingScheme indexingScheme = IndexingScheme.valueOf(args[i++]);
			
			int kStart 	= Integer.parseInt(args[i++]);
			int kEnd   	= Integer.parseInt(args[i++]);
			int kStep	= Integer.parseInt(args[i++]);
			ArrayList<Integer> K = new ArrayList<>();
			for (int k=kStart; k<=kEnd; k+=kStep) {
				K.add(k);
			}
			
			DataSet dataSet 	= DataSet.valueOf(args[i++]);
			String tFname       = args[i++];

			switch (dataSet) {
			case DBLP:
				DBLP.Process (args[0], mStart,mEnd, numQuery, indexingScheme, tFname, false, K);
				break;
			case DBLPAlternate:
				DBLPAlternate.Process (args[0], mStart,mEnd, numQuery, indexingScheme, tFname, false, K);
				break;
			case exit:
				break;
			case Pokec:
				System.out.println("Unimplemented");
				break;
			case IMDB:
				System.out.println("IMDB");
			}
			
			//ResultLogger.ShowResults	(K, true);
			//ResultLogger.ShowResultsAll	(K, true);
		}
	}
	

	@SuppressWarnings("unused")
	private static void Sayan() throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		DBLPAlternate.DumpNetworkToFile();
	}

	private static void Regular() throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		if (false)
			Experiments.GenerateRandomGraphs();
		
		do {
			//run experiments
			//Experiments.PrintStat();
			Experiments.RunExperiments();
			//print results
			Experiments.PrintResults();
		} while (!Experiments.Conclude());
	}

	private static void PrintHeaderHelper (String s, int l) {
		System.out.print(s);
		for (int i=0; i<l-s.length(); i++)
			System.out.print(" ");
		System.out.println(" *");
	}
	static void PrintHeader (String dataset, int mStart, int mEnd,
			int numQuery, RAQ.IndexingScheme indexingScheme, ArrayList<Integer> K){
		int l;
		String star  ="**********************************";
		String scheme = indexingScheme.toString();
		
		l = dataset.length()+1;
		if (scheme.length()+1 > l)
			l = scheme.length()+1;
		
		for (int i=0; i<l; i++) {
			star += "*";
		}
		
		System.out.println(star);
		System.out.print  ("* Data set                    : ");
		PrintHeaderHelper(dataset, l);
		
		System.out.print  ("* K                           : ");
		String kStr = "";
		for(Integer k : K) {
			kStr += k+",";
		}
		PrintHeaderHelper(kStr, l);
		
		System.out.print  ("* Num of Edges in query graph : ");
		PrintHeaderHelper(Integer.toString(mStart)+" - "+Integer.toString(mEnd),l);
		
		System.out.print  ("* Num of query graph          : ");
		PrintHeaderHelper(Integer.toString(numQuery),l);
		
		System.out.print  ("* Indexing Scheme             : ");
		PrintHeaderHelper(scheme,l);

		
		System.out.println(star);
		System.out.println();
	}
	
	/**
	 * find all permutations of a set.
	 * @param set
	 * @return
	 */
	private static <T> ArrayList<ArrayList<T>> GetPermutations (HashSet<T> set) {
		ArrayList<ArrayList<T>> perm = new ArrayList<>();
		
		if (set.size() != 0) {
			if (set.size() == 1) {
				ArrayList<T> arr = new ArrayList<>();

				arr.addAll(set);
				perm.add(arr);
			} else {
				for (T s : set) {
					HashSet<T> newSet = new HashSet<>(set);
					newSet.remove(s);
					
					ArrayList<ArrayList<T>> permIntermediate = GetPermutations(newSet);
					for (ArrayList<T> p : permIntermediate) {
						p.add(s);
					}
					
					perm.addAll(permIntermediate);
				}
			}
		}
		return perm;
	}
	
	static <T1 extends NodeFeatures> 
	void LogResultsAll(RAQ.IndexingScheme indexingScheme, Long elapsedTime, 
			CallByReference<Integer>depth, 
			CallByReference<Long> 	numDFS,
			CallByReference<Long> 	numEdgesTouched,
			ArrayList<ObjectDoublePair<ArrayList<Graph<T1>.Node>>> topKTA,
			ArrayList<Long> 	runTimeArr,
			ArrayList<Integer> 	depthArr,
			ArrayList<Long> 	numDFSArr,
			ArrayList<Long>		numEdgesTouchedArr,
			ArrayList<String> 	indexingSchemeArr,
			ArrayList<Integer> 	numResultsFoundArr,
			ArrayList<Double> 	avgDistanceArr) {
		indexingSchemeArr.add (indexingScheme.toString());
		runTimeArr.add        (elapsedTime);
		depthArr.add          (depth.element);
		numDFSArr.add         (numDFS.element);
		numEdgesTouchedArr.add(numEdgesTouched.element);
		numResultsFoundArr.add(topKTA.size());
		
		double dist = 0.0;
		for (ObjectDoublePair<ArrayList<Graph<T1>.Node>> o : topKTA)
			dist += o.value;
		avgDistanceArr.add(dist);
	}

	/**
	 * Find the distance between the query graph "g" and the set of nodes 
	 * from the target graph
	 * @param g
	 * @param targetGraph
	 * @param nodes
	 * @return
	 */
	static <T1 extends NodeFeatures> double 
	CGQDistGraphAndSubgraph(Graph<T1> g,
			Graph<T1> targetGraph, HashSet<Graph<T1>.Node> nodes ){
		double dist=1.0;
		
		if (g.SizeNodes() != nodes.size()) {
			System.err.println("g.SizeNodes() : "+g.SizeNodes()+" nodes.size() : "+nodes.size());
			System.err.println("The size of two graphs do not match in function CGQDistGraphs");
			System.exit(-1);
			dist = 1.0;
		} else {
			//we have find the distance as per all possible bijection between vertex sets
			//all possible bijections are of the form [0,..,n-1]*[all possible permutations of the set "nodes"]
			
			ArrayList<ArrayList<Graph<T1>.Node>> permutations = 
					GetPermutations(nodes);
			
			//traversing through all possible bijection
			for (ArrayList<Graph<T1>.Node> permutation : permutations) {
				//node i in graph g will map to node permutation[i]
				// /F_{\phi}(q,g)=\sum_{\forall e=(u,v)\in E}s(e,(\phi(u),\phi(v)))
				//A map is valid only if; $(u,v)\in E$ if and only if $(\phi(u),\phi(b))\in E'$
				if (GraphToNodeMapValid (g,targetGraph, permutation)) {
					double distTemp = CGQDistQueryGraphToMap (g,targetGraph, permutation);

					if (dist > distTemp)
						dist = distTemp;
				}
			}
		}
		
		return dist;
	}
	/**
	 * Function to find distance between a query graph and the mapped nodes in target graph
	 * @param qGraph
	 * @param targetGraph
	 * @param map is a permutation of nodes target graph nodes, node_i in
	 * 			query graph is mapped to i_th node in the permutation
	 * @return
	 */
	static <T1 extends NodeFeatures> double
	CGQDistQueryGraphToMap(Graph<T1> qGraph,
			Graph<T1> targetGraph,
			ArrayList<Graph<T1>.Node> map) {
		double dist = 0.0;
		for (Graph<T1>.Edge edge : qGraph.edgeSet) {
			int qNode1 = edge.node1.nodeID;
			int qNode2 = edge.node2.nodeID;

			//nodes to which these nodes are mapped
			Graph<T1>.Node node1 = map.get(qNode1);
			Graph<T1>.Node node2 = map.get(qNode2);

			Graph<T1>.Edge mappedEdge   = targetGraph.GetEdge (node1, node2);

			dist += edge.CGQDistEdges(mappedEdge);
		}
		
		return dist;
	}

	/**
	 * Function to find if the given map, via the permutation is a valid map
	 * @param g
	 * @param targetGraph
	 * @param map is a map of nodes target graph nodes, node_i in
	 * 			query graph is mapped to i_th node in the permutation
	 * @return
	 */
	static <T1 extends NodeFeatures> boolean
	 GraphToNodeMapValid(Graph<T1> qGraph,
				Graph<T1> targetGraph,
				ArrayList<Graph<T1>.Node> map) {
		//A map is valid only if; $(u,v)\in E$ if and only if $(\phi(u),\phi(b))\in E'$
		//node i in graph g will map to node map[i]
		
		//check if all edge in g have a correspondence in the target graph 
		for (Graph<T1>.Edge edge : qGraph.edgeSet) {
			int qNode1 = edge.source.nodeID;
			int qNode2 = edge.destination.nodeID;

			//nodes to which these nodes are mapped
			Graph<T1>.Node node1 = map.get(qNode1);
			Graph<T1>.Node node2 = map.get(qNode2);

			Graph<T1>.Edge mappedEdge   = targetGraph.GetEdge (node1, node2);
			
			if (mappedEdge == null)
				return false;
		}
		
		//check if all edge in target graph have a correspondence in qGraph

		HashSet<Graph<T1>.Node> nodeSet = new HashSet<>(map);

		for (int i=0; i<map.size(); i++) {
			Graph<T1>.Node node=map.get(i);

			for (Graph<T1>.Node neighbour : node.neighbours){
				if (nodeSet.contains(neighbour)) {
					int j = map.indexOf(neighbour);

					//we have a edge from i to j
					//does our graph also have?

					Graph<T1>.Node n1 = qGraph.nodeList.get(i);
					Graph<T1>.Node n2 = qGraph.nodeList.get(j);

					if (!n1.neighbours.contains(n2))
						//the edge is missing
						return false;
				}
			}
		}
		
		return true;
	}
	
	static <T1 extends NodeFeatures>
	void AnalyzeVeVectorBinary (Graph<T1> graph) {
		int s = graph.edgeSet.iterator().next().GetVe().length;
		int size = (int) Math.pow(2, s);
		int []distribution = new int [size];
				
		System.out.println("AnalyzeVeVectorBinary  size : "+size);
		for (Graph<T1>.Edge edge : graph.edgeSet) {
			double[] ve = edge.GetVe();
			
			//converting ve to integer value
			int sum = 0;
			int p = 1;
			for (int i=(ve.length-1); i>=0; i--) {
				sum += ve[i]*p;
				p *=2;			
			}
			
			distribution[sum]++;
		}
		
		int total = 0;
		for (int i=0; i<size; i++) {
			System.out.println(i+","+Integer.toBinaryString(i)+","+distribution[i]);
			total += distribution[i];
		}
		
		if (total != graph.edgeSet.size())
			System.out.println("Edge size does not match : AnalyzeVeVectorBinary");
	}

	static <T1 extends NodeFeatures>
	void AnalyzeDegreeDistribution (Graph<T1> graph) {
		TreeMap<Integer, Integer> map = new TreeMap<>();
		int maxDegree=0;
		
		System.out.println("AnalyzeDegreeDistribution");

		for (Graph<T1>.Node node : graph.nodeList) {
			int degree = node.TotalDegree();
			
			if (degree > maxDegree)
				maxDegree = degree;
			
			Integer count = map.get(degree);
			
			if (count == null) {
				count = 0;
			}
			map.put(degree, count+1);
		}
		
		for (int i=0; i<=maxDegree; i++) {
			Integer count = map.get(i);
			
			if (count == null)
				count = 0;
			
			System.out.println(i+","+count);
		}
	}

}
