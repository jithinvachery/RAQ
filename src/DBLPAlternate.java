import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class DBLPAlternateFeatures extends NodeFeatures {
	//categorical
	//none
	//non-categorical
	final HashSet<Integer> venues;
	final HashSet<Integer> subjects;
	final int medianRank;
	final int yearActive;
	final int numPapers;

	//this feature is not a feature but an info
	final int authorId;

	static final int numCategoricalFeatures    = 0;
	static final int numNonCategoricalFeatures = DBLPAlternate.numFeatures - numCategoricalFeatures;

	public final static int numFeatures = DBLPAlternate.numFeatures;

	public DBLPAlternateFeatures(DBLPAlternateEntry entry) {
		venues 		= entry.Venues();
		subjects 	= entry.Subjects();
		medianRank	= entry.MedianRank();
		yearActive 	= entry.ActiveYears();
		numPapers 	= entry.NumPapers();
		authorId 	= entry.AuthorID();
	}

	@Override
	String[] AllFeatures() {
		return DBLPAlternate.allFeaturesStrings;
	}

	@Override
	int NumFeatures() {
		return numFeatures;
	}

	@Override
	int NumCategoricalFeatures() {
		return numCategoricalFeatures;
	}
	@Override
	int NumNonCategoricalFeatures() {
		return numNonCategoricalFeatures;
	}

	@Override
	double[] GetVE(NodeFeatures features) {
		double[] ve = new double[numFeatures];
		DBLPAlternateFeatures nf = (DBLPAlternateFeatures)features;

		//processing non-categorical features

		HashSet<Integer> intersection, union;

		// we use jaccard distance, intersection by union
		//venues
		intersection = new HashSet<>(venues);
		union		 = new HashSet<>(venues);
		intersection.retainAll(nf.venues);
		union.addAll(nf.venues);

		GetVeHelper(ve, intersection.size(), union.size(), DBLPAlternate.allFeatures.venues.ordinal());

		//subjects
		intersection = new HashSet<>(subjects);
		union		 = new HashSet<>(subjects);
		intersection.retainAll(nf.subjects);
		union.addAll(nf.subjects);

		GetVeHelper(ve, intersection.size(), union.size(), DBLPAlternate.allFeatures.subjects.ordinal());
		//freeing the memory
		intersection = null;
		union = null;

		//medianRank
		GetVeHelper(ve, medianRank, nf.medianRank, DBLPAlternate.allFeatures.medianRank.ordinal());

		//yearActive
		GetVeHelper(ve, yearActive, nf.yearActive, DBLPAlternate.allFeatures.yearsActive.ordinal());

		//numPapers
		GetVeHelper(ve, numPapers, nf.numPapers, DBLPAlternate.allFeatures.numPapers.ordinal());

		return ve;
	}

	@Override
	public void Print() {
		System.out.print("numPapers : "		+numPapers);		
		System.out.print(" yearActive : "	+yearActive);		
		System.out.print(" venues : "		+venues);		
		System.out.print(" medianRank : "	+medianRank);		
		System.out.print(" subjects : "		+subjects);		

		System.out.println();

		System.out.println("*** actual values ***");
		System.out.print("Author : "+DBLPEntry.GetAuthorName(authorId)+" ");
		System.out.print(" numPapers : "		+numPapers);		
		System.out.print(" yearActive : "	+yearActive);
		System.out.print(" venues : ");
		for (Integer v : venues)
			System.out.print(DBLPEntry.GetVenueName(v)+",");
		System.out.print(" medianRank : "	+medianRank);
		System.out.print(" Subjects : ");
		for (Integer s : subjects)
			System.out.print(ConferenceInfo.GetSubjectReverse(s)+",");
		System.out.println();
	}

	public void PrintShort() {
		System.out.print(AuthorNameFormated()+":");
		System.out.print(String.format("%-3d", numPapers)		+":");		
		System.out.print(String.format("%-3d", yearActive)		+":");
		System.out.print(String.format("%-3d", venues.size())	+":");
		System.out.print(String.format("%-3d", medianRank)		+":");
		System.out.print(String.format("%-3d", subjects.size()));
	}

	@Override
	public void PrintCSV() {
		System.out.print(DBLPEntry.GetAuthorName(authorId)+",");
		System.out.print(numPapers+",");		
		System.out.print(yearActive+",");

		if (venues.size() == 0)
			System.out.print("null,");
		else {
			for (Integer v : venues)
				System.out.print(DBLPEntry.GetVenueName(v)+RAQ.Seperator);
		}
		System.out.print(",");

		System.out.print(medianRank+",");

		if (subjects.size() == 0)
			System.out.print("null,");
		else {
			for (Integer s : subjects)
				System.out.print(ConferenceInfo.GetSubjectReverse(s)+RAQ.Seperator);
		}
		System.out.print(",");
		System.out.println();
	}

	public void PrintCSVToFile(FileWriter fooWriter) throws IOException {
		fooWriter.write(DBLPEntry.GetAuthorName(authorId)+",");
		fooWriter.write(numPapers+",");		
		fooWriter.write(yearActive+",");

		if (venues.size() == 0)
			fooWriter.write("null");
		else {
			for (Integer v : venues)
				fooWriter.write(DBLPEntry.GetVenueName(v)+RAQ.Seperator);
		}
		fooWriter.write(",");

		fooWriter.write(medianRank+",");

		if (subjects.size() == 0)
			fooWriter.write("null");
		else {
			for (Integer s : subjects)
				fooWriter.write(ConferenceInfo.GetSubjectReverse(s)+RAQ.Seperator);
		}
		fooWriter.write(",\n");
	}

	@Override
	public void PrintCSVHeader() {
		System.out.print ("Author (Not a feature),");
		System.out.print ("numPapers,"); 		
		System.out.print ("yearsActive,");
		System.out.print ("venues,");    
		System.out.print ("medianRank,");
		System.out.print ("Subjects,"); 
		System.out.println();
	}

	public void PrintCSVHeaderToFile(FileWriter fooWriter) throws IOException {
		fooWriter.write ("Author,");
		fooWriter.write ("numPapers,"); 		
		fooWriter.write ("yearActive,");
		fooWriter.write ("venues,");    
		fooWriter.write ("medianRank,");
		fooWriter.write ("Subjects,"); 
		fooWriter.write ("\n");
	}

	public int MedianRank() {
		return medianRank;
	}

	public int NumPapers() {
		return numPapers;
	}

	public int YearsActive() {
		return yearActive;
	}

	public HashSet<Integer> Subjects() {
		return subjects;
	}

	public HashSet<Integer> Venues() {
		return venues;
	}

	static public void PrintStat() {
		System.out.println("num of Venues                   : "+DBLPAlternateEntry.NumVenues());
		System.out.println("num of Subjects                 : "+DBLPAlternateEntry.NumSubjects());
		System.out.println("num of Years (diff)             : "+DBLPAlternateEntry.MaxYearsActive());
		System.out.println("num of Papers(diff)             : "+DBLPAlternateEntry.NumDiffPapers());
		System.out.println("max num of Papers per authur    : "+DBLPAlternateEntry.MaxPapersPerAuthor());

		//free memory
		DBLPAlternateEntry.Destroy();
	}

	public String AuthorName() {
		return DBLPEntry.GetAuthorName(authorId);
	}

	public String AuthorNameFormated() {
		String authorName = DBLPEntry.GetAuthorName(authorId);
		authorName = String.format("%-20s", authorName);

		return authorName;
	}

	@Override
	public double Distance(NodeFeatures tFeatures) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean NodeIsSameAs(NodeFeatures tFeatures) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean Filter() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean Filter(Graph.Edge qEdge, Graph.Edge tEdge) {
		return false;
	}

	@Override
	ArrayList<ArrayList<String>> AllFeatureValues(NodeFeatures feature) {
		DBLPAlternateFeatures neighbourFeature = (DBLPAlternateFeatures)feature;

		ArrayList<ArrayList<String>> ret = new ArrayList<>(numFeatures);

		for (int i=0; i<numFeatures; i++) {
			ArrayList<String> s = new ArrayList<>(); 
			ret.add(s);
		}

		do {
			boolean diGraph = false;
			ArrayList<String> S;
			//all this mombo jumbo  of using switch case, to ensure we do not miss out a feature
			DBLPAlternate.allFeatures allF = DBLPAlternate.allFeatures.medianRank;
			switch (allF) {
			case medianRank:
				S = ret.get(DBLPAlternate.allFeatures.medianRank.ordinal());
				S.add(getString(medianRank, neighbourFeature.medianRank, diGraph));
			case numPapers:
				S = ret.get(DBLPAlternate.allFeatures.numPapers.ordinal());
				S.add(getString(numPapers, neighbourFeature.numPapers, diGraph));
			case subjects:
				S = ret.get(DBLPAlternate.allFeatures.subjects.ordinal());
				for (int sub1 : subjects) {
					for (int sub2 : neighbourFeature.subjects) {
						S.add(getString(sub1, sub2, diGraph));
					}
				}
			case venues:
				S = ret.get(DBLPAlternate.allFeatures.venues.ordinal());
				for (int v1 : venues) {
					for (int v2 : neighbourFeature.venues) {
						S.add(getString(v1, v2, diGraph));
					}
				}
			case yearsActive:
				S = ret.get(DBLPAlternate.allFeatures.yearsActive.ordinal());
				S.add(getString(yearActive, neighbourFeature.yearActive, diGraph));
				break;
			}
		} while (false);

		return ret;
	}

	@Override
	int[][] getSummaryVector(double[] ve) {
		//allocating memory
		int[][] neighbourhoodSummary = new int[NumFeatures()][BFSQuery.HeuristicsNumBin];

		//summarize the values take by feature
		//all this mombo jumbo  of using switch case, to ensure we do not miss out a feature
		int i, j;
		DBLPAlternate.allFeatures allF = DBLPAlternate.allFeatures.medianRank;
		switch (allF) {
		case medianRank:
			i = DBLPAlternate.allFeatures.medianRank.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case numPapers:
			i = DBLPAlternate.allFeatures.numPapers.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case subjects:
			i = DBLPAlternate.allFeatures.subjects.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case venues:
			i = DBLPAlternate.allFeatures.venues.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case yearsActive:
			i = DBLPAlternate.allFeatures.yearsActive.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		}

		return neighbourhoodSummary;
	}
}

public class DBLPAlternate {
	public static final String WeightsFName 	= "../data/DBLPAlternate_Weights_H"+RAQ.H+"_test_"+DBLPHelper.test;
	public static final String queryFName   	= "../data/dblp/coAuthor"+(DBLPHelper.test?"_test_true":"")+".query";
	public static final String queryFNameBase   = "../data/DBLPAlternate_test_"+DBLPHelper.test+"TargetSizeExperiment.query";
	public static final String nodeFname	   	= "../data/DBLPAlternateNode.txt";
	public static final String edgeFname	   	= "../data/DBLPAlternateEdge.txt";

	//generate partial graph
	static final boolean test = DBLPEntry.test;
	static final int testID   = DBLPEntry.testID;

	static private Graph <DBLPAlternateFeatures> DBLPAlternateGraph;
	static private ArrayList<ArrayList<Graph <DBLPAlternateFeatures>.Edge>> queryGraphList;

	//this is the order of features used
	enum allFeatures {
		numPapers, yearsActive, venues, medianRank, subjects;

		public static String[] strings() {
			String []ret = new String[allFeatures.values().length];

			int i=0;
			for (allFeatures features:allFeatures.values()) {
				ret[i++] = features.toString();
			}

			return ret;
		}
	}

	static final int numFeatures = allFeatures.values().length;
	static final  String[] allFeaturesStrings = allFeatures.strings();
	static double[][] Prob;
	static Probability probability;

	static private CGQHierarchicalIndex<DBLPAlternateFeatures> DBLPCGQhierarchicalIndex;
	private static String dataSet = RAQ.DataSet.DBLPAlternate.toString();

	/**
	 * Function to process the DBLP dataset
	 * @param fName : the data file of amazon
	 * @param mStart the number of nodes in smallest query graph
	 * @param mEnd the number of nodes in largest query graph
	 * @param numQuery the number of query graphs to be generated
	 * @param indexingScheme 0:TA 1:Degree Based
	 * @param kList 
	 * @param runBaseCase 
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	static void Process (String fName, int mStart, int mEnd, int numQuery,
			RAQ.IndexingScheme indexingScheme, String testFile,
			boolean useTestFile, ArrayList<Integer> kList)
					throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		String[] name = fName.split("/");
		RAQ.PrintHeader ("DBLP Alternate:"+name[name.length-1], mStart, mEnd,
				numQuery, indexingScheme, kList);
		LoadGraph (fName, indexingScheme);

		System.out.println("By PRESS ENTER to start the experiment : ");
		System.in.read();

		//RAQ.AnalyzeVeVectorBinary (DBLPAlternateGraph);
		//RAQ.AnalyzeDegreeDistribution (DBLPAlternateGraph);

		if (useTestFile) {
			numQuery = 1;
		}
		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<DBLPAlternateFeatures>> queryGraphArray;
			System.out.println("Generating Random graphs : ");
			if (RAQ.edgeGraph)
				queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayEdge (0.4, mStart, mEnd);
			else {
				if (useTestFile)
					queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayNodeFromFile (testFile, mStart, mEnd);
				else 
					queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayNode (0.4, mStart, mEnd, true);
			}

			for (Graph<DBLPAlternateFeatures> queryGraph : queryGraphArray) {
				//we now have a query graph having m nodes
				queryGraph.UpdateWeights (probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);
				queryGraph.Print();

				for (int K : kList) {
					//Find the topK based on our indexing scheme
					ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>>>
					topKTA=null;

					System.out.print("Finding topK based on ");

					Helper.CallByReference<Integer> depth 			= new Helper.CallByReference<>();
					Helper.CallByReference<Long> 	numEdgesTouched = new Helper.CallByReference<>();
					Helper.CallByReference<Long> 	numDFS			= new Helper.CallByReference<>();

					CGQTimeOut.startTimeOut(); 
					TicToc.Tic();
					switch (indexingScheme) {
					case BaseCase:
					case BFSWOIndex:
					case BFSWOHeuristics:
						System.out.println("NOT IMPLEMENTED");
						break;
					case CGQHierarchicalindex:
						System.out.print("CGQ Index for query graph "+i+" :     ");
						CGQHierarchicalIndexQuery<DBLPAlternateFeatures> cgqHierarcicalIndexQuery =
								new CGQHierarchicalIndexQuery<>();
						topKTA = cgqHierarcicalIndexQuery.GetTopKSubgraphs(K, queryGraph, DBLPAlternateGraph,
								DBLPCGQhierarchicalIndex, false, depth, numDFS, numEdgesTouched, false);
						break;
					case All:
						System.out.println(" all techniques");
						ArrayList<Integer> 	depthArr 			= new ArrayList<>();
						ArrayList<Long>    	numDFSArr			= new ArrayList<>();
						ArrayList<Long>    	runTimeArr 			= new ArrayList<>();
						ArrayList<Double> 	avgDistanceArr 		= new ArrayList<>();
						ArrayList<String> 	indexingSchemeArr 	= new ArrayList<>();
						ArrayList<Integer> 	numResultsFoundArr	= new ArrayList<>();
						ArrayList<Long> 	numEdgesTouchedArr	= new ArrayList<>();

						Long elapsedTime;

						depth.element 			= 0;
						numDFS.element 			= 0L;
						numEdgesTouched.element = 0L;

						System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
						CGQTimeOut.startTimeOut(); 
						TicToc.Tic();
						boolean avoidTAHeuristics = !Helper.useHeuristics;

						topKTA = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
								DBLPCGQhierarchicalIndex, true, depth, numDFS, 
								numEdgesTouched, true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
						elapsedTime=TicToc.Toc();
						CGQTimeOut.stopTimeOut();
						if (InterruptSearchSignalHandler.Interrupt()) {
							//we were interrupted from keyboard
							InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
							//return;
							Helper.searchInterruptMesseage ();
						}

						System.out.println("Number of results : "+topKTA.size());
						System.out.println("Target graph size : "+DBLPAlternateGraph.SizeEdges());
						System.out.println();
						RAQ.LogResultsAll (RAQ.IndexingScheme.CGQHierarchicalindexFuzzyBFS,
								elapsedTime, depth, numDFS, numEdgesTouched,
								topKTA, runTimeArr, depthArr, numDFSArr, numEdgesTouchedArr,
								indexingSchemeArr, numResultsFoundArr, avgDistanceArr);
						Helper.PrintTopK (topKTA);

						if (!test) {
							ResultLogger.LogResultAll(queryGraph.SizeNodes(), queryGraph.SizeEdges(),
									runTimeArr, false, depthArr, numDFSArr, numEdgesTouchedArr,
									indexingSchemeArr, K, numResultsFoundArr, avgDistanceArr,
									"DBLPAlternate_"+DBLPHelper.edgeThreshold+"_");
						}
						break;
					case CGQHierarchicalindexFuzzy:
						//TODO
						break;
					case CGQHierarchicalindexFuzzyBFS:
						//TODO
						break;
					}

					switch (indexingScheme) {
					case All:
						break;
					default:
						Long elapsedTime = TicToc.Toc();
						CGQTimeOut.stopTimeOut();
						int numResults = topKTA.size();
						System.out.println("Found "+numResults+" subgraphs");

						double dist = 0.0;
						for (Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>>
						o : topKTA)
							dist += o.value;

						System.out.println("Avearge distance of Index based search : "+(dist/numResults));
						if (!test) {
							ResultLogger.LogResult(queryGraph.SizeNodes(), queryGraph.SizeEdges(),
									elapsedTime, false, depth.element, numDFS.element,
									numEdgesTouched.element, indexingScheme, K, numResults,
									(dist/numResults), "DBLPAlternate_"+DBLPHelper.edgeThreshold+"_");
						}
						break;
					}

					//this will print for the last last
					Helper.PrintTopK (topKTA);
				}
			}
		}
	}

	/**
	 * Lad the graph to memory
	 * @param fName
	 * @param indexingScheme
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	static private void LoadGraph (String fName, RAQ.IndexingScheme indexingScheme)
			throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		boolean useHeuristics = Helper.useHeuristics;
		CreateGraph (fName, 1.0);

		System.out.println("Num edges                       : "+DBLPAlternateGraph.SizeEdges());

		switch (indexingScheme) {
		case All:
			//CreateTAIndex(useChi);		
			//ConvertCGQIndex();			
			CreateCGQHierarcicalIndex(RAQ.BranchingFactor, useHeuristics);
			break;
		case CGQHierarchicalindex:
		case CGQHierarchicalindexFuzzy:
		case BFSWOIndex:
			CreateCGQHierarcicalIndex(RAQ.BranchingFactor, useHeuristics);
			break;
		case CGQHierarchicalindexFuzzyBFS:
			CreateCGQHierarcicalIndex(RAQ.BranchingFactor, useHeuristics);
			break;
		case BaseCase:
			break;
		case BFSWOHeuristics:
			useHeuristics = true;
			CreateCGQHierarcicalIndex(RAQ.BranchingFactor, useHeuristics);
			break;
		}
	}

	private static void CreateCGQHierarcicalIndex(int branchingFactor, boolean useHeuristics) throws InterruptedException, ExecutionException {
		int a;
		int b;
		a=0;
		b=DBLPAlternateGraph.nodeList.size();
		System.out.print("Creating CGQHierarchicalindex   :     ");
		ShowProgress.ShowPercent(a, b);

		//TicToc.Tic();
		DBLPCGQhierarchicalIndex = new CGQHierarchicalIndex<DBLPAlternateFeatures>
		(DBLPAlternateGraph.edgeSet,
				DBLPAlternateFeatures.numCategoricalFeatures,
				DBLPAlternateFeatures.numNonCategoricalFeatures, branchingFactor, useHeuristics);
		
		//for heuristics all edges have to be summarized
		boolean showProgress = true;
		if (useHeuristics)
			DBLPAlternateGraph.updateNeighbourhoodSummary(showProgress);

		/*
		//create local index structure in each node
		System.out.print("\nCreating local index structures :     ");
		for (Graph<DBLPAlternateFeatures>.Node node : DBLPAlternateGraph.nodeList) {
			ShowProgress.ShowPercent(++a, b);
			node.cgqHierarchicalIndex = new CGQHierarchicalIndex<DBLPAlternateFeatures> 
								(node.edges,
								DBLPAlternateFeatures.numCategoricalFeatures,
								DBLPAlternateFeatures.numNonCategoricalFeatures);
		}
		 */
		System.out.println();
		//System.out.print("created in this time : ");
		//TicToc.Toc();
	}

	/**
	 * The graph data structure is being populated
	 * @param fName
	 * @param frac 
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void CreateGraph(String fName, double frac) 
			throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {		
		//loading data to memory
		TreeMap<Integer, DBLPAlternateEntry> dblpEntries;

		System.out.print("Loading data into memory        :     ");
		//parse the CORE file
		ConferenceInfo.Init(fName);
		dblpEntries	= DBLPAlternateEntry.loadDataset(fName);

		System.out.println();
		System.out.print("Loading the graph to memory     :     ");

		DBLPAlternateGraph = new Graph<>(17525000, false);

		//allocate memory for probability
		probabilityInit();

		//Add nodes, and their features values
		int id=0;
		int maxID = (int) (dblpEntries.size()*frac);

		Iterator<Map.Entry<Integer, DBLPAlternateEntry>> it = dblpEntries.entrySet().iterator();

		while (it.hasNext()) {
			if (id == maxID)
				break;
			DBLPAlternateEntry entry = it.next().getValue();

			//add each entry in to the graph as node
			DBLPAlternateFeatures dblpFeatures = new DBLPAlternateFeatures(entry);

			if (dblpFeatures.authorId != id) {
				System.err.println("Fatal error DBLPAlternate.CreateGraph : we expect author ids to be sequential");
				System.exit(-1);
			}
			DBLPAlternateGraph.AddNode(id++, dblpFeatures);
			//we shall update  the statistic of each feature to
			//find the probability of the feature
			UpdateProbability (dblpFeatures);
			ShowProgress.Show();
		}
		//we now have a count of the features so we shall now make it a probability
		UpdateProbability (null);
		//ConferenceInfo.ShowUnRankedVenues();
		//free memory
		ConferenceInfo.Destroy();

		//add edges
		it = dblpEntries.entrySet().iterator();

		Graph<DBLPAlternateFeatures>.EdgeThreshold edgeThreshold =
				DBLPAlternateGraph.new EdgeThreshold(DBLPHelper.edgeThreshold);

		while (it.hasNext()) {
			DBLPAlternateEntry entry = it.next().getValue();

			HashSet<Integer> neighbours = entry.neighbours;

			//add edge from node->neighbour
			for (int neighbour : neighbours) {
				//neighbour nodeid
				//
				edgeThreshold.AddEdgeThreshold(entry.AuthorID(), neighbour);
				//if (entry.AuthorID() >= neighbour)
				//DBLPAlternateGraph.AddEdge(entry.AuthorID(), neighbour);
			}
			ShowProgress.Show();
		}
		edgeThreshold.AddEdgesToGraph(maxID);

		//we shall update  the statistic of each feature to
		//find the probability of the feature
		probability.UpdateProbability (DBLPAlternateGraph.edgeSet);

		//we now have a count of the features so we shall now make it a probability
		probability.UpdateProbability ();


		//freeing memory
		dblpEntries 	= null;
		edgeThreshold 	= null;

		System.out.println("");
		System.out.println("Num edges                       : "+DBLPAlternateGraph.SizeEdges());
		System.out.println("Num Nodes                       : "+DBLPAlternateGraph.SizeNodes());
		DBLPAlternateFeatures.PrintStat();
	}

	/**
	 * Allocate memory for probability
	 */
	private static void probabilityInit() {
		int []size =new int[numFeatures];
		do {
			//all this mombo jumbo to ensure we do not miss out a feature
			allFeatures allF = allFeatures.medianRank;
			switch (allF) {
			case medianRank:
			case numPapers:
			case subjects:
			case venues:
			case yearsActive:
				size[allFeatures.medianRank.ordinal()]	= DBLPAlternateEntry.NumRanks();
				size[allFeatures.numPapers.ordinal()]	= DBLPAlternateEntry.MaxPapersPerAuthor();
				size[allFeatures.venues.ordinal()]		= DBLPAlternateEntry.NumVenues();
				size[allFeatures.yearsActive.ordinal()]	= DBLPAlternateEntry.MaxYearsActive();
				size[allFeatures.subjects.ordinal()]	= DBLPAlternateEntry.NumSubjects();
				break;
			}
		} while (false);

		probability = new Probability(numFeatures, size);

		//allocating memory for probability
		//we are adding "1" since the values start from 1, and not  zero
		Prob = new double[numFeatures][];
		Prob[allFeatures.medianRank.ordinal()]	= new double [DBLPAlternateEntry.NumRanks()+1];
		Prob[allFeatures.numPapers.ordinal()]	= new double [DBLPAlternateEntry.MaxPapersPerAuthor()+1];
		Prob[allFeatures.venues.ordinal()]		= new double [DBLPAlternateEntry.NumVenues()];
		Prob[allFeatures.yearsActive.ordinal()]	= new double [DBLPAlternateEntry.MaxYearsActive()+1];
		Prob[allFeatures.subjects.ordinal()]	= new double [DBLPAlternateEntry.NumSubjects()];
	}

	private static void UpdateProbability(DBLPAlternateFeatures features)
			throws InterruptedException, ExecutionException {
		int numP = Runtime.getRuntime().availableProcessors();
		final class Worker implements Runnable {
			int _start,_end,_f;
			double _sum;
			Worker (int start, int end, int f, double sum) {
				_start 	= start;
				_end	= end;
				_sum	= sum;
				_f		= f;
			}

			@Override
			public void run() {
				for (int d=_start; d<_end; d++) {
					Prob[_f][d] /= _sum;
				}
			}
		}
		
		class Normalizer implements Callable<Double>{
			int _start,_end,_f;
			public Normalizer (int start, int end, int f) {
				_start 	= start;
				_end	= end;
				_f		= f;
			}
			
			@Override
			public Double call() throws Exception {
				Double _sum=0.0;
				
				for (int d=_start; d<_end; d++) {
					_sum += Prob[_f][d];
				}
				
				return _sum;
			}
		}

		if (features == null) {
			//we have to convert all the counts in to probability
			for (int f=0; f<Prob.length; f++) {
				double sum = 0.0;
				int len  = Prob[f].length;
				int jump = len/numP;
				
				if (jump > 1) {
					ExecutorService executor = Executors.newFixedThreadPool(numP);
					ArrayList<Future<Double>> future = new ArrayList<>();
					System.err.print("calculating sum");
					for (int i=0; i< numP; i++) {
						int s = i*jump;
						int e = (i+1)*jump;
						
						if (i == (numP-1))
							e = Prob[f].length;
					
						Normalizer worker 		= new Normalizer(s, e, f);				
						Future<Double> result 	= executor.submit(worker);

						future.add(result);
					}
					
					for (Future<Double> result : future) {
						sum += result.get();
					}

					System.err.print(", Normalizing");
					for (int i=0; i< numP; i++) {
						int s = i*jump;
						int e = (i+1)*jump;
					
						if (i == (numP-1))
							e = Prob[f].length;
						
						Runnable worker = new Worker(s,e,f,sum);
						executor.submit(worker);						
					}
					executor.shutdown();

					while (!executor.isTerminated()) {   }
					System.err.println(" : done");
				} else {
					for (int d=0; d<Prob[f].length; d++) {
						sum += Prob[f][d];
					}
					for (int d=0; d<Prob[f].length; d++) {
						Prob[f][d] /= sum;
					}
				}
			}
		} else {
			Prob[allFeatures.medianRank.ordinal()][features.MedianRank()]++;
			Prob[allFeatures.numPapers.ordinal()][features.NumPapers()]++;
			for (Integer venue : features.Venues())
				Prob[allFeatures.venues.ordinal()][venue]++;
			Prob[allFeatures.yearsActive.ordinal()][features.YearsActive()]++;
			for (int subject : features.Subjects())
				Prob[allFeatures.subjects.ordinal()][subject]++;
		}
	}	

	/**
	 * We are running the experiment Graphsize_vs_Runtime
	 * @param mStart
	 * @param mEnd
	 * @param numQuery
	 * @param maxEdges 
	 * @param kMin
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	
	public static void Run_Graphsize_vs_Runtime_withUniformWeight (int mStart, int mEnd, int numQuery, int K, int maxEdges)
			throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		boolean printResults = true;
		boolean avoidTAHeuristics = !Helper.useHeuristics;
		boolean considerUniformWeight = true;

		Run_Graphsize_vs_Runtime(mStart, mEnd, numQuery,
				K, maxEdges, printResults, avoidTAHeuristics, considerUniformWeight);
	}
	public static void Run_Graphsize_vs_Runtime(int mStart, int mEnd, int numQuery, int K, int maxEdges)
			throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		boolean printResults = true;
		Run_Graphsize_vs_Runtime(mStart, mEnd, numQuery, K, maxEdges, printResults);
	}

	public static void Run_Graphsize_vs_Runtime(int mStart, int mEnd, int numQuery,
			int K, int maxEdges, boolean printResults)
					throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		boolean avoidTAHeuristics = !Helper.useHeuristics;
		boolean considerUniformWeight = false;

		Run_Graphsize_vs_Runtime(mStart, mEnd, numQuery,
				K, maxEdges, printResults, avoidTAHeuristics, considerUniformWeight);
	}

	public static void Run_Graphsize_vs_Runtime(int mStart, int mEnd, int numQuery,
			int K, int maxEdges, boolean printResults, boolean avoidTAHeuristics,
			boolean considerUniformWeight)
					throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {

		//initialize the graph and index structure
		Init();

		String fname=Helper.GetFname(Experiments.AllExperiments.Graphsize_vs_Runtime, 1.0, dataSet);
		ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);
		//create Query query Graphs
		ArrayList<Graph<DBLPAlternateFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<DBLPAlternateFeatures> queryGraph : queryGraphArray) {
				//we do not want to run an experiment on more than max edges 
				if(queryGraph.SizeEdges() > maxEdges)
					break;

				//we now have a query graph having m nodes
				if (considerUniformWeight)
					queryGraph.UpdateWeightsUniformly();
				else 
					queryGraph.UpdateWeights (probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>>> topK;

				if (printResults)
					queryGraph.Print();
				showProgress = printResults;
				System.out.print("Processing query graph "+i+" of size "+queryGraph.SizeEdges() +"    :     ");
				CGQTimeOut.startTimeOut(); 
				TicToc.Tic(printResults);
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
						DBLPCGQhierarchicalIndex, showProgress, null, null, null,
						true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();
				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme, K, elapsedTime, topK.size(), -1);
			}

			resultLogger.Flush();
		}
	}

	private static void Init(int branchingFactor) 
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean doNotLoadQueryGraphs = false;

		Init(1.0, false, branchingFactor, doNotLoadQueryGraphs);
	}

	private static void Init()
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		Init(1.0, false);
	}

	private static void Init(double frac, boolean runTargetSizeExperiment)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean doNotLoadQueryGraphs = false;

		Init (frac, runTargetSizeExperiment, RAQ.BranchingFactor, doNotLoadQueryGraphs);
	}

	private static void Init(double frac, boolean runTargetSizeExperiment,
			boolean doNotLoadQueryGraphs)
					throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		Init (frac, runTargetSizeExperiment, RAQ.BranchingFactor, doNotLoadQueryGraphs);
	}

	/**
	 * initialize the graph and the index structure 
	 * @param frac  
	 * @param runTargetSizeExperiment
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void Init(double frac, boolean runTargetSizeExperiment,
			int branchingFactor, boolean doNotLoadQueryGraphs)
					throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean useHeuristics = Helper.useHeuristics;

		if (DBLPAlternateGraph == null) {
			System.out.println("Loading the DBLP Data set");
			CreateGraph 	(DBLPHelper.fName, frac);

			CreateCGQHierarcicalIndex(branchingFactor, useHeuristics);
			//DBLPAlternateGraph.Analyse();

			//load the query graphs
			if (doNotLoadQueryGraphs) {
				queryGraphList = null;
			} else {
				if (runTargetSizeExperiment) {
					queryGraphList = DBLPAlternateGraph.LoadQueryGraphEdgeList(queryFNameBase);
				} else {
					queryGraphList = DBLPAlternateGraph.LoadQueryGraphEdgeList(queryFName);
				}
			}
		}
	}

	/**
	 * De-initialize the graph and index structure
	 */
	private static void DeInit () {
		DBLPAlternateGraph = null;
	}

	public static void PrintResults (Experiments.AllExperiments experiment) {
		boolean minimal = false;

		PrintResults (experiment, minimal);
	}

	public static void PrintResults (Experiments.AllExperiments experiment, boolean minimal) {
		String fname=Helper.GetFname(experiment, 1.0, dataSet);
		switch (experiment) {
		case Exit:
			break;
		case Graphsize_vs_Runtime:
		case Graphsize_vs_Runtime_uniform: {
			ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);			
			resultLogger.PrintAllResults (-1, minimal);
		}
		break;
		case K_vs_Runtime: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.K_vs_Runtime);
			resultLogger.PrintAllResults ();
		}
		break;
		case K_vs_Runtime_uniform: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.K_vs_Runtime_Uniform);
			resultLogger.PrintAllResults ();
		}
		break;
		case BaseCase: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BaseCase);
			resultLogger.PrintAllResults ();
		}
		break;
		case BeamWidth: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.BeamWidth);
			resultLogger.PrintAllResults ();
		}
		break;
		case WidthThreshold: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.WidthThreshold);
			resultLogger.PrintAllResults ();
		}
		break;
		case TargetGraphSize: {
			double frac = Experiments.GetFraction();
			fname=Helper.GetFname(experiment, frac, dataSet);
			ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);			
			resultLogger.PrintAllResults (-1, minimal);
		}
		break;
		case BeamWOIndex: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BFSWOIndex);
			resultLogger.PrintAllResults ();
		}
		case BranchingFactor: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.BranchingFactor);
			resultLogger.PrintAllResults ();
		}
		break;
		case Qualitative:
			break;
		case QualitativeQuantitative:
			break;
		case Heuristicts: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BFSWOHeuristics);
			resultLogger.PrintAllResults ();
		}
		break;
		}
	}

	/**
	 * run the K_vs_Runtime experiment, k varies from kMin to kMax in step size kStep
	 * And query graph is of a random size between mStart and mEnd
	 * @param parameter 
	 * @param mStart
	 * @param mEnd
	 * @param numQuery
	 * @param paraMin
	 * @param paraMax
	 * @param paraStep
	 * @param maxEdges 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void Run_Para_vs_Runtime(Experiments.AllExperimentsParameters parameter,
			int mStart, int mEnd, int numQuery,
			ArrayList<Integer> paraL, int maxEdges)
					throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		//initialize the graph and index structure
		Init();

		String  fname=null;
		//useUniformWeight : do we want the query graph to have uniform weight
		boolean useUniformWeight=false;
		
		switch (parameter) {
		case BeamWidth:
			fname=Helper.GetFname(Experiments.AllExperiments.BeamWidth, 1.0, dataSet);
			break;
		case K_vs_Runtime:
			fname=Helper.GetFname(Experiments.AllExperiments.K_vs_Runtime, 1.0, dataSet);
			break;
		case WidthThreshold:
			fname=Helper.GetFname(Experiments.AllExperiments.WidthThreshold, 1.0, dataSet);
			break;		
		case BranchingFactor:
			System.err.println("Run_Para_vs_Runtime : BranchingFactor Not implemented");
			return;
		case K_vs_Runtime_Uniform:
			useUniformWeight = true;
			fname=Helper.GetFname(Experiments.AllExperiments.K_vs_Runtime_uniform, 1.0, dataSet);
			break;
		}

		ResultLogger.Para_vs_Runtime resultLogger =
				new ResultLogger.Para_vs_Runtime (fname, parameter);

		int m = (mStart+mEnd)/2;	
		boolean avoidTAHeuristics = !Helper.useHeuristics;

		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<DBLPAlternateFeatures>> queryGraphArray;
			queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayFromEdgeList
					(m, m, queryGraphList.get(i));

			//we are not changing the size of the query
			Graph<DBLPAlternateFeatures> queryGraph =
					queryGraphArray.get(0);


			if (useUniformWeight)
				queryGraph.UpdateWeightsUniformly();
			else
				queryGraph.UpdateWeights (probability);
			boolean showProgress = false;
			queryGraph.updateNeighbourhoodSummary(showProgress);
			
			System.out.println("Query Graph");
			queryGraph.Print();

			for (int para : paraL){
				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>>> topK=null;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				int K = Experiments.K;

				CGQTimeOut.startTimeOut(); 
				TicToc.Tic();
				switch (parameter) {
				case BeamWidth:
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
							DBLPCGQhierarchicalIndex, true, null, null, null,
							true, avoidTAHeuristics, para, RAQ.WidthThreshold, false);
					break;
				case K_vs_Runtime:
				case K_vs_Runtime_Uniform:
					K = para;
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
							DBLPCGQhierarchicalIndex, true, null, null, null,
							true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
					break;
				case WidthThreshold:
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
							DBLPCGQhierarchicalIndex, true, null, null, null,
							true, avoidTAHeuristics, RAQ.BeamWidth, para, false);
					break;
				case BranchingFactor:
					System.err.println("Run_Para_vs_Runtime : BranchingFactor Not implemented");
					return;
				}

				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();
				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme, K, elapsedTime, topK.size(), para);
			}
			resultLogger.Flush();
		}	
	}

	/**
	 * Run a Base experiment
	 * @param maxEdges 
	 * @param numQuery 
	 * @param mEnd 
	 * @param mStart 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void Run_BaseCase(int mStart, int mEnd, int numQuery, int maxEdges)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		String fname=Helper.GetFname(Experiments.AllExperiments.BaseCase, 1.0, dataSet);
		RunExperimentHelper (mStart, mEnd, numQuery, maxEdges,
				RAQ.IndexingScheme.BaseCase, fname);
	}

	public static void RunExperimentHelper(int mStart, int mEnd, int numQuery, int maxEdges,
			RAQ.IndexingScheme indexingSchemeComparison, String fname) throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		//initialize the graph and index structure
		Init();

		ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname, indexingSchemeComparison);

		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<DBLPAlternateFeatures>> queryGraphArray;
			queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<DBLPAlternateFeatures> queryGraph : queryGraphArray) {
				queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				System.out.println("Query Graph");
				queryGraph.Print();

				int K = Experiments.K;

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>>> topK;

				//using our technique
				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				if (indexingSchemeComparison == RAQ.IndexingScheme.BaseCase)
					CGQTimeOut.startTimeOutLong();
				else
					CGQTimeOut.startTimeOut();
				
				boolean avoidTAHeuristics = !Helper.useHeuristics;
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
						DBLPCGQhierarchicalIndex, true, null, null,
						null, true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();


				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme, K, elapsedTime, topK.size(), -1);

				//run other experiment
				System.out.println();
				if (indexingSchemeComparison == RAQ.IndexingScheme.BaseCase) {
					//run base case
					System.out.print("Base Case for query graph "+i+" :     ");
					ModifiedTA<DBLPAlternateFeatures> baseCase =
							new ModifiedTA<>();

					CGQTimeOut.startTimeOutLong();
					TicToc.Tic();	
					topK = baseCase.GetTopKSubgraphs (K, queryGraph, DBLPAlternateGraph, false, null,
							null, null, false, true, true);
					elapsedTime=TicToc.Toc();
					CGQTimeOut.stopTimeOut();
				} else if (indexingSchemeComparison == RAQ.IndexingScheme.BFSWOIndex) {
					System.out.print("Beam without any "+i+" :     ");

					CGQTimeOut.startTimeOut(); 
					TicToc.Tic();
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
							DBLPCGQhierarchicalIndex, true, null, null,
							null, true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, true);
					elapsedTime=TicToc.Toc();
					CGQTimeOut.stopTimeOut();
				} else {
					System.out.println("Fatal error wrong indexing scheme");
					System.exit(-1);
				}
				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				resultLogger.Log (numNodes, numEdges, indexingSchemeComparison, K, elapsedTime, topK.size(), -1);
			}
			resultLogger.Flush();			
		}
	}

	/**
	 * Generate the random inputs
	 * @param setSize
	 * @param listSize
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void GenerateRandomGraphs(int setSize, int listSize, double frac)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean doNotLoadQueryGraphs = true;

		Init (frac, false, doNotLoadQueryGraphs);

		System.out.println("Edges will be printed as list of edges represented as list of node pairs");
		DBLPAlternateGraph.GetSetOfRandomListOfConnectedEdges(setSize, listSize);

		//De-initialize
		DBLPAlternateGraph = null;
	}

	/**
	 * run the experiment TargetGraphSize
	 * @param mstart
	 * @param mend
	 * @param numquery
	 * @param k
	 * @param maxedges
	 * @param frac
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void Run_TargetGraphSize(int mStart, int mEnd,
			int numQuery, int K, int maxEdges, double frac)
					throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		Init(frac, true);

		String fname=Helper.GetFname(Experiments.AllExperiments.TargetGraphSize, frac, dataSet);
		ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);
		ArrayList<Graph<DBLPAlternateFeatures>> queryGraphArray;	
		boolean avoidTAHeuristics = !Helper.useHeuristics;
		for (int i=0; i<numQuery; i++) {
			//create Query query Graphs
			queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<DBLPAlternateFeatures> queryGraph : queryGraphArray) {
				//we do not want to run an experiment on more than max edges 
				if(queryGraph.SizeEdges() > maxEdges)
					break;

				//we now have a query graph having m nodes
				queryGraph.UpdateWeights (probability);
				queryGraph.Print();
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>>> topK;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				CGQTimeOut.startTimeOut(); 
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
						DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme, K, elapsedTime, topK.size(), -1);
			}

			resultLogger.Flush();
		}

		if (frac != 1.0) {
			System.out.println ("Deinitializing");
			DeInit();
		}
	}

	public static void RunBeamWOIndex(int mStart, int mEnd, int numQuery, int maxEdges)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		String fname=Helper.GetFname(Experiments.AllExperiments.BeamWOIndex, 1.0, dataSet);
		RunExperimentHelper (mStart, mEnd, numQuery, maxEdges, RAQ.IndexingScheme.BFSWOIndex, fname);
	}

	public static void RunBranchingFactor(int m, int branchingFactor, int numQuery)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		DeInit();
		Init (branchingFactor);

		String fname=Helper.GetFname(Experiments.AllExperiments.BranchingFactor, 1.0, dataSet);

		ResultLogger.Para_vs_Runtime resultLogger = 
				new ResultLogger.Para_vs_Runtime (fname, Experiments.AllExperimentsParameters.BranchingFactor);
		
		boolean avoidTAHeuristics = !Helper.useHeuristics;
		ArrayList<Graph<DBLPAlternateFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			//create Query query Graphs
			queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayFromEdgeList
					(m, m, queryGraphList.get(i));

			for (Graph<DBLPAlternateFeatures> queryGraph : queryGraphArray) {
				//we now have a query graph having m nodes
				queryGraph.UpdateWeights (probability);
				queryGraph.Print();
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>>> topK;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				CGQTimeOut.startTimeOut(); 
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(Experiments.K, queryGraph, DBLPAlternateGraph,
						DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme, 
						Experiments.K, elapsedTime, topK.size(), branchingFactor);
			}

			resultLogger.Flush();
		}
		DeInit();
	}

	public static void RunQualitative(int mStart, int mEnd, int K)
			throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		//initialize the graph and index structure
		Init();

		//create Query query Graphs
		ArrayList<Graph<DBLPAlternateFeatures>> queryGraphArray;
		for (int i=0; i<queryGraphList.size(); i++) {

			queryGraphArray = DBLPAlternateGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<DBLPAlternateFeatures> queryGraph : queryGraphArray) {
				RunQualitativeHelper(K, 
						"../results/Qualitative/DBLPAlternate/"+(i+1)+"_"+queryGraph.SizeEdges()+".csv",
						queryGraph);

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
					return;
				}
			}
		}
	}

	public static void RunQualitativeInteractive (int K, Scanner reader)
			throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		//initialize the graph and index structure
		Init();

		while(true) {
			int input;
			System.out.print ("Do you want to  create a new query graph ? (enter 0 to exit) : ");
			try {
				input = reader.nextInt();
			} catch (Exception e) {
				reader.next();
				input = 1;
			}

			if (input == 0)
				break;

			Graph<DBLPAlternateFeatures> queryGraph = CreateQueryGraph(reader);

			if (queryGraph.SizeNodes() == 0) {
				System.out.println("Empty Query Graph");
				continue;	
			}

			System.out.print ("K : ");
			K = reader.nextInt();
			reader.nextLine();

			if (K <= 0) {
				System.out.println("Invalid K : ");
				continue;
			}

			System.out.print ("Fname to store the results : ");
			String fname = reader.next();

			RunQualitativeHelper (K, "../results/Qualitative/"+fname+".csv",
					queryGraph);
			if (InterruptSearchSignalHandler.Interrupt()) {
				//we were interrupted from keyboard
				InterruptSearchSignalHandler.ResetFlag(DBLPAlternateGraph);
				return;
			}
		}
	}

	private static void RunQualitativeHelper(int K, String fname,
			Graph<DBLPAlternateFeatures> queryGraph)
					throws IOException {
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
		//System.out.println("PRESS ENTER to see the query : ");
		//System.in.read();

		File myFoo = new File(fname);
		FileWriter fooWriter = new FileWriter(myFoo, false); 

		queryGraph.UpdateWeights (probability);
		boolean showProgress = false;
		queryGraph.updateNeighbourhoodSummary(showProgress);
		QualitativeHelperHelper(K, queryGraph, fooWriter);
		fooWriter.write   (",\n");
		fooWriter.write   (",\n");
		fooWriter.write   ("%%%%%%%%%,\n");
		fooWriter.write   (",\n");
		fooWriter.write   ("Isomorphism ,\n");
		fooWriter.write   (",\n");

		queryGraph.UpdateWeightsUniformly ();
		QualitativeHelperHelper(K, queryGraph, fooWriter);

		fooWriter.close();
	}

	private static void QualitativeHelperHelper(int K, Graph<DBLPAlternateFeatures> queryGraph,
			FileWriter fooWriter) throws IOException {
		//we now have a query graph having m nodes
		queryGraph.Print();
		System.out.println("Query Graph ,");
		queryGraph.PrintCSVToFile(fooWriter);

		//find the topK
		ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>>> topK;

		System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		//System.out.print("CGQ Hierarchical BFS Index for query graph    :     ");
		CGQTimeOut.startTimeOut(); 
		TicToc.Tic();
		boolean avoidTAHeuristics = !Helper.useHeuristics;

		topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPAlternateGraph,
				DBLPCGQhierarchicalIndex, true, null, null, null,
				true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
		TicToc.Toc();
		CGQTimeOut.stopTimeOut();

		//int l=1;
		int r=1;
		for (Helper.ObjectDoublePair<ArrayList<Graph<DBLPAlternateFeatures>.Node>> od : topK) {
			ArrayList<Graph<DBLPAlternateFeatures>.Node> arr = od.element;
			Double dist = od.value;

			System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			//System.out.println("PRESS ENTER to see the results "+ (l++) +": ");
			//System.in.read();

			/*
			System.out.println("Mapping");
			for (int j =0 ;j<arr.size(); j++) {
				Graph<DBLPAlternateFeatures>.Node qNode = queryGraph.nodeList.get(j);
				Graph<DBLPAlternateFeatures>.Node tNode = arr.get(j);

				System.out.print(qNode.nodeID+":"+tNode.nodeID+",");
			}
			System.out.println();
			for (Graph<DBLPAlternateFeatures>.Node tNode : arr)
				tNode.features.Print();
			 */
			//System.out.println(" ***** CSV *****");
			//System.out.println(",\nResult : "+ r   +", Distance : "+dist+",");
			fooWriter.write   (",\nResult : "+ r++ +", Distance : "+dist+",\n");

			//print edges
			DecimalFormat dFormat = new DecimalFormat("0.00");
			for (Graph<DBLPAlternateFeatures>.Edge qEdge : queryGraph.edgeSet) {
				Graph<DBLPAlternateFeatures>.Node qNode1 = qEdge.node1;
				Graph<DBLPAlternateFeatures>.Node qNode2 = qEdge.node2;

				int qNode1Index = queryGraph.nodeList.indexOf(qNode1);
				int qNode2Index = queryGraph.nodeList.indexOf(qNode2);

				Graph<DBLPAlternateFeatures>.Node tNode1 = arr.get(qNode1Index);
				Graph<DBLPAlternateFeatures>.Node tNode2 = arr.get(qNode2Index);

				Graph<DBLPAlternateFeatures>.Edge tEdge = tNode1.GetEdgeTo(tNode2);

				fooWriter.write (tEdge.node1.features.AuthorName() +" --- "+
						tEdge.node2.features.AuthorName()+",");
				//print ve
				for (int i=0; i<tEdge.NumFeatures(); i++)
					fooWriter.write (dFormat.format(tEdge.GetVe(i))+RAQ.Seperator);
				fooWriter.write(",");
				//print weight
				for (int i=0; i<tEdge.NumFeatures(); i++)
					fooWriter.write (dFormat.format(queryGraph.weights[i])+RAQ.Seperator);
				fooWriter.write(",");

				fooWriter.write("\n");
			}

			int n=0;
			//System.out.print("Node Mapped to,");
			fooWriter.write ("Node Mapped to,");

			for (Graph<DBLPAlternateFeatures>.Node tNode : arr){
				//tNode.features.PrintCSVHeader();
				tNode.features.PrintCSVHeaderToFile(fooWriter);
				break;
			}
			for (Graph<DBLPAlternateFeatures>.Node tNode : arr) {
				//System.out.print(n +   ",");
				fooWriter.write (n++ + ",");
				//tNode.features.PrintCSV();
				tNode.features.PrintCSVToFile(fooWriter);
			}
			fooWriter.write   (",\n");
		}
	}

	private static Graph<DBLPAlternateFeatures> CreateQueryGraph (Scanner reader ) {
		class QualitativeComparator 
		implements  Comparator<Graph<DBLPAlternateFeatures>.Edge> {

			@Override
			public int compare(Graph<DBLPAlternateFeatures>.Edge e1,
					Graph<DBLPAlternateFeatures>.Edge e2) {
				String s1 = GetEdgeName (e1);
				String s2 = GetEdgeName (e2);

				return s1.compareTo(s2);
			}
		}

		Graph<DBLPAlternateFeatures> graph = new Graph<>(5, false);
		HashSet<Graph<DBLPAlternateFeatures>.Node> nodesSelected = new HashSet<>();
		HashSet<Graph<DBLPAlternateFeatures>.Edge> edgesSelected = new HashSet<>();

		System.out.println("Creating graph : ");

		if (RAQ.test){
			int i =0;
			for (Graph<DBLPAlternateFeatures>.Node node : DBLPAlternateGraph.nodeList) {
				node.features.PrintShort();
				System.out.println();

				i++;
				if (i>10)
					break;
			}
		}
		//get the first node to start with
		reader.nextLine();
		while (true) {
			System.out.print("Author name : (enter \"exit\" to exit)");
			String author = reader.nextLine();

			if (author.equals("exit"))
				return graph;

			//find the node corresponding to the author
			Integer authorID = DBLPEntry.GetAuthorID(author);

			if (authorID == null) {
				System.out.println("Invalid author name ");
				continue;
			}

			//find the node
			Graph<DBLPAlternateFeatures>.Node node = DBLPAlternateGraph.GetNode (authorID);
			nodesSelected.add(node);

			break;
		}

		while (true) {
			int input;
			System.out.print ("exit ? (enter 0 to exit) : ");
			try {
				input = reader.nextInt();
			} catch (Exception e) {
				reader.next();
				input = 1;
			}

			if (input == 0)
				break;

			//select an edge
			//create an HashSet of possible edges
			HashSet<Graph<DBLPAlternateFeatures>.Edge> edges = new HashSet<>();

			for (Graph<DBLPAlternateFeatures>.Edge edge : edgesSelected) {
				nodesSelected.add(edge.node1);
				nodesSelected.add(edge.node2);
			}

			for (Graph<DBLPAlternateFeatures>.Node node : nodesSelected) {
				edges.addAll(node.AllEdges());
			}

			edges.removeAll(edgesSelected);

			if (edges.isEmpty())
				continue;

			ArrayList<Graph<DBLPAlternateFeatures>.Edge> edgesArr = new ArrayList<>(edges);

			Collections.sort(edgesArr, new QualitativeComparator());
			int i=0;
			for (Graph<DBLPAlternateFeatures>.Edge edge : edgesArr) {
				System.out.print (i++ + " : ");
				System.out.println(GetEdgeName(edge));
			}

			System.out.print ("Edge : (-1 for deatiled print, -2 continue without selecting) ");
			try {
				input = reader.nextInt();
			} catch (Exception e) {
				reader.next();
				continue;
			}

			if (input == -1) {
				for (Graph<DBLPAlternateFeatures>.Edge edge : edgesArr) {
					edge.node1.features.PrintCSVHeader();
					break;
				}
				i=0;
				for (Graph<DBLPAlternateFeatures>.Edge edge : edgesArr) {
					System.out.print (i++ + " : ");

					PrintEdge(edge);
				}

				System.out.print ("Edge :  ");
				try {
					input = reader.nextInt();
				} catch (Exception e) {
					reader.next();
					continue;
				}
			} else if (input == -2) {
				//do nothing so that we restart
			} else {
				edgesSelected.add(edgesArr.get(input));
			}

			System.out.println("Edges selected : ");
			for (Graph<DBLPAlternateFeatures>.Edge edge : edgesSelected) {
				PrintEdge(edge);
			}
		}

		ArrayList<Graph<DBLPAlternateFeatures>.Edge> edgesArr = new ArrayList<>(edgesSelected);
		graph = DBLPAlternateGraph.ConvertEdgesToGraph(edgesArr);
		return graph;
	}

	private static void PrintEdge(Graph<DBLPAlternateFeatures>.Edge edge) {
		Graph<DBLPAlternateFeatures>.Node node1, node2;
		String a1 = edge.node1.features.AuthorName();
		String a2 = edge.node2.features.AuthorName();

		if (a1.compareTo(a2) < 0) {
			node1 = edge.node1;
			node2 = edge.node2;
		} else {
			node1 = edge.node2;
			node2 = edge.node1;
		}

		node1.features.PrintShort();
		System.out.print (" --- ");
		node2.features.PrintShort();
		System.out.println();
	}
	/**
	 * Return a name alphabetically sorted
	 * @param e
	 * @return
	 */
	private static String GetEdgeName (Graph<DBLPAlternateFeatures>.Edge e) {
		String s1 = e.node1.features.AuthorNameFormated();
		String s2 = e.node2.features.AuthorNameFormated();

		String ret;
		if (s1.compareTo(s2) < 0)
			ret = s1+" --- "+s2;
		else
			ret = s2+" --- "+s1;

		return ret;
	}

	public static void PrintNumOfEdges() throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		Double fArray[]={0.25, 0.5, 0.75, 1.0};

		for (Double f : fArray) {
			Init(f, true);
			System.out.println (f+": num of edges "+DBLPAlternateGraph.SizeEdges());

			if (f != 1.0) {
				System.out.println ("Deinitializing");
				DeInit();
			}
		}
	}

	/**
	 * Store the network into two files,
	 * One for nodes,
	 * other for edges
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void DumpNetworkToFile() throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		Init();
		DBLPAlternateGraph.PrintNodesToFile (nodeFname);
		DBLPAlternateGraph.PrintEdgesToFile (edgeFname);
	}
}