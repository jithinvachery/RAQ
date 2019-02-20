import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class DBLPFeatures extends NodeFeatures {
	//categorical
	private final int venue;
	//non-categorical
	private final HashSet<Integer> authors;
	private final int year;
	private final int rank;
	private final HashSet<Integer> subjects;

	private final int yearProb; //used to index prob
	//this feature is not a feature but an info
	private final int paperId;

	static final int numCategoricalFeatures    = 1;
	static final int numNonCategoricalFeatures = DBLP.numFeatures - numCategoricalFeatures;

	public final static int numFeatures = DBLP.numFeatures;

	//the farthest we have referred a paper
	private static double farthestReference = 0;
	public static boolean farthestReferenceSet = false;
	
	public DBLPFeatures (DBLPEntry entry) {
		venue	= entry.Venue();
		rank	= entry.Rank();
		authors = entry.Authors();
		year	= entry.Year();
		paperId	= entry.PaperID();
		yearProb= entry.YearProb();
		subjects= entry.Subjects();
	}
	
	@Override
	String[] AllFeatures() {
		return DBLP.allFeaturesStrings;
	}

	@Override
	int NumFeatures() {
		return numFeatures;
	}

	@Override
	double[] GetVE(NodeFeatures features) {
		double[] ve = new double[numFeatures];
		DBLPFeatures nf = (DBLPFeatures)features;
		     
		//feature venue is categorical
		if (venue == nf.venue)
			ve[DBLP.allFeatures.venue.ordinal()]=1.0;
		else
			ve[DBLP.allFeatures.venue.ordinal()]=0;

		//processing non-categorical features

		double min, max;
		HashSet<Integer> intersection;
		HashSet<Integer> union;
		
		/*
		//authors
		// we use jaccard distance, intersection by union
		//we assuming author disambiguation has been done
		HashSet<Integer> intersection = new HashSet<>(authors);
		intersection.retainAll(nf.authors);
		HashSet<Integer> union = new HashSet<>(authors);
		union.addAll(nf.authors);
		

		min = intersection.size();
		max = union.size();
		
		//freeing the memory
		intersection = null;
		union = null;
		
		if (max == 0) 
			ve[DBLP.allFeatures.authors.ordinal()] = 1;
		else
			ve[DBLP.allFeatures.authors.ordinal()] = min/max;
		*/
		//max num of authors
		int a = nf.numAuthors ();
		int b = numAuthors();
		ve[DBLP.allFeatures.numAuthors.ordinal()] = MinByMax(a, b);
		
		//
		//Year
		min = Math.abs(year-nf.year);
		if (farthestReferenceSet)
			max = farthestReference;
		else {
			//we want to raise an exception
			max = 0;
			System.err.println("farthestReferenceSet is not set");
			System.exit(-1);
		}
		
		if (max == 0)
			ve[DBLP.allFeatures.year.ordinal()] = 1;
		else
			ve[DBLP.allFeatures.year.ordinal()] = 1-min/max;
		
		//rank
		GetVeHelper(ve, rank, nf.rank, DBLP.allFeatures.rank.ordinal());
		
		//subject
		intersection = new HashSet<>(subjects);
		intersection.retainAll(nf.subjects);
		union = new HashSet<>(subjects);
		union.addAll(nf.subjects);
		
		GetVeHelper(ve, intersection.size(), union.size(), DBLP.allFeatures.subjects.ordinal());

		//freeing the memory
		intersection = null;
		union = null;
		
		return ve;
	}

	int numAuthors() {
		return authors.size();
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
	public void Print() {
		System.out.print("Paper ID : "+paperId+" ");
		System.out.print("Authors : "+authors+" ");
		System.out.print("Year : "+year+" ");
		System.out.print("Venue : "+venue+" ");
		System.out.print("Rank : "+rank+" ");
		System.out.print("Subjects : "+subjects+" ");

		System.out.println("\n*** actual values ***");
		System.out.print("Paper ID : "+DBLPEntry.GetPaperIDName(paperId)+" ");
		System.out.print("Authors : ");
		for (Integer a : authors)
			System.out.print(DBLPEntry.GetAuthorName(a)+",");
		System.out.print("Year : "+DBLPEntry.GetYear(year)+" ");
		System.out.print("Venue : "+DBLPEntry.GetVenueName(venue)+" ");
		System.out.print("Rank : "+rank+" ");
		System.out.print("Subjects : ");
		for (Integer s : subjects)
			System.out.print(ConferenceInfo.GetSubjectReverse(s)+",");
		System.out.println();
	}
	
	@Override
	public void PrintCSV() {
		System.out.print(DBLPEntry.GetPaperIDName(paperId)+",");
		if (authors.size() == 0)
			System.out.print("null,");
		else {
			for (Integer a : authors)
				System.out.print(DBLPEntry.GetAuthorName(a)+RAQ.Seperator);
		}
		System.out.print(",");
		System.out.print(DBLPEntry.GetYear(year)+",");
		System.out.print(DBLPEntry.GetVenueName(venue)+",");
		System.out.print(rank+",");
		if (subjects.size() == 0)
			System.out.print("null,");
		else {
			for (Integer s : subjects)
				System.out.print(ConferenceInfo.GetSubjectReverse(s)+RAQ.Seperator);
		}
		System.out.print(",");
		System.out.println();
	}

	@Override
	public void PrintCSVToFile(FileWriter fooWriter) throws IOException {
		fooWriter.write(DBLPEntry.GetPaperIDName(paperId)+",");
		if (authors.size() == 0)
			fooWriter.write("null,");
		else {
			for (Integer a : authors)
				fooWriter.write(DBLPEntry.GetAuthorName(a)+RAQ.Seperator);
		}
		fooWriter.write(",");
		fooWriter.write(DBLPEntry.GetYear(year)+",");
		fooWriter.write(DBLPEntry.GetVenueName(venue)+",");
		fooWriter.write(rank+",");
		if (subjects.size() == 0)
			fooWriter.write("null,");
		else {
			for (Integer s : subjects)
				fooWriter.write(ConferenceInfo.GetSubjectReverse(s)+RAQ.Seperator);
		}
		fooWriter.write(",");
		fooWriter.write(",\n,\n");
	}

	@Override
	public void PrintCSVHeader() {
		System.out.print("Paper ID (not a feature)"+",");
		System.out.print("Authors"+",");
		System.out.print("Year"+",");
		System.out.print("Venue"+",");
		System.out.print("Rank"+",");
		System.out.print("Subjects"+",");
		System.out.println();
	}

	@Override
	public void PrintCSVHeaderToFile(FileWriter fooWriter) throws IOException {
		fooWriter.write("Paper ID (not a feature)"+",");
		fooWriter.write("Authors"+",");
		fooWriter.write("Year"+",");
		fooWriter.write("Venue"+",");
		fooWriter.write("Rank"+",");
		fooWriter.write("Subjects"+",");
		fooWriter.write("\n");
	}

	/**
	 * We need to find the 
	 * @param dist
	 * @param farthestReferenceSet
	 */
	static void updateFarthestReference (int dist, boolean farthestReferenceSet) {
		if (farthestReference < dist)
			farthestReference = dist;
		
		if (farthestReferenceSet)
			DBLPFeatures.farthestReferenceSet = true;
	}

	public int Venue() {
		return venue;
	}

	public int PaperID() {
		return paperId;
	}

	public int Year() {
		return year;
	}

	public int YearProb() {
		return yearProb;
	}

	public HashSet<Integer> Authors() {
		return authors;
	}

	public HashSet<Integer> Subjects() {
		return subjects;
	}

	public int Rank() {
		return rank;
	}

	static void PrintStat() {
		System.out.println("num of Venues   : "+DBLPEntry.NumVenues());
		System.out.println("num of Authors  : "+DBLPEntry.NumAuthors());
		System.out.println("num of Years    : "+DBLPEntry.NumYears());
		System.out.println("num of Papers   : "+DBLPEntry.NumPapers());
		System.out.println("num of Subjects : "+DBLPEntry.NumSubjects());
		
		//free memory
		DBLPEntry.Destroy();
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
	ArrayList<ArrayList<String>> AllFeatureValues(NodeFeatures features2) {
		DBLPFeatures neighbourFeature = (DBLPFeatures)features2;
		
		ArrayList<ArrayList<String>> ret = new ArrayList<>(numFeatures);

		for (int i=0; i<numFeatures; i++) {
			ArrayList<String> s = new ArrayList<>(); 
			ret.add(s);
		}
		
		do {
			boolean diGraph = true;
			//all this mombo jumbo  of using switch case, to ensure we do not miss out a feature
			DBLP.allFeatures allF = DBLP.allFeatures.numAuthors;
			ArrayList<String> S;
			
			switch (allF) {
			case numAuthors:
				S = ret.get(DBLP.allFeatures.numAuthors.ordinal());
				S.add(getString(numAuthors(), neighbourFeature.numAuthors(), diGraph));
				
			/*
			case authors:
				S = ret.get(DBLP.allFeatures.authors.ordinal());
				for (int auth1 : authors) {
					for (int auth2 : neighbourFeature.authors) {
						S.add(getString(auth1, auth2, diGraph));
					}
				}
				*/
			case rank:
				S = ret.get(DBLP.allFeatures.rank.ordinal());
				S.add(getString(rank, neighbourFeature.rank, diGraph));
			case subjects:
				S = ret.get(DBLP.allFeatures.subjects.ordinal());
				for (int sub1 : subjects) {
					for (int sub2 : neighbourFeature.subjects) {
						S.add(getString(sub1, sub2, diGraph));
					}
				}
			case venue:
				S = ret.get(DBLP.allFeatures.venue.ordinal());
				S.add(getString(venue, neighbourFeature.venue, diGraph));
			case year:
				S = ret.get(DBLP.allFeatures.year.ordinal());
				S.add(getString(year, neighbourFeature.year, diGraph));
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
		DBLP.allFeatures allF = DBLP.allFeatures.numAuthors;
		switch (allF) {
		case numAuthors:
			i = DBLP.allFeatures.numAuthors.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++; 
		case rank:
			i = DBLP.allFeatures.rank.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case subjects:
			i = DBLP.allFeatures.subjects.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case venue:
			i = DBLP.allFeatures.venue.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case year:
			i = DBLP.allFeatures.year.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		}
		
		return neighbourhoodSummary;
	}

	@Override
	public boolean Filter(Graph.Edge qEdge, Graph.Edge tEdge) {
		return false;
	}
}

public class DBLP {
	public static final String WeightsFName 	= "../data/DBLP_Weights_H"+RAQ.H+"_test_"+DBLPHelper.test;
	public static final String queryFName   	= "../data/dblp/dblp"+(DBLPHelper.test?"_test_true":"")+".query";
	public static final String queryFNameBase   = "../data/DBLP_test_"+DBLPHelper.test+"TargetSizeExperiment.query";
	public static final String permutationFname = "../data/DBLP_test_"+DBLPHelper.test+"_nodePermutation";

	//generate partial graph
	static final boolean test = DBLPEntry.test;
	static final int testID   = DBLPEntry.testID;

	static private Graph <DBLPFeatures> DBLPGraph;
	static private ArrayList<ArrayList<Graph<DBLPFeatures>.Edge>> queryGraphList;
	
	//this is the order of features used
	enum allFeatures {
		//authors, 
		numAuthors,
		year, venue, rank, subjects;

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
	private static final Integer DBLP_Num_Nodes = 3198780;
	static double[][] Prob;
	static Probability probability;
	
	static private CGQHierarchicalIndex<DBLPFeatures> DBLPCGQhierarchicalIndex;
	private static String dataSet = RAQ.DataSet.DBLP.toString();
	
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
		RAQ.PrintHeader ("DBLP:"+name[name.length-1], mStart, mEnd,
				numQuery, indexingScheme, kList);
		LoadGraph (fName, indexingScheme);

		System.out.println("By PRESS ENTER to start the experiment : ");
		System.in.read();

		//RAQ.AnalyzeVeVectorBinary (DBLPGraph);
		//RAQ.AnalyzeDegreeDistribution (DBLPGraph);
		//writeEdges (fName+"_Edges.csv");
		if (useTestFile) {
			numQuery = 1;
		}
		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<DBLPFeatures>> queryGraphArray;
			System.out.println("Generating Random graphs : ");
			if (RAQ.edgeGraph)
				queryGraphArray = DBLPGraph.GetRandomSubgraphArrayEdge (0.4, mStart, mEnd);
			else {
				if (useTestFile)
					queryGraphArray = DBLPGraph.GetRandomSubgraphArrayNodeFromFile (testFile, mStart, mEnd);
				else 
					queryGraphArray = DBLPGraph.GetRandomSubgraphArrayNode (0.4, mStart, mEnd, true);
			}

			for (Graph<DBLPFeatures> queryGraph : queryGraphArray) {
				//we now have a query graph having m nodes
				queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);
				
				queryGraph.Print();
				
				for (int K : kList) {

					//Find the topK based on our indexing scheme
					ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>>> topKTA=null;

					System.out.print("Finding topK based on ");

					Helper.CallByReference<Integer> depth 			= new Helper.CallByReference<>();
					Helper.CallByReference<Long> 	numEdgesTouched = new Helper.CallByReference<>();
					Helper.CallByReference<Long> 	numDFS			= new Helper.CallByReference<>();

					if (indexingScheme != RAQ.IndexingScheme.All) {
						if (indexingScheme != RAQ.IndexingScheme.BaseCase)
							CGQTimeOut.startTimeOutLong();// 100sec sleep
						else
							CGQTimeOut.startTimeOut();
						TicToc.Tic();
					}
					switch (indexingScheme) {
					case BaseCase:
						System.out.print("Base Case for query graph "+i+" :     ");
						ModifiedTA<DBLPFeatures> baseCase =
								new ModifiedTA<>();
						topKTA = baseCase.GetTopKSubgraphs (K, queryGraph, 
								DBLPGraph, false, depth, numDFS,
								numEdgesTouched, false, true, false);
						break;
					case CGQHierarchicalindex:
						System.out.print("CGQ Index for query graph "+i+" :     ");
						CGQHierarchicalIndexQuery<DBLPFeatures> cgqHierarcicalIndexQuery =
								new CGQHierarchicalIndexQuery<>();
						topKTA = cgqHierarcicalIndexQuery.GetTopKSubgraphs(K, queryGraph, DBLPGraph,
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

						/*
					System.out.print("Base Case for query graph "+i+"                 :       ");
					TicToc.Tic();
					baseCase = new ModifiedTA<>();
					topKTA = baseCase.GetTopKSubgraphs (RAQ.K, queryGraph, 
							DBLPGraph, DBLPTaIndex, true, depth, numDFS,
							numEdgesTouched, false, true);

					elapsedTime=TicToc.Toc();
					RAQ.LogResultsAll (-1, elapsedTime, depth, numDFS, numEdgesTouched,
							topKTA, runTimeArr, depthArr, numDFSArr, numEdgesTouchedArr,
							indexingSchemeArr, numResultsFoundArr, avgDistanceArr);

						 */
						/*
					depth.element 			= 0;
					numDFS.element 			= 0L;
					numEdgesTouched.element = 0L;

					System.out.print("Degree Distribution index for query graph "+i+" :       ");
					TicToc.Tic();
					degreeDist = new ModifiedTA<>();
					topKTA = degreeDist.GetTopKSubgraphs (RAQ.K, queryGraph, 
							DBLPGraph, DBLPTaIndex, true, depth, numDFS,
							numEdgesTouched, false, false);
					elapsedTime=TicToc.Toc();
					RAQ.LogResultsAll (1, elapsedTime, depth, numDFS, numEdgesTouched,
							topKTA, runTimeArr, depthArr, numDFSArr, numEdgesTouchedArr,
							indexingSchemeArr, numResultsFoundArr, avgDistanceArr);
						 */
						/*
					depth.element 			= 0;
					numDFS.element 			= 0L;
					numEdgesTouched.element = 0L;					

					System.out.print("TA chi for query graph "+i+"                    :       ");
					TicToc.Tic();
					modifiedTAChi = new ModifiedTA<> ();
					topKTA = modifiedTAChi.GetTopKSubgraphs (RAQ.K, queryGraph, 
							DBLPGraph, DBLPTaIndex, true, depth, numDFS,
							numEdgesTouched, true, false);
					elapsedTime=TicToc.Toc();
					RAQ.LogResultsAll (3, elapsedTime, depth, numDFS, numEdgesTouched,
							topKTA, runTimeArr, depthArr, numDFSArr, numEdgesTouchedArr,
							indexingSchemeArr, numResultsFoundArr, avgDistanceArr);

					depth.element 			= 0;
					numDFS.element 			= 0L;
					numEdgesTouched.element = 0L;
					System.out.print("CGQ Index for query graph "+i+"                 :     ");
					TicToc.Tic();
					cgqIndexQuery = new CGQIndexQuery<>();
					topKTA = cgqIndexQuery.GetTopKSubgraphs(RAQ.K, queryGraph, DBLPGraph,
							DBLPCGQIndex, true, depth, numDFS, numEdgesTouched);
					elapsedTime=TicToc.Toc();
					RAQ.LogResultsAll (RAQ.IndexingScheme.CGQindex, elapsedTime, depth, numDFS, numEdgesTouched,
							topKTA, runTimeArr, depthArr, numDFSArr, numEdgesTouchedArr,
							indexingSchemeArr, numResultsFoundArr, avgDistanceArr);
					PrintTopK (topKTA);
						 */

						/*
					depth.element 			= 0;
					numDFS.element 			= 0L;
					numEdgesTouched.element = 0L;

					System.out.print("CGQ Hierarchical Index for query graph "+i+"    :     ");
					TicToc.Tic();
					cgqHierarcicalIndexQuery = new CGQHierarchicalIndexQuery<>();
					topKTA = cgqHierarcicalIndexQuery.GetTopKSubgraphs(RAQ.K, queryGraph, DBLPGraph,
							DBLPCGQhierarchicalIndex, true, depth, numDFS, numEdgesTouched,false);
					elapsedTime=TicToc.Toc();
					System.out.println("Number of results : "+topKTA.size());
					System.out.println("Target graph size : "+DBLPGraph.SizeEdges());
					System.out.println();
					RAQ.LogResultsAll (RAQ.IndexingScheme.CGQHierarchicalindex, elapsedTime, depth, numDFS, numEdgesTouched,
							topKTA, runTimeArr, depthArr, numDFSArr, numEdgesTouchedArr,
							indexingSchemeArr, numResultsFoundArr, avgDistanceArr);
					PrintTopK (topKTA);
						 */
						/*
					depth.element 			= 0;
					numDFS.element 			= 0L;
					numEdgesTouched.element = 0L;

					System.out.print("CGQ Hierarchical Index for query graph "+i+"    :     ");
					TicToc.Tic();
					cgqHierarcicalIndexQuery = new CGQHierarchicalIndexQuery<>();
					topKTA = cgqHierarcicalIndexQuery.GetTopKSubgraphs(RAQ.K, queryGraph, DBLPGraph,
							DBLPCGQhierarchicalIndex, true, depth, numDFS, numEdgesTouched, true);
					elapsedTime=TicToc.Toc();
					System.out.println("Number of results : "+topKTA.size());
					System.out.println("Target graph size : "+DBLPGraph.SizeEdges());
					System.out.println();
					RAQ.LogResultsAll (RAQ.IndexingScheme.CGQHierarchicalindexFuzzy, elapsedTime, depth, numDFS, numEdgesTouched,
							topKTA, runTimeArr, depthArr, numDFSArr, numEdgesTouchedArr,
							indexingSchemeArr, numResultsFoundArr, avgDistanceArr);
					PrintTopK (topKTA);

						 */
						depth.element 			= 0;
						numDFS.element 			= 0L;
						numEdgesTouched.element = 0L;

						System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
						CGQTimeOut.startTimeOut();
						boolean avoidTAHeuristics = !Helper.useHeuristics;
						TicToc.Tic();
						topKTA = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
								DBLPCGQhierarchicalIndex, true, depth, numDFS, numEdgesTouched,
								true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
						elapsedTime=TicToc.Toc();
						CGQTimeOut.stopTimeOut();
						
						if (InterruptSearchSignalHandler.Interrupt()) {
							//we were interrupted from keyboard
							InterruptSearchSignalHandler.ResetFlag(DBLPGraph);
							//return;
							Helper.searchInterruptMesseage ();
						}
						System.out.println("Number of results : "+topKTA.size());
						System.out.println("Target graph size : "+DBLPGraph.SizeEdges());
						System.out.println();
						RAQ.LogResultsAll (RAQ.IndexingScheme.CGQHierarchicalindexFuzzyBFS,
								elapsedTime, depth, numDFS, numEdgesTouched,
								topKTA, runTimeArr, depthArr, numDFSArr, numEdgesTouchedArr,
								indexingSchemeArr, numResultsFoundArr, avgDistanceArr);
						Helper.PrintTopK (topKTA);

						if (!test) {
							ResultLogger.LogResultAll(queryGraph.SizeNodes(), queryGraph.SizeEdges(),
									runTimeArr, false, depthArr, numDFSArr, numEdgesTouchedArr,
									indexingSchemeArr, K, numResultsFoundArr, avgDistanceArr, "DBLP");
						}
						break;
					case CGQHierarchicalindexFuzzy:
						//TODO
						break;
					case CGQHierarchicalindexFuzzyBFS:
						//TODO
						break;
					case BFSWOIndex:
						//TODO
						break;
					case BFSWOHeuristics:
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
						for (Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>> o : topKTA)
							dist += o.value;
						System.out.println("Avearge distance of Index based search : "+(dist/numResults));
						if (!test) {
							ResultLogger.LogResult(queryGraph.SizeNodes(), queryGraph.SizeEdges(),
									elapsedTime, false, depth.element, numDFS.element,
									numEdgesTouched.element, indexingScheme, K,
									numResults, (dist/numResults), "DBLP");
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

		CreateGraph (fName, 1.0, false);

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

	private static void CreateCGQHierarcicalIndex(int branchingFactor, boolean useHeuristics)
			throws InterruptedException, ExecutionException {
		int a;
		int b;
		a=0;
		b=DBLPGraph.nodeList.size();
		System.out.print("Creating CGQHierarchicalindex   :     ");
		ShowProgress.ShowPercent(a, b);
		DBLPCGQhierarchicalIndex = new CGQHierarchicalIndex<DBLPFeatures>(DBLPGraph.edgeSet,
				DBLPFeatures.numCategoricalFeatures, DBLPFeatures.numNonCategoricalFeatures,
				branchingFactor, useHeuristics); //false is to avoid using heuristics
		
		//for heuristics all edges have to summarized
		boolean showProgress = true;
		if (useHeuristics)
			DBLP.DBLPGraph.updateNeighbourhoodSummary(showProgress);
		
		/*
		//create local index structure in each node
		System.out.print("\nCreating local index structures :     ");
		for (Graph<DBLPFeatures>.Node node : DBLPGraph.nodeList) {
			ShowProgress.ShowPercent(++a, b);
			node.cgqHierarchicalIndex = new CGQHierarchicalIndex<DBLPFeatures> (node.edges,
					DBLPFeatures.numCategoricalFeatures, DBLPFeatures.numNonCategoricalFeatures);
		}
		*/
		System.out.println();
		System.out.println("created");
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
	private static void CreateGraph(String fName, double frac, boolean runTargetGraphSize)
			throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {		
		//loading data to memory
		ArrayList<DBLPEntry> dblpEntries;
		
		ArrayList<Integer> nodePermutation = LaodPermuation (null);//permutationFname);

		System.out.print("Loading data into memory        :     ");
		//parse the CORE file
		ConferenceInfo.Init(fName);
		dblpEntries	= DBLPEntry.loadDataset(fName, nodePermutation);
		
		System.out.println();
		System.out.print("Loading the graph to memory     :     ");
		DBLPGraph = new Graph<>(dblpEntries.size(), true);
		
		probabilityInit();

		//Add nodes, and their features values
		int id=0;
		int maxID = (int) (dblpEntries.size()*frac);
		
		//sort the entries
		//this sorting in crucial do not undo it, we also sort the nodeid in other class
		Collections.sort(dblpEntries);
		
		for (Integer index : nodePermutation) {
			DBLPEntry entry = dblpEntries.get(index);
			if (test) {
				if (id == testID)
					break;
			} else {
				if (id == maxID)
					break;
			}
			
			//add each entry in to the graph as node
			DBLPFeatures dblpFeatures = new DBLPFeatures(entry);
			
			/*
			//this is not neede I hope
			if (dblpFeatures.PaperID() != id) {
				System.err.println("Fatal error DBLP.CreateGraph : we expect paper ids to be sequential");
				System.exit(-1);
			}
			*/
			DBLPGraph.AddNode(id++, dblpFeatures);
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
		
		//find the max difference in years in reference
		for (Integer index : nodePermutation) {
			DBLPEntry entry = dblpEntries.get(index);
			if (test) {
				if (id == testID)
					break;
			} else {
				if (id == maxID)
					break;
			}
			HashSet<Integer> neighboursYear = entry.NeighboursYear(frac);

			//add edge from node->neighbour
			for (int year : neighboursYear) {
				//update year difference
				int diff = Math.abs(entry.Year() - year);
				
				DBLPFeatures.updateFarthestReference(diff, false);
			}
			ShowProgress.Show();
		}
		
		DBLPFeatures.updateFarthestReference(0, true);

		//add edges
		id=0;
		for (Integer index : nodePermutation) {
			DBLPEntry entry = dblpEntries.get(index);
			if (test) {
				if (id == testID)
					break;
			} else {
				if (id == maxID)
					break;
			}
			int nodeID = entry.NodeID();
			id++;
			
			HashSet<Integer> neighbours = entry.Neighbours(frac);
			
			//add edge from node->neighbour
			for (int neighbour : neighbours) {
				if (test) {
					if (neighbour >= testID)
						continue;
				} else {
					if (neighbour >= maxID)
						continue;
				}
				//neighbour nodeid
				DBLPGraph.AddEdge(nodeID, neighbour);
			}
			ShowProgress.Show();
		}
		
		//we shall update  the statistic of each feature to
		//find the probability of the feature
		probability.UpdateProbability (DBLPGraph.edgeSet);
		
		//we now have a count of the features so we shall now make it a probability
		probability.UpdateProbability ();

		//freeing memory
		dblpEntries = null;
		
		System.out.println("");
		System.out.println("Num edges                       : "+DBLPGraph.SizeEdges());
		System.out.println("Num Nodes                       : "+DBLPGraph.SizeNodes());
		
		DBLPGraph.DegreeStat();
		DBLPFeatures.PrintStat();
		DBLPGraph.Analyse();

		//freeing memory
		nodePermutation = null;
	}


	private static void probabilityInit() {
		int []size =new int[numFeatures];
		do {
			//all this mombo jumbo to ensure we do not miss out a feature
			allFeatures allF = allFeatures.venue;
			switch (allF) {
			case venue:
			case numAuthors:
			//case authors:
			case rank:
			case year:
			case subjects:
				size[allFeatures.venue.ordinal()]	    = DBLPEntry.NumVenues();
				//size[allFeatures.authors.ordinal()]	= DBLPEntry.NumAuthors();  
				size[allFeatures.numAuthors.ordinal()]	= DBLPEntry.MaxNumAuthors();   
				size[allFeatures.year.ordinal()]		= DBLPEntry.NumYears();
				size[allFeatures.rank.ordinal()]		= DBLPEntry.NumRanks(); 
				size[allFeatures.subjects.ordinal()]	= DBLPEntry.NumSubjects();
				break;
			}
		} while (false);

		probability = new Probability(numFeatures, size);
		//////////////////
		
		//allocate memory for probability
		Prob = new double[numFeatures][];
		Prob[allFeatures.venue.ordinal()]	= new double [DBLPEntry.NumVenues()];
		//Prob[allFeatures.authors.ordinal()]	= new double [DBLPEntry.NumAuthors()];
		Prob[allFeatures.numAuthors.ordinal()]	= new double [DBLPEntry.MaxNumAuthors()+1];
		Prob[allFeatures.year.ordinal()]	= new double [DBLPEntry.NumYears()];
		Prob[allFeatures.rank.ordinal()]	= new double [DBLPEntry.NumRanks()+1];
		Prob[allFeatures.subjects.ordinal()]= new double [DBLPEntry.NumSubjects()];
	}
	
	/**
	 * We want to shuffle the input file so rather we shuffle the index of the dblp entries,
	 * to preserve the shuffle across various fractions of target-graph we use the same shuffle
	 * @param permutationfname2
	 * @return
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	private static ArrayList<Integer> LaodPermuation(String fName) throws NumberFormatException, IOException {
		ArrayList<Integer> ret = new ArrayList<>(DBLP_Num_Nodes+2);
		
		if (fName == null) {
			//no permutation to be performed
			for (Integer i=0; i<DBLP_Num_Nodes; i++) {
				ret.add(i);
			}
		} else {
			BufferedReader br = new BufferedReader(new FileReader(fName));
			Scanner scanner = new Scanner(br);

			while (scanner.hasNextInt() ) {
				ret.add(scanner.nextInt());
			}

			scanner.close();
			br.close();
		}
		return ret;
	}


	private static void UpdateProbability(DBLPFeatures features) throws InterruptedException, ExecutionException {
		if (features == null) {
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
					//we have to convert all the counts in to probability
					for (int d=0; d<Prob[f].length; d++) {
						sum += Prob[f][d];
					}
					for (int d=0; d<Prob[f].length; d++) {
						Prob[f][d] /= sum;
					}
				}
			}
		} else {
			Prob[allFeatures.venue.ordinal()][features.Venue()]++;
			//for (int author : features.Authors())
				//Prob[allFeatures.authors.ordinal()][author]++;
			Prob[allFeatures.numAuthors.ordinal()][features.numAuthors()]++;
			Prob[allFeatures.year.ordinal()][features.YearProb()]++;
			Prob[allFeatures.rank.ordinal()][features.Rank()]++;
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
	public static void Run_Graphsize_vs_Runtime_withUniformWeight(int mStart, int mEnd, int numQuery, int K, int maxEdges)
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
				K, maxEdges, printResults, avoidTAHeuristics,
				considerUniformWeight);
	}
	
	public static void Run_Graphsize_vs_Runtime(int mStart, int mEnd, int numQuery, 
			int K, int maxEdges, boolean printResults,  boolean avoidTAHeuristics,
			boolean considerUniformWeight)
	
			throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
		//initialize the graph and index structure
		Init();
		
		String fname;
		if (considerUniformWeight)
			fname=Helper.GetFname(Experiments.AllExperiments.Graphsize_vs_Runtime_uniform, 1.0, dataSet);
		else
			fname=Helper.GetFname(Experiments.AllExperiments.Graphsize_vs_Runtime, 1.0, dataSet);
		ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);
		ArrayList<Graph<DBLPFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			//create Query query Graphs
			queryGraphArray = DBLPGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<DBLPFeatures> queryGraph : queryGraphArray) {
				//we do not want to run an experiment on more than max edges 
				if(queryGraph.SizeEdges() > maxEdges)
					break;
				
				//we now have a query graph having m nodes
				if (considerUniformWeight)
					queryGraph.UpdateWeightsUniformly();
				else
					queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);
				
				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>>> topK;

				if (printResults)
					queryGraph.Print();
				showProgress = printResults;
				System.out.print("Processing query graph "+i+" of size "+queryGraph.SizeEdges() +"    :     ");

				CGQTimeOut.startTimeOut();
				TicToc.Tic(printResults);
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
						DBLPCGQhierarchicalIndex, showProgress, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();
				
				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPGraph);
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
		boolean doNotLoadQuery = false;
		Init(1.0, false, branchingFactor, doNotLoadQuery);
	}

	private static void Init()
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		Init(1.0, false);		
	}

	private static void Init(double frac, boolean runTargetSizeExperiment)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean doNotLoadQuery = false;
		Init(frac, runTargetSizeExperiment, doNotLoadQuery);
	}
	
	private static void Init(double frac, boolean runTargetSizeExperiment,
			boolean doNotLoadQuery) throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		Init (frac, runTargetSizeExperiment, RAQ.BranchingFactor, doNotLoadQuery);		
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
			int branchingFactor, boolean doNotLoadQuery)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean useHeuristics = Helper.useHeuristics;
		if (DBLPGraph == null) {
			System.out.println("Loading the DBLP Data set");
			CreateGraph 	(DBLPHelper.fName, frac, runTargetSizeExperiment);
			CreateCGQHierarcicalIndex(branchingFactor, useHeuristics);
			
			//load the query graphs
			if (doNotLoadQuery == true) {
				queryGraphList = null;
			} else {
				if (runTargetSizeExperiment) {
					queryGraphList = DBLPGraph.LoadQueryGraphEdgeList(queryFNameBase);//+"_"+frac);
				} else {
					queryGraphList = DBLPGraph.LoadQueryGraphEdgeList(queryFName);
				}
			}
		} else {
			if (DBLPCGQhierarchicalIndex.branchingFactor != branchingFactor)
				CreateCGQHierarcicalIndex(branchingFactor, useHeuristics);
		}
	}

	/**
	 * De-initialize the graph and index structure
	 */
	private static void DeInit () {
		DBLPGraph = null;
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
			ResultLogger.Para_vs_Runtime resultLogger= 
					new ResultLogger.Para_vs_Runtime (fname,
							Experiments.AllExperimentsParameters.K_vs_Runtime);
			resultLogger.PrintAllResults ();
		}
		break;
		case K_vs_Runtime_uniform: {
			ResultLogger.Para_vs_Runtime resultLogger= 
					new ResultLogger.Para_vs_Runtime (fname,
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
			resultLogger.PrintAllResults (30, minimal);
		}
		break;
		case BeamWOIndex: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BFSWOIndex);
			resultLogger.PrintAllResults ();
		}
		break;
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
	 * @param maxEdges : maximum number of edges in the query graph 
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
		
		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<DBLPFeatures>> queryGraphArray;
			queryGraphArray = DBLPGraph.GetRandomSubgraphArrayFromEdgeList
					(m, m, queryGraphList.get(i));
			
			Graph<DBLPFeatures> queryGraph = queryGraphArray.get(0);
			if (useUniformWeight)
				queryGraph.UpdateWeightsUniformly();
			else
				queryGraph.UpdateWeights(probability);
			
			boolean showProgress = false;
			queryGraph.updateNeighbourhoodSummary(showProgress);
			
			System.out.println("Query Graph");
			queryGraph.Print();
			
			boolean avoidTAHeuristics = !Helper.useHeuristics;
			
			for (int para : paraL){
				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>>> topK = null;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				int K = Experiments.K;
				
				CGQTimeOut.startTimeOut();
				TicToc.Tic();
				switch (parameter) {
				case BeamWidth:
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
							DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
							para, RAQ.WidthThreshold, false);
					break;
				case K_vs_Runtime:
				case K_vs_Runtime_Uniform:
					K = para;
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
							DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
							RAQ.BeamWidth, RAQ.WidthThreshold, false);
					break;
				case WidthThreshold:
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
							DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
							RAQ.BeamWidth, para, false);
					break;
				case BranchingFactor:
					System.err.println("Run_Para_vs_Runtime : BranchingFactor Not implemented");
					return;
				}
				
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();
				
				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPGraph);
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
		DeInit();
		String fname=Helper.GetFname(Experiments.AllExperiments.BaseCase, 1.0, dataSet);
		RunExperimentHelper (mStart, mEnd, numQuery, maxEdges, RAQ.IndexingScheme.BaseCase, fname);
		DeInit();
	}
	
	static void RunExperimentHelper(int mStart, int mEnd, int numQuery, int maxEdges,
			RAQ.IndexingScheme indexingSchemeComparison, String fname)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		//initialize the graph and index structure
		Init();
		

		ResultLogger.WeVsRest resultLogger =
				new ResultLogger.WeVsRest(fname, indexingSchemeComparison);
		
		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<DBLPFeatures>> queryGraphArray;
			//create Query query Graphs
			queryGraphArray = DBLPGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<DBLPFeatures> queryGraph : queryGraphArray) {
				queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);
				
				System.out.println("Query Graph");
				queryGraph.Print();
			}
			

			int K = Experiments.K;
			//our technique
			for (Graph<DBLPFeatures> queryGraph : queryGraphArray) {
				boolean avoidTAHeuristics = !Helper.useHeuristics;

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>>> topK;

				//using our technique
				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				if (indexingSchemeComparison == RAQ.IndexingScheme.BaseCase)
					CGQTimeOut.startTimeOutLong();
				else
					CGQTimeOut.startTimeOut();
				
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
						DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme, K, elapsedTime, topK.size(), -1);
			}
			
			//run other experiment
			for (Graph<DBLPFeatures> queryGraph : queryGraphArray) {
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>>> topK=null;
				long elapsedTime=0;

				System.out.println();
				if (indexingSchemeComparison == RAQ.IndexingScheme.BaseCase) {
					//run base case
					System.out.print("Base Case for query graph "+i+" :     ");
					ModifiedTA<DBLPFeatures> baseCase =
							new ModifiedTA<>();
					
					CGQTimeOut.startTimeOutLong();
					TicToc.Tic();	
					topK = baseCase.GetTopKSubgraphs (K, queryGraph, DBLPGraph, false, null,
							null, null, false, true, true);
					elapsedTime=TicToc.Toc();
					CGQTimeOut.stopTimeOut();
				} else if (indexingSchemeComparison == RAQ.IndexingScheme.BFSWOIndex) {
					System.out.print("Beam without any "+i+" :     ");

					CGQTimeOut.startTimeOut();
					TicToc.Tic();	
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
							DBLPCGQhierarchicalIndex, true, null, null, null, true, true,
							RAQ.BeamWidth, RAQ.WidthThreshold, true);
					elapsedTime=TicToc.Toc();
					CGQTimeOut.stopTimeOut();
				} else if (indexingSchemeComparison == RAQ.IndexingScheme.BFSWOHeuristics) {
					System.out.print("Beam without heuristics "+i+" :     ");

					boolean avoidTAHeuristics = true;
					CGQTimeOut.startTimeOut();
					TicToc.Tic();	
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
							DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
							RAQ.BeamWidth, RAQ.WidthThreshold, false);
					elapsedTime=TicToc.Toc();
					CGQTimeOut.stopTimeOut();
				} else {
					System.out.println("Fatal error wrong indexing scheme");
					System.exit(-1);
				}

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();
				resultLogger.Log (numNodes, numEdges, indexingSchemeComparison, K, elapsedTime, topK.size(), -1);
			}

			resultLogger.Flush();
		}
	}

	/**
	 * Generate the random inputs
	 * @param setSize
	 * @param listSize
	 * @param frac 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void GenerateRandomGraphs(int setSize, int listSize, double frac)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean doNotLoadQuery = true;
		Init(frac, false, doNotLoadQuery);
		
		System.out.println("Edges will be printed as list of edges represented as list of node pairs");
		DBLPGraph.GetSetOfRandomListOfConnectedEdges(setSize, listSize);
		
		DeInit();
	}



	/**
	 * Run query graph experiment for different fraction of inputs
	 * @param mStart
	 * @param mEnd
	 * @param numQuery
	 * @param K
	 * @param maxEdges
	 * @param frac
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void Run_TargetGraphSize(int mStart, int mEnd, int numQuery,
			int K, int maxEdges, double frac)
					throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		Init(frac, true);
		boolean avoidTAHeuristics = !Helper.useHeuristics;
		
		String fname=Helper.GetFname(Experiments.AllExperiments.TargetGraphSize, frac, dataSet);
		ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);
		ArrayList<Graph<DBLPFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			//create Query query Graphs
			queryGraphArray = DBLPGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<DBLPFeatures> queryGraph : queryGraphArray) {
				//we do not want to run an experiment on more than max edges 
				if(queryGraph.SizeEdges() > maxEdges)
					break;

				//we now have a query graph having m nodes
				queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);
				
				queryGraph.Print();

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>>> topK;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				CGQTimeOut.startTimeOut();
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
						DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPGraph);
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

		ArrayList<Graph<DBLPFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			//create Query query Graphs
			queryGraphArray = DBLPGraph.GetRandomSubgraphArrayFromEdgeList
					(m, m, queryGraphList.get(i));

			for (Graph<DBLPFeatures> queryGraph : queryGraphArray) {
				//we now have a query graph having m nodes
				queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);
				
				queryGraph.Print();

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>>> topK;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				boolean avoidTAHeuristics = !Helper.useHeuristics;
				CGQTimeOut.startTimeOut();
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(Experiments.K, queryGraph, DBLPGraph,
						DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				int numNodes = queryGraph.SizeNodes();
				int numEdges = queryGraph.SizeEdges();

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.CGQHierarchicalindexFuzzy;

				resultLogger.Log (numNodes, numEdges, indexingScheme,  Experiments.K, elapsedTime, topK.size(), branchingFactor);
			}

			resultLogger.Flush();
		}
		DeInit();
	}


	public static void RunQualitative(int mStart, int mEnd, int K)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		//initialize the graph and index structure
		Init();
		ArrayList<Graph<DBLPFeatures>> queryGraphArray;
		boolean avoidTAHeuristics = !Helper.useHeuristics;

		for (int i=0; i<queryGraphList.size(); i++) {
			//create Query query Graphs
			queryGraphArray = DBLPGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<DBLPFeatures> queryGraph : queryGraphArray) {				
				System.out.println("PRESS ENTER to see the query : ");
				System.in.read();
				//we now have a query graph having m nodes
				queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);
				
				queryGraph.Print();

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>>> topK;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				//CGQTimeOut.startTimeOut();
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, DBLPGraph,
						DBLPCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				TicToc.Toc();
				//CGQTimeOut.stopTimeOut();
				
				int l=1;
				for (Helper.ObjectDoublePair<ArrayList<Graph<DBLPFeatures>.Node>> od : topK) {
					ArrayList<Graph<DBLPFeatures>.Node> arr = od.element;
					System.out.println("PRESS ENTER to see the results "+ (l++) +": ");
					System.in.read();
					
					System.out.println("Mapping");
					for (int j =0 ;j<arr.size(); j++) {
						Graph<DBLPFeatures>.Node qNode = queryGraph.nodeList.get(j);
						Graph<DBLPFeatures>.Node tNode = arr.get(j);

						System.out.print(qNode.nodeID+":"+tNode.nodeID+",");
					}
					System.out.println();
					for (Graph<DBLPFeatures>.Node tNode : arr)
						tNode.features.Print();
				}
				
				if (InterruptSearchSignalHandler.Interrupt()) {
					
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(DBLPGraph);
					//break;
					return;
				}
			}
		}
	}


	
	/**
	 * Run experiment comparing search times with and without heuristics
	 * @param mstart
	 * @param menddblp
	 * @param numquery
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void RunHeurisitics(int mStart, int mEnd, int numQuery, int maxEdges)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		String fname=Helper.GetFname(Experiments.AllExperiments.Heuristicts, 1.0, dataSet);
		
		RunExperimentHelper (mStart, mEnd, numQuery, maxEdges, RAQ.IndexingScheme.BFSWOHeuristics, fname);		
	}
}
