import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class PokecFeatures extends NodeFeatures {
	//non features
	Integer oldId;
	Integer newId;

	//categorical
	int publicFriendships; 		//bool, 1 - all friendships are public  
	int gender; 				//bool, 1 - man
	//non-categorical
	int completionPercentage;	//integer, percentage proportion of filled values
	//region, 				//string, mostly regions in Slovakia (example: "zilinsky kraj,
	//kysucke nove mesto" means county Zilina, town Kysucke Nove Mesto,
	//Slovakia), some foreign countries (example: "zahranicie, 
	//zahranicie - nemecko" means foreign country Germany (nemecko)),
	//some Czech regions (example: "ceska republika, cz - ostravsky 
	// kraj" means Czech Republic, county Ostrava (ostravsky kraj))
	int lastLogin; 				//datetime, last time at which the user has logged in
	int registration;			//datetime, time at which the user registered at the site
	int age;

	//stat and other data
	public final static int numFeatures 		= Pokec.numFeatures;
	static final int numCategoricalFeatures    	= 2;
	static final int numNonCategoricalFeatures 	= numFeatures - numCategoricalFeatures;

	public PokecFeatures(PokecRawData.Profile profile) {
		oldId				=profile.userID;               
		newId				=profile.newId;               
		publicFriendships	=profile.publicFriendships? 1 : 0;   
		gender				=profile.genderMale? 1 : 0;              
		completionPercentage=profile.completionPercentage;
		lastLogin			=profile.lastLogin;           
		registration		=profile.registration;        
		age					=profile.age;                 
	}

	@Override
	String[] AllFeatures() {
		return Pokec.allFeaturesStrings;
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
		PokecFeatures nf = (PokecFeatures)features;

		int index;
		//categorical
		//publicFriendships
		index = Pokec.allFeatures.publicFriendships.ordinal();
		if (publicFriendships == nf.publicFriendships)
			ve[index]=1.0;
		else
			ve[index]=0;
		//gender
		index = Pokec.allFeatures.gender.ordinal();
		if (gender == nf.gender)
			ve[index]=1.0;
		else
			ve[index]=0;

		//non-categorical
		//completionPercentage
		index = Pokec.allFeatures.completionPercentage.ordinal();
		GetVeHelper(ve, completionPercentage, nf.completionPercentage, index);
		//lastLogin;
		index = Pokec.allFeatures.lastLogin.ordinal();
		GetVeHelper(ve, lastLogin, nf.lastLogin, index);
		//registration
		index = Pokec.allFeatures.registration.ordinal();
		GetVeHelper(ve, registration, nf.registration, index);
		//age
		index = Pokec.allFeatures.age.ordinal();
		GetVeHelper(ve, age, nf.age, index);

		return ve;
	}

	@Override
	public void Print() {
		System.out.print("oldId"+oldId);
		System.out.print("newId"+newId);
		System.out.print("publicFriendships"+publicFriendships); 
		System.out.print("gender"+ gender); 		
		System.out.print("completionPercentage"+ completionPercentage);
		System.out.print("lastLogin"+lastLogin); 			
		System.out.print("registration"+registration);		
		System.out.print("age"+age);
		System.out.println();
	}

	String CSVValue () {
		String s="";

		s += oldId+",";
		s += newId+",";
		s += publicFriendships+",";
		s += gender+",";
		s += completionPercentage+",";
		s += lastLogin+",";
		s += registration+",";
		s += age+",";

		return s;
	}

	@Override
	public void PrintCSV() {
		System.out.println(CSVValue());
	}

	@Override
	public void PrintCSVToFile(FileWriter fooWriter) throws IOException {
		fooWriter.write(CSVValue ()+"\n");
	}

	@Override
	public void PrintCSVHeader() {
		System.out.println(Headers());
	}


	String Headers () {
		String S;

		S = "Original userID (not a feature), New userID (not a feature),";

		for (String s : AllFeatures())
			S = S+s+", ";

		return S;
	}
	@Override
	public void PrintCSVHeaderToFile(FileWriter fooWriter) throws IOException {
		fooWriter.write(Headers());
		fooWriter.write("\n");
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
	ArrayList<ArrayList<String>> AllFeatureValues(NodeFeatures features2) {
		PokecFeatures neighbourFeature = (PokecFeatures)features2;

		ArrayList<ArrayList<String>> ret = new ArrayList<>(numFeatures);

		for (int i=0; i<numFeatures; i++) {
			ArrayList<String> s = new ArrayList<>(); 
			ret.add(s);
		}

		do {
			boolean diGraph = true;
			ArrayList<String> S;
			//all this mombo jumbo  of using switch case, to ensure we do not miss out a feature
			Pokec.allFeatures allF = Pokec.allFeatures.age;
			switch (allF) {
			case age:
				S = ret.get(Pokec.allFeatures.age.ordinal());
				S.add(getString(age, neighbourFeature.age, diGraph));
			case completionPercentage:
				S = ret.get(Pokec.allFeatures.completionPercentage.ordinal());
				S.add(getString(completionPercentage, neighbourFeature.completionPercentage, diGraph));
			case gender:
				S = ret.get(Pokec.allFeatures.gender.ordinal());
				S.add(getString(gender, neighbourFeature.gender, diGraph));
			case lastLogin:
				S = ret.get(Pokec.allFeatures.lastLogin.ordinal());
				S.add(getString(lastLogin, neighbourFeature.lastLogin, diGraph));
			case publicFriendships:
				S = ret.get(Pokec.allFeatures.publicFriendships.ordinal());
				S.add(getString(publicFriendships, neighbourFeature.publicFriendships, diGraph));
			case registration:
				S = ret.get(Pokec.allFeatures.registration.ordinal());
				S.add(getString(registration, neighbourFeature.registration, diGraph));
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
		Pokec.allFeatures allF = Pokec.allFeatures.age;
		switch (allF) {
		case age:
			i = Pokec.allFeatures.age.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case completionPercentage:
			i = Pokec.allFeatures.completionPercentage.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case gender:
			i = Pokec.allFeatures.gender.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case lastLogin:
			i = Pokec.allFeatures.lastLogin.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case publicFriendships:
			i = Pokec.allFeatures.publicFriendships.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		case registration:
			i = Pokec.allFeatures.registration.ordinal();
			j = veToBinIndex(ve[i]);
			neighbourhoodSummary[i][j]++;
		}

		return neighbourhoodSummary;
	}

}

/**
 * Raw data of Pokec
 * @author jithin
 *
 */
class PokecRawData {
	public static final String fNameProfiles 		= "../data/Pokec/soc-pokec-profiles.txt";
	public static final String fNameRelationships 	= "../data/Pokec/soc-pokec-relationships.txt";

	static Integer maxAge = 0; //maximum age of any user in the dataset

	static HashMap<Integer, Integer> oldIdTonewId = new HashMap<>();
	static Integer numEdges=0;

	static class Profile implements Comparable<Profile> {
		Integer userID;
		boolean publicFriendships;
		Integer completionPercentage;
		boolean genderMale;
		Integer lastLogin;
		Integer registration;
		Integer age;
		HashSet<Integer> neighboursUserId = new HashSet<>();

		Integer newId;

		Profile (Integer userID, boolean publicFriendships, Integer completionPercentage,
				boolean genderMale,	Integer lastLogin, Integer registration, Integer age) {
			this.userID               = userID;
			this.publicFriendships    = publicFriendships;
			this.completionPercentage = completionPercentage;
			this.genderMale           = genderMale;
			this.lastLogin            = lastLogin;
			this.registration         = registration;
			this.age                  = age;
		}

		@Override
		public int compareTo(Profile o) {
			return Integer.compare(userID, o.userID);
		}

		/**
		 * set the new ID and maintain hash
		 * @param integer
		 */
		public void SetID(Integer id) {
			newId = id;
			oldIdTonewId.put(userID, id);
		}

		/**
		 * Add relation to the corresponding node
		 * @param from
		 * @param to
		 */
		public static void AddRelation(Integer from, Integer to) {
			//find the node
			Integer newFrom = oldIdTonewId.get(from);
			if (newFrom == null) {
				//the node does not exist
			} else {
				Profile profile = profiles.get(newFrom);
				//sanity check
				if (profile.newId != newFrom) {
					System.err.println("newId != "+newFrom);
					System.exit(-1);
				}

				profile.neighboursUserId.add(to);
				numEdges++;
			}
		}

		static void Free () {
			oldIdTonewId.clear();
		}
	}
	static ArrayList<Profile> profiles = new ArrayList<>();

	/**
	 * Note : any memory allocated in this should be freed in Free()
	 * @throws IOException 
	 */
	public static void Load() throws IOException {
		//loading profiles
		LoadProfiles ();

		//loading relationships
		LoadRelationships ();

		System.out.println("num nodes : "+ profiles.size());
		System.out.println("num edges : "+ numEdges);
	}

	private static void LoadProfiles() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fNameProfiles));

		ArrayList<String> allLastLogin 		= new ArrayList<>();
		ArrayList<String> allRegistration 	= new ArrayList<>();
		ArrayList<String> allAges			= new ArrayList<>();

		String l;
		while ((l = br.readLine()) != null) {
			String[] S = l.split("\t");

			if(S.length > 0) {
				int i=0;

				Integer userID = Integer.parseInt(S[i++]);

				boolean publicFriendships = false;
				if (Integer.parseInt(S[i++]) == 1)
					publicFriendships = true;

				Integer completionPercentage = Integer.parseInt(S[i++]);
				boolean genderMale = Helper.random.nextBoolean();

				if ("null".compareTo(S[i]) != 0)
					if (Integer.parseInt(S[i++]) == 1)
						genderMale = true;

				i++; //region is discarded

				String dateTime;

				dateTime = S[i++];
				if ("null".compareTo(dateTime) == 0) {
					//pick one random time from all seen so far
					dateTime = allLastLogin.get(Helper.random.nextInt(allLastLogin.size()));
				} else
					allLastLogin.add(dateTime);
				Integer lastLogin 		= GetDate (dateTime);

				dateTime = S[i++];
				if ("null".compareTo(dateTime) == 0) {
					//pick one random time from all seen so far
					dateTime = allRegistration.get(Helper.random.nextInt(allRegistration.size()));
				} else
					allRegistration.add(dateTime);

				Integer registration 	= GetDate (dateTime);

				String ageS = S[i++];
				if ("null".compareTo(ageS) == 0) {
					//pick one random time from all seen so far
					ageS = allAges.get(Helper.random.nextInt(allAges.size()));
				} else
					allAges.add(ageS);

				Integer age = Integer.parseInt(ageS);
				if (age > maxAge)
					maxAge = age;

				profiles.add(new Profile(userID, publicFriendships,
						completionPercentage, genderMale, lastLogin,
						registration, age));
				ShowProgress.Show();
			}
		}

		Collections.sort(profiles);

		Integer i=0;
		for (Profile p : profiles)
			p.SetID (i++);

		allLastLogin.clear();
		allRegistration.clear();
		allAges.clear();
		br.close();
	}

	/**
	 * Convert the date into the number of days since yearStart jan 1
	 * I am assuming all the years have 365 days and month has 30
	 * @param string
	 * @return
	 */
	static Integer GetDate(String inDate) {
		//split on space
		String S[] = inDate.split(" ");
		Integer yearStart = 1995;
		String d[] = S[0].split("-");

		int i=0;
		Integer year 	= Integer.parseInt(d[i++]);
		Integer month 	= Integer.parseInt(d[i++]);
		Integer day 	= Integer.parseInt(d[i++]);

		if (year < yearStart) {
			System.err.println("error : year < "+yearStart);
			System.exit(-1);
		}

		year -= yearStart;

		return (year*365 + month*30 + day);
	}

	private static void LoadRelationships() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fNameRelationships));

		String l;
		while ((l = br.readLine()) != null) {
			String S[] = l.split("\t");

			Integer from = Integer.parseInt(S[0]);
			Integer to	 = Integer.parseInt(S[1]);

			Profile.AddRelation (from, to);
			ShowProgress.Show();
		}

		br.close();
	}

	/**
	 * free the memory
	 */
	public static void Free() {
		profiles.clear();
		oldIdTonewId.clear();
		numEdges=0;
		Profile.Free();
		maxAge = 0;
	}
}

public class Pokec {
	public static int numFeatures = allFeatures.values().length;
	static private Graph <PokecFeatures> PokecGraph;
	public static final String WeightsFName   	= "../data/Pokec/Pokec_Weights_test_false";
	public static final String queryFName   	= "../data/Pokec/Pokec.query";
	public static final String queryFNameBase   = "../data/Pokec/Pokec_test_false_0.25.query";

	//this is the order of features used
	enum allFeatures {
		//categorical
		publicFriendships, 		//bool, 1 - all friendships are public  
		gender, 				//bool, 1 - man
		//non-categorical
		completionPercentage,	//integer, percentage proportion of filled values
		//region, 				//string, mostly regions in Slovakia (example: "zilinsky kraj,
		//kysucke nove mesto" means county Zilina, town Kysucke Nove Mesto,
		//Slovakia), some foreign countries (example: "zahranicie, 
		//zahranicie - nemecko" means foreign country Germany (nemecko)),
		//some Czech regions (example: "ceska republika, cz - ostravsky 
		// kraj" means Czech Republic, county Ostrava (ostravsky kraj))
		lastLogin, 				//datetime, last time at which the user has logged in
		registration,			//datetime, time at which the user registered at the site
		age; 					//integer, 0 - age attribute not set

		public static String[] strings() {
			String []ret = new String[allFeatures.values().length];

			int i=0;
			for (allFeatures features:allFeatures.values()) {
				ret[i++] = features.toString();
			}

			return ret;
		}
	}
	static final  String[] allFeaturesStrings = allFeatures.strings();

	static double[][] Prob;
	static Probability probability;

	static private CGQHierarchicalIndex<PokecFeatures>PokecCGQhierarchicalIndex;
	static private ArrayList<ArrayList<Graph <PokecFeatures>.Edge>> queryGraphList;

	/**
	 * Generate the random inputs
	 * @param setSize : how many random graphs
	 * @param listSize : how many edges in the random graph
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void GenerateRandomGraphs(int setSize, 
			int listSize, double frac) throws IOException, InterruptedException, ExecutionException {
		boolean doNotLoadQueryGraphs    = true;
		boolean runTargetSizeExperiment = false;
		Init(frac, runTargetSizeExperiment, doNotLoadQueryGraphs);

		System.out.println("Edges will be printed as list "
				+ "of edges represented as list of node pairs");
		PokecGraph.GetSetOfRandomListOfConnectedEdges(setSize, listSize);
	}

	private static void Init(double frac, boolean runTargetSizeExperiment,
			boolean doNotLoadQueryGraphs) throws IOException, InterruptedException, ExecutionException {
		Init (frac, runTargetSizeExperiment, RAQ.BranchingFactor, doNotLoadQueryGraphs);
	}

	/**
	 * initialize Pokec
	 * @param frac fraction of data to be considered
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void Init(double frac) throws IOException, InterruptedException, ExecutionException {

		Init (frac, false);
	}

	private static void Init(int branchingFactor) throws IOException, InterruptedException, ExecutionException {
		boolean doNotLoadQueryGraphs    = false;
		Init (1.0, false, branchingFactor, doNotLoadQueryGraphs);
	}

	private static void Init(double frac, boolean runTargetSizeExperiment)
			throws IOException, InterruptedException, ExecutionException {
		boolean doNotLoadQueryGraphs    = false;
		Init (frac, runTargetSizeExperiment, RAQ.BranchingFactor, doNotLoadQueryGraphs);
	}

	private static void Init(double frac, boolean runTargetSizeExperiment,
			int branchingFactor, boolean doNotLoadQueryGraphs) throws IOException, InterruptedException, ExecutionException {
		if (PokecGraph == null) {
			System.out.println("Loading the pokec graph");
			CreateGraph(frac);
			//UpdateWeights	(PokecGraph, true, true, frac);
			CreateCGQHierarcicalIndex(branchingFactor);
			//PokecGraph.Analyse();

			//load the query graphs
			if (doNotLoadQueryGraphs) {
				queryGraphList = null;
			} else {
				if (runTargetSizeExperiment) {
					queryGraphList = PokecGraph.LoadQueryGraphEdgeList(queryFNameBase);
				} else {
					queryGraphList = PokecGraph.LoadQueryGraphEdgeList(queryFName);
				}
			}
		}
	}

	private static void CreateCGQHierarcicalIndex(int branchingFactor) throws InterruptedException, ExecutionException {
		int a;
		int b;
		a=0;
		b=PokecGraph.nodeList.size();
		System.out.print("Creating CGQHierarchicalindex   :     ");
		ShowProgress.ShowPercent(a, b);

		boolean withHeuristics = true;
		PokecCGQhierarchicalIndex = new CGQHierarchicalIndex<PokecFeatures>
		(PokecGraph.edgeSet, PokecFeatures.numCategoricalFeatures,
				PokecFeatures.numNonCategoricalFeatures, branchingFactor,
				withHeuristics, true);
		
		//for heuristics all edges have to be summarized
		boolean showProgress = true;
		boolean useHeuristics = Helper.useHeuristics;
		if (useHeuristics)
			PokecGraph.updateNeighbourhoodSummary(showProgress);

		System.out.println();
	}

	/**
	 * The graph data structure is being populated,
	 * and update the probability distribution
	 * @param frac : fraction of dataset to be used
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void CreateGraph(double frac) throws IOException, InterruptedException, ExecutionException {
		//load raw data into memory
		PokecRawData.Load ();

		int maxNumNodes = (int) (PokecRawData.profiles.size() * frac);
		PokecGraph = new Graph<>(maxNumNodes, true);

		//allocate memory for probability
		probabilityInit();

		int id = 0;
		for (PokecRawData.Profile profile : PokecRawData.profiles) {
			if (profile.newId != id) {
				System.err.println("fatale error : nodes out of order : Pokec.CreateGraph");
				System.exit(-1);
			}
			if (profile.newId == maxNumNodes)
				break;

			PokecFeatures features = new PokecFeatures(profile);

			PokecGraph.AddNode(id, features);
			UpdateProbability (features);
			id++;
			ShowProgress.Show();
		}
		//we now have a count of the features so we shall now make it a probability
		UpdateProbability (null);

		//add edges
		for (PokecRawData.Profile profile : PokecRawData.profiles) {
			if (profile.newId == maxNumNodes)
				break;

			for (Integer actualNeigbourID : profile.neighboursUserId) {
				Integer newIDNeighbour = PokecRawData.oldIdTonewId.get(actualNeigbourID);

				if (newIDNeighbour < maxNumNodes) {
					//we can add this edge
					PokecGraph.AddEdge(profile.newId, newIDNeighbour);
				} else {
					if (frac == 1)
						System.out.print(newIDNeighbour+":"+maxNumNodes+"?");
				}
			}
		}

		//we shall update  the statistic of each feature to
		//find the probability of the feature
		probability.UpdateProbability (PokecGraph.edgeSet);

		//we now have a count of the features so we shall now make it a probability
		probability.UpdateProbability ();

		System.out.println("Graph loaded to memory");
		System.out.println("Num edges                       : "+PokecGraph.SizeEdges());
		System.out.println("Num Nodes                       : "+PokecGraph.SizeNodes());

		//free raw data from memory
		PokecRawData.Free ();
	}

	private static void probabilityInit() {
		int []size =new int[numFeatures];
		do {
			//all this mombo jumbo to ensure we do not miss out a feature
			allFeatures allF = allFeatures.publicFriendships;
			switch (allF) {
			case age:
			case completionPercentage:
			case gender:
			case lastLogin:
			case publicFriendships:
			case registration:
				size[allFeatures.publicFriendships.ordinal()]	= 2;
				size[allFeatures.gender.ordinal()]				= 2;
				size[allFeatures.completionPercentage.ordinal()]= 101;
				size[allFeatures.lastLogin.ordinal()]			= PokecRawData.GetDate("2016-1-1");
				size[allFeatures.registration.ordinal()]		= PokecRawData.GetDate("2016-1-1");
				size[allFeatures.age.ordinal()]					= PokecRawData.maxAge+1;
				break;
			}
		} while (false);

		probability = new Probability(numFeatures, size);
		//////////////////

		//allocating memory for probability
		//we are adding "1" since the values start from 1, and not  zero
		Prob = new double[numFeatures][];
		Prob[allFeatures.publicFriendships.ordinal()]	= new double [2];
		Prob[allFeatures.gender.ordinal()]				= new double [2];
		Prob[allFeatures.completionPercentage.ordinal()]= new double [101];
		Prob[allFeatures.lastLogin.ordinal()]			= new double [PokecRawData.GetDate("2016-1-1")];
		Prob[allFeatures.registration.ordinal()]		= new double [PokecRawData.GetDate("2016-1-1")];
		Prob[allFeatures.age.ordinal()]					= new double [PokecRawData.maxAge+1];
	}

	private static void UpdateProbability(PokecFeatures features)
			throws InterruptedException, ExecutionException {
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
			Prob[allFeatures.publicFriendships.ordinal()][features.publicFriendships]++;
			Prob[allFeatures.gender.ordinal()][features.gender]++;
			Prob[allFeatures.completionPercentage.ordinal()][features.completionPercentage]++;
			Prob[allFeatures.lastLogin.ordinal()][features.lastLogin]++;
			Prob[allFeatures.registration.ordinal()][features.registration]++;
			Prob[allFeatures.age.ordinal()][features.age]++;
		}
	}

	public static void Run_Graphsize_vs_Runtime_withUniformWeight(int mStart, int mEnd,
			int numQuery, int K, int maxEdges)
					throws IOException, InterruptedException, ExecutionException {
		boolean printResults = true;
		boolean considerUniformWeight = true;
		Run_Graphsize_vs_Runtime(mStart, mEnd, numQuery, K, maxEdges, printResults, considerUniformWeight);
	}

	public static void Run_Graphsize_vs_Runtime(int mStart, int mEnd,
			int numQuery, int K, int maxEdges)
					throws IOException, InterruptedException, ExecutionException {
		boolean printResults = true;
		boolean considerUniformWeight = false;
		Run_Graphsize_vs_Runtime(mStart, mEnd, numQuery, K, maxEdges, printResults, considerUniformWeight);
	}

	public static void Run_Graphsize_vs_Runtime(int mStart, int mEnd,
			int numQuery, int K, int maxEdges, boolean printResults,
			boolean considerUniformWeights) throws IOException, InterruptedException, ExecutionException {

		//initialize the graph and index structure
		Init(1.0);

		String fname=Helper.GetFname(Experiments.AllExperiments.Graphsize_vs_Runtime, 1.0, RAQ.DataSet.Pokec.toString());
		ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);
		//create Query query Graphs
		ArrayList<Graph<PokecFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			queryGraphArray = PokecGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<PokecFeatures> queryGraph : queryGraphArray) {
				//we do not want to run an experiment on more than max edges 
				if(queryGraph.SizeEdges() > maxEdges)
					break;

				//we now have a query graph having m nodes
				if (considerUniformWeights) 
					queryGraph.UpdateWeightsUniformly();
				else
					queryGraph.UpdateWeights(probability);
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<PokecFeatures>.Node>>> topK;

				boolean avoidTAHeuristics = !Helper.useHeuristics;
				if (printResults)
					queryGraph.Print();
				showProgress = printResults;
				System.out.print("Processing query graph "+i+" of size "+queryGraph.SizeEdges() +"    :     ");
				CGQTimeOut.startTimeOut(); 
				TicToc.Tic(printResults);
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, PokecGraph,
						PokecCGQhierarchicalIndex, showProgress, null, null, null,
						true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();
				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(PokecGraph);
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

	public static void PrintResults (Experiments.AllExperiments experiment) {
		boolean minimal = false;

		PrintResults (experiment, minimal);
	}

	public static void PrintResults (Experiments.AllExperiments experiment, boolean minimal) {
		String fname=Helper.GetFname(experiment, 1.0, RAQ.DataSet.Pokec.toString());
		switch (experiment) {
		case BaseCase: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BaseCase);
			resultLogger.PrintAllResults ();
		}
		break;
		case BeamWOIndex: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BFSWOIndex);
			resultLogger.PrintAllResults ();
		}
		break;
		case Heuristicts: {
			ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,
					RAQ.IndexingScheme.BFSWOHeuristics);
			resultLogger.PrintAllResults ();
		}
		break;
		case BeamWidth: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.BeamWidth);
			resultLogger.PrintAllResults ();
		}
		break;
		case BranchingFactor: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.BranchingFactor);
			resultLogger.PrintAllResults ();
		}
		break;
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
			resultLogger.PrintAllResults (minimal);
		}
		break;
		case K_vs_Runtime_uniform: {
			ResultLogger.Para_vs_Runtime resultLogger= new ResultLogger.Para_vs_Runtime (fname,
					Experiments.AllExperimentsParameters.K_vs_Runtime_Uniform);
			resultLogger.PrintAllResults (minimal);
		}
		break;
		case Qualitative:
			break;
		case QualitativeQuantitative:
			break;
		case TargetGraphSize: {
			double frac = Experiments.GetFraction();
			fname=Helper.GetFname(experiment, frac, RAQ.DataSet.Pokec.toString());
			ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);			
			resultLogger.PrintAllResults (-1, minimal);
		}
		break;
		case WidthThreshold:
			break;
		}
	}

	public static void Run_TargetGraphSize(int mStart, int mEnd,
			int numQuery, int K, int maxEdges, double frac)
					throws IOException, InterruptedException, ExecutionException {
		Init(frac, true);

		String fname=Helper.GetFname(Experiments.AllExperiments.TargetGraphSize, frac, RAQ.DataSet.Pokec.toString());
		ResultLogger.Graphsize_vs_Runtime resultLogger= new ResultLogger.Graphsize_vs_Runtime (fname);
		ArrayList<Graph<PokecFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			//create Query query Graphs
			queryGraphArray = PokecGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<PokecFeatures> queryGraph : queryGraphArray) {
				//we do not want to run an experiment on more than max edges 
				if(queryGraph.SizeEdges() > maxEdges)
					break;

				//we now have a query graph having m nodes
				queryGraph.UpdateWeights(probability);
				queryGraph.Print();
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<PokecFeatures>.Node>>> topK;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				boolean avoidTAHeuristics = !Helper.useHeuristics;
				CGQTimeOut.startTimeOut(); 
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, PokecGraph,
						PokecCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(PokecGraph);
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

	private static void DeInit() {
		PokecGraph = null;
	}

	public static void Run_Para_vs_Runtime(Experiments.AllExperimentsParameters parameter,
			int mStart, int mEnd, int numQuery,
			ArrayList<Integer> paraL, int maxEdges)
					throws IOException, InterruptedException, ExecutionException {
		Init(1.0);

		String  fname=null;
		//useUniformWeight : do we want the query graph to have uniform weight
		boolean useUniformWeight=false;
		
		switch (parameter) {
		case BeamWidth:
			fname=Helper.GetFname(Experiments.AllExperiments.BeamWidth, 1.0, RAQ.DataSet.Pokec.toString());
			break;
		case K_vs_Runtime:
			fname=Helper.GetFname(Experiments.AllExperiments.K_vs_Runtime, 1.0, RAQ.DataSet.Pokec.toString());
			break;
		case K_vs_Runtime_Uniform:
			useUniformWeight = true;
			fname=Helper.GetFname(Experiments.AllExperiments.K_vs_Runtime_uniform, 1.0, RAQ.DataSet.Pokec.toString());
			break;
		case WidthThreshold:
			fname=Helper.GetFname(Experiments.AllExperiments.WidthThreshold, 1.0, RAQ.DataSet.Pokec.toString());
			break;		
		case BranchingFactor:
			System.err.println("Run_Para_vs_Runtime : BranchingFactor Not implemented");
			return;
		}

		ResultLogger.Para_vs_Runtime resultLogger =
				new ResultLogger.Para_vs_Runtime (fname, parameter);

		int m = (mStart+mEnd)/2;
		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<PokecFeatures>> queryGraphArray;
			queryGraphArray = PokecGraph.GetRandomSubgraphArrayFromEdgeList
					(m, m, queryGraphList.get(i));

			Graph<PokecFeatures> queryGraph =
					queryGraphArray.get(0);

			if (useUniformWeight)
				queryGraph.UpdateWeightsUniformly();
			else
				queryGraph.UpdateWeights(probability);
			System.out.println("Query Graph");
			queryGraph.Print();
			boolean showProgress = false;
			queryGraph.updateNeighbourhoodSummary(showProgress);

			boolean avoidTAHeuristics;
			for (int para : paraL){
				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<PokecFeatures>.Node>>> topK=null;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				int K = Experiments.K;

				CGQTimeOut.startTimeOut(); 
				TicToc.Tic();
				switch (parameter) {
				case BeamWidth:
					avoidTAHeuristics = !Helper.useHeuristics;
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, PokecGraph,
							PokecCGQhierarchicalIndex, true, null, null, null,
							true, false, para, RAQ.WidthThreshold, false);
					break;
				case K_vs_Runtime:
				case K_vs_Runtime_Uniform:
					K = para;
					avoidTAHeuristics = true;
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, PokecGraph,
							PokecCGQhierarchicalIndex, true, null, null, null,
							true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
					break;
				case WidthThreshold:
					avoidTAHeuristics = true;
					topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, PokecGraph,
							PokecCGQhierarchicalIndex, true, null, null, null,
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
					InterruptSearchSignalHandler.ResetFlag(PokecGraph);
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


	public static void RunBranchingFactor(int m, int branchingFactor, int numQuery)
			throws IOException, InterruptedException, ExecutionException {
		DeInit();
		Init(branchingFactor);

		String fname=Helper.GetFname(Experiments.AllExperiments.BranchingFactor, 1.0, RAQ.DataSet.Pokec.toString());

		ResultLogger.Para_vs_Runtime resultLogger = 
				new ResultLogger.Para_vs_Runtime (fname, Experiments.AllExperimentsParameters.BranchingFactor);

		ArrayList<Graph<PokecFeatures>> queryGraphArray;
		for (int i=0; i<numQuery; i++) {
			//create Query query Graphs
			queryGraphArray = PokecGraph.GetRandomSubgraphArrayFromEdgeList
					(m, m, queryGraphList.get(i));

			for (Graph<PokecFeatures> queryGraph : queryGraphArray) {
				//we now have a query graph having m nodes
				queryGraph.UpdateWeights(probability);
				queryGraph.Print();
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<PokecFeatures>.Node>>> topK;

				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				boolean avoidTAHeuristics = true;
				CGQTimeOut.startTimeOut(); 
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(Experiments.K, queryGraph, PokecGraph,
						PokecCGQhierarchicalIndex, true, null, null, null, true, avoidTAHeuristics,
						RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(PokecGraph);
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

	public static void Run_BaseCase(int mStart, int mEnd, int numQuery, int maxEdges)
			throws IOException, InterruptedException, ExecutionException {
		String fname=Helper.GetFname(Experiments.AllExperiments.BaseCase, 1.0, RAQ.DataSet.Pokec.toString());

		Init(1.0);
		
		boolean avoidTAHeuristics = !Helper.useHeuristics;
		ResultLogger.WeVsRest resultLogger= new ResultLogger.WeVsRest(fname,  RAQ.IndexingScheme.BaseCase);

		for (int i=0; i<numQuery; i++) {
			ArrayList<Graph<PokecFeatures>> queryGraphArray;
			queryGraphArray = PokecGraph.GetRandomSubgraphArrayFromEdgeList
					(mStart, mEnd, queryGraphList.get(i));

			for (Graph<PokecFeatures> queryGraph : queryGraphArray) {
				queryGraph.UpdateWeights(probability);
				System.out.println("Query Graph");
				queryGraph.Print();
				boolean showProgress = false;
				queryGraph.updateNeighbourhoodSummary(showProgress);

				int K = Experiments.K;

				//find the topK
				ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<PokecFeatures>.Node>>> topK;

				//using our technique
				System.out.print("CGQ Hierarchical BFS Index for query graph "+i+"    :     ");
				CGQTimeOut.startTimeOutLong(); 
				TicToc.Tic();
				topK = BFSQuery.GetTopKSubgraphsCGQHierarchical(K, queryGraph, PokecGraph,
						PokecCGQhierarchicalIndex, true, null, null,
						null, true, avoidTAHeuristics, RAQ.BeamWidth, RAQ.WidthThreshold, false);
				long elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();


				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(PokecGraph);
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

				//run base case
				System.out.print("Base Case for query graph "+i+" :     ");
				ModifiedTA<PokecFeatures> baseCase =
						new ModifiedTA<>();

				CGQTimeOut.startTimeOutLong();
				TicToc.Tic();	
				topK = baseCase.GetTopKSubgraphs (K, queryGraph, PokecGraph, false, null,
						null, null, false, true, true);
				elapsedTime=TicToc.Toc();
				CGQTimeOut.stopTimeOut();

				if (InterruptSearchSignalHandler.Interrupt()) {
					//we were interrupted from keyboard
					InterruptSearchSignalHandler.ResetFlag(PokecGraph);
					//resultLogger.Clear();
					//break;
					Helper.searchInterruptMesseage ();
				}

				resultLogger.Log (numNodes, numEdges, RAQ.IndexingScheme.BaseCase, K, elapsedTime, topK.size(), -1);
			}
			resultLogger.Flush();			
		}
	}
}
