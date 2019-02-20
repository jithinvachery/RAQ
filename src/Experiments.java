import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class Experiments {
	static final int K 			= 10;
	static final int MaxEdges 	= 30;
	static final int mStart	 	= 3;
	static final int mEnd	 	= 12;
	static final int mEndDBLP 	= 10;
	static final int numQuery 	= 30;

	static Scanner reader = new Scanner(System.in);
	static enum AllExperimentsParameters {
		K_vs_Runtime,
		BeamWidth,
		WidthThreshold,
		BranchingFactor,
		K_vs_Runtime_Uniform;
	}
	
	static enum AllExperiments {
		Exit,
		K_vs_Runtime,
		Graphsize_vs_Runtime,
		BaseCase,
		BeamWidth,
		WidthThreshold,
		TargetGraphSize,
		BeamWOIndex,
		BranchingFactor,
		Qualitative,
		QualitativeQuantitative,
		Heuristicts,
		K_vs_Runtime_uniform,
		Graphsize_vs_Runtime_uniform,
		Exemplar, //run the exemplar query
		ExemplarQuery, // produce the input file for exemplar query
		ExemplarQueryReverse; // convert the result of exemplar query to corresponding graph 
		
		static int maxLen = getMaxlen();
		private static AllExperiments previousConversion=AllExperiments.Exit;
		private static int getMaxlen() {
			int ret = -1;
			for (AllExperiments experiments : AllExperiments.values()) {
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

		public static AllExperiments ConvertIntToAllExperiments(int input) {
			AllExperiments ret = Exit;
			
			for (AllExperiments e : AllExperiments.values()) {
				if (e.ordinal() == input) {
					ret = e;
					break;
				}
			}
			previousConversion = ret;
			return ret;
		}
		
		public static String PreviousConversion () {
			return previousConversion.toString();
		}
	}

	static enum AllExperimentsSigmod {
		Exit,
		TopK,
		CoAuthor,
		IMDB;
		
		static int maxLen = getMaxlen();
		private static AllExperimentsSigmod previousConversion=AllExperimentsSigmod.Exit;

		private static int getMaxlen() {
			int ret = -1;
			for (AllExperimentsSigmod experiments : AllExperimentsSigmod.values()) {
				ret = Math.max(ret, experiments.toString().length());
			}
			return ret;
		}

		public String Message () {
			String ret="";
			switch (this) {
			case Exit:
				ret = "Exit";
				break;
			case CoAuthor:
				ret = "Perform a top-k query on the DBLP co-author network in a interactive manner";
				break;
			case TopK:
				ret = "Perform a top-10 query for query graphs of size varying from 3 to 10";
				break;
			case IMDB:
				ret = "Perform a top-k query on the IMDB network in a interactive manner";
				break;
			}
			return ret;
		}

		public static AllExperimentsSigmod ConvertIntToAllExperiments(int input) {
			AllExperimentsSigmod ret = Exit;
			
			for (AllExperimentsSigmod e : AllExperimentsSigmod.values()) {
				if (e.ordinal() == input) {
					ret = e;
					break;
				}
			}
			previousConversion = ret;
			return ret;
		}

		public static String PreviousConversion() {
			return previousConversion.toString();
		}
	}

	static void RunExperiments() throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		System.out.println("We are running experiments");

		boolean condition = true;
		while (condition) {
			AllExperiments input = GetUserInput();
			boolean considerUniformWeight;
			switch (input) {
			case Exit:
				condition = false;
				break;
			case Graphsize_vs_Runtime:
				considerUniformWeight = false;
				Run_Graphsize_vs_Runtime(considerUniformWeight);
				break;
			case Graphsize_vs_Runtime_uniform:
				considerUniformWeight = true;
				Run_Graphsize_vs_Runtime(considerUniformWeight);
				break;
			case K_vs_Runtime_uniform:
				considerUniformWeight = true;
				Run_Para_vs_Runtime(AllExperimentsParameters.K_vs_Runtime_Uniform, considerUniformWeight);
				break;
			case K_vs_Runtime:
				considerUniformWeight = false;
				Run_Para_vs_Runtime(AllExperimentsParameters.K_vs_Runtime, considerUniformWeight);
				break;
			case BaseCase:
				RunBaseCase();
				break;
			case BeamWidth:
				considerUniformWeight = false;
				Run_Para_vs_Runtime(AllExperimentsParameters.BeamWidth, considerUniformWeight);
				break;
			case WidthThreshold:
				considerUniformWeight = false;
				Run_Para_vs_Runtime(AllExperimentsParameters.WidthThreshold, considerUniformWeight);
				break;
			case TargetGraphSize:
				Run_TargetGraphSize();
				break;
			case BeamWOIndex:
				RunBeamWOIndex();
				break;
			case BranchingFactor:
				RunBranchingFactor();
				break;
			case Qualitative:
				RunQualitative();
				break;
			case QualitativeQuantitative:
				RunQualitativeQuantitative();
				break;
			case Heuristicts:
				RunHeuristicts();
				break;
			case Exemplar:
				RunExemplar();
				break;
			case ExemplarQuery:
				RunExemplarQuery();
				break;
			case ExemplarQueryReverse:
				RunExemplarQueryReverse();
				break;
			}
		}
	}

	/**
	 * Generate all the files as required by Exemplar
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void RunExemplarQuery() throws IOException, InterruptedException, ExecutionException {
		System.out.println("Experiment : "+AllExperiments.ExemplarQuery);
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
			case DBLPAlternate:
			case IMDB:
				IMDB.RunExemplarQuery(reader);
				break;
			case Pokec:
			case exit:
				condition=false;
				break;
			}
		}
	}
	/**
	 * Convert the out put of exemplar into the actual graph 
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void RunExemplarQueryReverse() throws IOException, InterruptedException, ExecutionException {
		System.out.println("Experiment : "+AllExperiments.ExemplarQuery);
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
			case DBLPAlternate:
			case IMDB:
				IMDB.RunExemplarQueryReverse(reader);
				break;
			case Pokec:
			case exit:
				condition=false;
				break;
			}
		}
	}
	/**
	 * Generate all the files as required by Exemplar
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void RunExemplar() throws IOException, InterruptedException, ExecutionException {
		System.out.println("Experiment : "+AllExperiments.Exemplar);
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
			case DBLPAlternate:
			case IMDB:
				IMDB.RunExemplar();
				break;
			case Pokec:
			case exit:
				condition=false;
				break;
			}
		}
	}

	/**
	 * Run query using with and without heuristics
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void RunHeuristicts()
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		System.out.println("Experiment : "+AllExperiments.Heuristicts);
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
				DBLP.RunHeurisitics (mStart, mEndDBLP, numQuery, MaxEdges);
				break;
			case DBLPAlternate:
			case Pokec:
			case IMDB:
				break;
			case exit:
				condition = false;
				break;
			}
		}
	}

	private static void RunQualitativeQuantitative() 
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
				break;
			case DBLPAlternate:
				break;
			case IMDB:
				break;
			case exit:
				condition = false;
				break;
			case Pokec:
				break;
			}
		}
	}

	private static void RunQualitative()
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		System.out.println("Experiment : "+AllExperiments.Qualitative);
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
				DBLP.RunQualitative (mStart, 6, 5);
				break;
			case DBLPAlternate:
				//DBLPAlternate.RunQualitative (mStart, 6, 5);
				DBLPAlternate.RunQualitativeInteractive(5, reader);
				break;
			case exit:
				condition = false;
				break;
			case Pokec:
				break;
			case IMDB:
				IMDB.RunQualitativeInteractive(3000, reader);
				break;
			}
		}
	}

	private static void RunBranchingFactor()
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		System.out.println("Experiment : "+AllExperiments.BeamWOIndex);

		HashSet<Integer> allowedValues = new HashSet<>();
		
		for (int i=2; i<=10; i+=2) {
			allowedValues.add(i);
		}

		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();

			for (int branchingFactor : allowedValues) {
				System.out.println("Branching factor : "+branchingFactor);
				switch (dataSet) {
				case DBLP:
					DBLP.RunBranchingFactor(7, branchingFactor, numQuery);
					break;
				case DBLPAlternate:
					DBLPAlternate.RunBranchingFactor(7, branchingFactor, numQuery);
					break;
				case exit:
					condition = false;
					break;
				case Pokec:
					Pokec.RunBranchingFactor(7, branchingFactor, numQuery);
					break;
				case IMDB:
					System.out.println("only qualitative experimets conducted");
					break;
				}
			}
		}
	}

	private static void RunBeamWOIndex()
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		System.out.println("Experiment : "+AllExperiments.BeamWOIndex);
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
				DBLP.RunBeamWOIndex(mStart, 5, numQuery, MaxEdges);
				break;
			case DBLPAlternate:
				DBLPAlternate.RunBeamWOIndex(mStart, 5, numQuery, MaxEdges);
				break;
			case exit:
				condition = false;
				break;
			case Pokec:
				System.out.println("Unimplemented");
				break;
			case IMDB:
				System.out.println("only qualitative experimets conducted");
				break;
			}
		}
	}

	private static void Run_TargetGraphSize()
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		
		System.out.println("Experiment : "+AllExperiments.TargetGraphSize);
		boolean condition = true;
		while (condition) {
			double frac = GetFraction();
			
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
				DBLP.Run_TargetGraphSize (mStart, mEndDBLP, numQuery, K, MaxEdges, frac);
				break;
			case DBLPAlternate:
				DBLPAlternate.Run_TargetGraphSize (mStart, mEndDBLP, numQuery, K, MaxEdges, frac);
				break;
			case exit:
				condition = false;
				break;
			case Pokec:
				Pokec.Run_TargetGraphSize (mStart, mEndDBLP, numQuery, K, MaxEdges, frac);
				break;
			case IMDB:
				System.out.println("only qualitative experimets conducted");
				break;
			}
		}
	}

	/**
	 * Get how much fraction of data is to be considered
	 * @return
	 */
	static double GetFraction() {
		double []para={0.25, 0.5, 0.75, 1.0};
		double frac;
		
		//What fraction to be considered
		while(true) {
			System.out.println("What fraction do you want?");
			int i=0;
			for (double p : para) {
				System.out.println(p+"\t: "+i++);
			}

			//get the input
			System.out.print("Input : ");
			int input;

			try {
				input = reader.nextInt();
				if ((input >= 0) &&
						(input < para.length)) {
					frac = para[input];
					break;
				}
			} catch (Exception e) {
				reader.next();
			}
		}
		
		return frac;
	}

	private static void RunBaseCase() throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		System.out.println("Experiment : "+AllExperiments.BaseCase);
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
				DBLP.Run_BaseCase(mStart, 5, numQuery, MaxEdges);
				break;
			case DBLPAlternate:
				DBLPAlternate.Run_BaseCase(mStart, 5, numQuery, MaxEdges);
				break;
			case exit:
				condition = false;
				break;
			case Pokec:
				Pokec.Run_BaseCase(mStart, 5, numQuery, MaxEdges);
				break;
			case IMDB:
				System.out.println("only qualitative experimets conducted");
				break;
			}
		}
	}

	/**
	 * 
	 * @param parameter
	 * @param useUniformWeight : do we want the query graph weights to be uniform
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void Run_Para_vs_Runtime(AllExperimentsParameters parameter,
			boolean useUniformWeight)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		
		ArrayList<Integer> para = new ArrayList<>(5);
		
		switch (parameter) {
		case BeamWidth:
			para.add(1);
			para.add(20);
			para.add(40);
			para.add(60);
			para.add(80);
			para.add(100);
			para.add(7000000);	
			break;
		case K_vs_Runtime:
		case K_vs_Runtime_Uniform:
			para.add(1);
			para.add(10);
			para.add(50);
			para.add(100);
			para.add(500);
			break;
		case WidthThreshold:
			para.add(20);
			para.add(40);
			para.add(60);
			para.add(80);
			para.add(100);
			break;
		case BranchingFactor:
			System.err.println("Run_Para_vs_Runtime : BranchingFactor Not implemented");
			return;
		}
		System.out.println("Experiment : "+parameter);
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
				DBLP.Run_Para_vs_Runtime (parameter, mStart, mEndDBLP, numQuery, para, MaxEdges);
				break;
			case DBLPAlternate:
				DBLPAlternate.Run_Para_vs_Runtime (parameter, mStart, mEndDBLP, numQuery, para, MaxEdges);
				break;
			case exit:
				condition = false;
				break;
			case Pokec:
				Pokec.Run_Para_vs_Runtime (parameter, mStart, mEndDBLP, numQuery, para, MaxEdges);
				break;
			case IMDB:
				IMDB.Run_Para_vs_Runtime (parameter, mStart, mEndDBLP, numQuery, para, MaxEdges);
				break;
			}
		}
	}

	/**
	 * 
	 * @param considerUniformWeight : should we set the query graph weights as uniform
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	private static void Run_Graphsize_vs_Runtime(boolean considerUniformWeight)
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean condition = true;
		
		if (considerUniformWeight) {
			System.out.println("Experiment : "+AllExperiments.Graphsize_vs_Runtime_uniform);
			while (condition) {
				RAQ.DataSet dataSet = GetUserInputDataSet();

				switch (dataSet) {
				case DBLP:
					DBLP.Run_Graphsize_vs_Runtime_withUniformWeight (mStart, mEndDBLP, numQuery, K, MaxEdges);
					break;
				case DBLPAlternate:
					DBLPAlternate.Run_Graphsize_vs_Runtime_withUniformWeight (mStart, mEndDBLP, numQuery, K, MaxEdges);
					break;
				case exit:
					condition = false;
					break;
				case Pokec:
					Pokec.Run_Graphsize_vs_Runtime_withUniformWeight (mStart, mEndDBLP, numQuery, K, MaxEdges);
					break;
				case IMDB:
					IMDB.Run_Graphsize_vs_Runtime_withUniformWeight (mStart, mEndDBLP, numQuery, K, MaxEdges);
					break;
				}
			}			
		} else {
			System.out.println("Experiment : "+AllExperiments.Graphsize_vs_Runtime);
			while (condition) {
				RAQ.DataSet dataSet = GetUserInputDataSet();

				switch (dataSet) {
				case DBLP:
					DBLP.Run_Graphsize_vs_Runtime (mStart, mEndDBLP, numQuery, K, MaxEdges);
					break;
				case DBLPAlternate:
					DBLPAlternate.Run_Graphsize_vs_Runtime (mStart, mEndDBLP, numQuery, K, MaxEdges);
					break;
				case exit:
					condition = false;
					break;
				case Pokec:
					Pokec.Run_Graphsize_vs_Runtime (mStart, mEndDBLP, numQuery, K, MaxEdges);
					break;
				case IMDB:
					IMDB.Run_Graphsize_vs_Runtime (mStart, mEndDBLP, numQuery, K, MaxEdges);
					break;
				}
			}
		}
	}

	public static void PrintResults() {
		System.out.println("********************************************************");
		System.out.println("Displaying results : ");

		boolean condition = true;
		while (condition) {
			AllExperiments input = GetUserInput();
			
			switch (input) {
			case Exit:
				condition = false;
				break;
			default:
				PrintResultsHelper (input);
				break;
			}
		}		
	}

	private static void PrintResultsHelper(AllExperiments experiment) {
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			
			switch (dataSet) {
			case DBLP:
				DBLP.PrintResults (experiment);
				break;
			case DBLPAlternate:
				DBLPAlternate.PrintResults (experiment);
				break;
			case exit:
				condition = false;
				break;
			case Pokec:
				Pokec.PrintResults (experiment);
				break;
			case IMDB:
				IMDB.PrintResults (experiment);
				break;
			}
		}				
	}

	private static AllExperiments GetUserInput() {
		//display message
		for (AllExperiments experiments : AllExperiments.values()) {
			System.out.println(experiments+experiments.Padding()+" : "+experiments.ordinal());
		}
		//get the input
		System.out.print("\nWhich experiment do you want to consider ? ");
		int input;
		
		try {
			input = reader.nextInt();
		} catch (Exception e) {
			reader.next();
			input = AllExperiments.Exit.ordinal();
		}
		
		//convert int to AllExperiments
		AllExperiments ret = AllExperiments.ConvertIntToAllExperiments(input); 
		return ret;
	}

	private static AllExperimentsSigmod GetUserInputSigmod() {
		System.out.println("List of experiments");
		System.out.println();
		//display message
		for (AllExperimentsSigmod experiments : AllExperimentsSigmod.values()) {
			System.out.println(experiments.ordinal()+" : "+experiments.Message());
		}
		
		//get the input
		System.out.print("\nWhich experiment do you want to consider ? ");
		int input;
		
		try {
			input = reader.nextInt();
		} catch (Exception e) {
			reader.next();
			input = AllExperimentsSigmod.Exit.ordinal();
		}
		
		//convert int to AllExperiments
		AllExperimentsSigmod ret = AllExperimentsSigmod.ConvertIntToAllExperiments(input); 
		return ret;
	}

	private static RAQ.DataSet GetUserInputDataSet () {
		for (RAQ.DataSet dataSet : RAQ.DataSet.values()) {
			System.out.println(dataSet+dataSet.Padding()+" : "+dataSet.ordinal());
		}
		//get the input
		System.out.print("\nWe are running "+AllExperiments.PreviousConversion()+ " : Which dataset do you want? ");
		int input;
		
		try {
			input = reader.nextInt();
		} catch (Exception e) {
			reader.next();
			input = RAQ.DataSet.exit.ordinal();
		}
		
		//convert int to AllExperiments
		return RAQ.DataSet.ConvertIntToDataSet(input);
	}

	/**
	 * Function to close the open file
	 */
	public static boolean Conclude() {
		boolean ret = false;
		System.out.print("Enter 0 to exit : ");
		int input;
		
		try {
			input = reader.nextInt();
		} catch (Exception e) {
			reader.next();
			input = -1;
		}
		
		if (input == 0) {
			reader.close();
			ret = true;
		}
		
		return ret;
	}

	
	/**
	 * Generate all the random input graphs
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	
	public static void GenerateRandomGraphs()
			throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		int setSize = 100;
		int listSize= 12;
		
		System.out.println("Generate the random graphs");
		boolean condition = true;
		while (condition) {
			RAQ.DataSet dataSet = GetUserInputDataSet();
			double frac = GetFraction();

			switch (dataSet) {
			case DBLP:
				DBLP.GenerateRandomGraphs(setSize, listSize, frac);
				break;
			case DBLPAlternate:
				DBLPAlternate.GenerateRandomGraphs(setSize, listSize, frac);
				break;
			case exit:
				condition = false;
				break;
			case Pokec:
				Pokec.GenerateRandomGraphs(setSize, listSize, frac);
				break;
			case IMDB:
				IMDB.GenerateRandomGraphs(setSize, listSize);
				break;
			}
		}
	}

	
	public static void PrintStat() throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		//print the number of edges
		DBLPAlternate.PrintNumOfEdges();
	}
	
	private static void Run_Graphsize_vs_RuntimeSigmod() throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
		boolean condition = true;
		boolean avoidTAHeuristics = true;

		while (condition) {
			RAQ.DataSetSigmod dataSet = GetUserInputDataSetSigmod();
			boolean printResults = false;
			boolean considerUniformWeight = false;
			
			switch (dataSet) {
			case DBLP:
				DBLP.Run_Graphsize_vs_Runtime (mStart, mEndDBLP, numQuery, K, MaxEdges,
						printResults, avoidTAHeuristics, considerUniformWeight);
				break;
			case coAuthor:
				DBLPAlternate.Run_Graphsize_vs_Runtime (mStart, mEndDBLP, numQuery, K, MaxEdges,
						printResults, avoidTAHeuristics, considerUniformWeight);
				break;
				/*
			case NorthEast:
				NorthEast.Run_Graphsize_vs_Runtime (mStart, mEndDBLP, numQuery, K, MaxEdges,
						printResults, considerUniformWeight);
				break;
				*/
			case exit:
				condition = false;
				break;
			case Pokec:
				Pokec.Run_Graphsize_vs_Runtime (mStart, mEndDBLP, numQuery, K, MaxEdges,
						printResults, considerUniformWeight);
				break;
			}
		}		
	}

	private static RAQ.DataSetSigmod GetUserInputDataSetSigmod() {
		for (RAQ.DataSetSigmod dataSet : RAQ.DataSetSigmod.values()) {
			System.out.println(dataSet.ordinal()+" : "+dataSet);
		}
		//get the input
		//System.out.print("\nWe are running "+AllExperimentsSigmod.PreviousConversion()+ " : Which dataset do you want? ");
		System.out.print ("Which dataset do you want? ");
		int input;
		
		try {
			input = reader.nextInt();
		} catch (Exception e) {
			reader.next();
			input = RAQ.DataSetSigmod.exit.ordinal();
		}
		
		//convert int to AllExperiments
		return RAQ.DataSetSigmod.ConvertIntToDataSet(input);
	}
}
