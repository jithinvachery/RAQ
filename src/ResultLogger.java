import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ResultLogger {	
	public static class WeVsRest {
		final String fName;
		ArrayList<Result> resultsFile = new ArrayList<>();
		ArrayList<Result> resultsDisplay;
		TreeMap<Integer, ResultAggregate> resultMap = new TreeMap<>();

		RAQ.IndexingScheme indexingScheme;
		
		public WeVsRest (String fname, RAQ.IndexingScheme indexingScheme) {
			fName = fname;
			this.indexingScheme = indexingScheme;
		}
		
		private class ResultAggregate {
			ArrayList<Integer> numResults;
			ArrayList<Integer> numNodes;
			ArrayList<Long> elapsedTime;
			
			//other technique
			ArrayList<Integer> numResultsBC;
			//ArrayList<Integer> numNodesBC;
			ArrayList<Long> elapsedTimeBC;

			public ResultAggregate(ArrayList<Integer> numNodes,
					ArrayList<Long> elapsedTime,
					ArrayList<Integer> numResults,
					ArrayList<Integer> numNodesBC,
					ArrayList<Long> elapsedTimeBC,
					ArrayList<Integer> numResultsBC) {
				this.numNodes 	= numNodes;
				this.elapsedTime= elapsedTime;
				this.numResults = numResults;
				
				//other technique
				//this.numNodesBC 	= numNodesBC;
				this.elapsedTimeBC	= elapsedTimeBC;
				this.numResultsBC 	= numResultsBC;
			}
		}

		/**
		 * temporarily store the result, so that we can write all the results after all experiments done 
		 * @param numNodes
		 * @param numEdges
		 * @param indexingScheme
		 * @param k
		 * @param elapsedTime
		 * @param numResults 
		 * @param para
		 */
		public void Log(int numNodes, int numEdges, RAQ.IndexingScheme indexingScheme,
				int k, long elapsedTime, int numResults, int para) {
			resultsFile.add(new Result(numNodes, numEdges, indexingScheme,
					k, elapsedTime, numResults, para));
		}

		/**
		 * Add the result to the file.
		 * We call it after the experiment is over
		 * @throws IOException 
		 */
		public void Flush() throws IOException {
			DumpToFile (fName, resultsFile);
			resultsFile.clear();
		}

		public void Clear() {
			resultsFile.clear();
		}

		public void PrintAllResults() {
			resultsDisplay = GetAllResults(fName+".json");

			//all the results are in results arraylist
			Collections.sort(resultsDisplay, new ResultComparatorNumEdges());

			Iterator<Result> it = resultsDisplay.iterator();
			if (it.hasNext()) {
				//first result
				Result result = it.next();
				int numEdges  = result.numEdges;
				ArrayList<Integer> numNodes 	= new ArrayList<>();
				ArrayList<Integer> numResults 	= new ArrayList<>();
				ArrayList<Long> elapsedTime 	= new ArrayList<>();

				//other technique
				ArrayList<Integer> numNodesBC 	= new ArrayList<>();
				ArrayList<Integer> numResultsBC	= new ArrayList<>();
				ArrayList<Long> elapsedTimeBC 	= new ArrayList<>();

				if (result.indexingScheme == indexingScheme)
					AddStatToList(result, numNodesBC, elapsedTimeBC, numResultsBC);
				else
					AddStatToList(result, numNodes, elapsedTime, numResults);

				while (it.hasNext()) {
					result = it.next();
					if (result.numEdges != numEdges) {
						resultMap.put  (numEdges, new ResultAggregate(numNodes, elapsedTime,
								numResults,numNodesBC, elapsedTimeBC, numResultsBC));
						numEdges  	= result.numEdges;
						numNodes 	= new ArrayList<>();
						numResults 	= new ArrayList<>();
						elapsedTime = new ArrayList<>();
						
						//other technique
						numNodesBC 	 = new ArrayList<>();
						numResultsBC = new ArrayList<>();
						elapsedTimeBC= new ArrayList<>();
					}
					if (result.indexingScheme == indexingScheme)
						AddStatToList(result, numNodesBC, elapsedTimeBC, numResultsBC);
					else
						AddStatToList(result, numNodes, elapsedTime, numResults);
				}
				resultMap.put  (numEdges, new ResultAggregate(numNodes, elapsedTime, 
						numResults, numNodesBC, elapsedTimeBC, numResultsBC));
				PrintAllResultsHelper();
			}
		}
		
		private void PrintAllResultsHelper() {
			
			System.out.println(fName+",");
			System.out.println(",\n,");
			System.out.println("Number of Edges,,Elapsed Time (Milli Sec),,");
			System.out.print  (",,Our Technique,,,");
			System.out.println(",,"+indexingScheme.toString()+",,,");
			System.out.print  (",Mean,Mean(Stable),Median,Std Dev, # experiments, #>"+CGQTimeOut.shortDelay);
			System.out.println(",Mean,Mean(Stable),Median,Std Dev, # experiments, #>"+CGQTimeOut.shortDelay);
			for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
				ArrayList<Long> arr = entry.getValue().elapsedTime;
				System.out.print  (entry.getKey()+","+Helper.Mean(arr)+","+
						Helper.MeanStable(arr, -1)+","+
						Helper.Median(arr)+","+Helper.StandardDeviation(arr)+","+
						arr.size()+","+Helper.rightTail(arr, CGQTimeOut.shortDelay));
				
				//basecase
				arr = entry.getValue().elapsedTimeBC;
				System.out.println(","+Helper.Mean(arr)+","+
						Helper.MeanStable(arr, -1)+","+
						Helper.Median(arr)+","+Helper.StandardDeviation(arr)+","+
						arr.size()+","+Helper.rightTail(arr, CGQTimeOut.shortDelay));
			}

			System.out.println(",\n,");
			System.out.println("Number of Edges,,Number of Nodes,,");
			System.out.println(",Mean,Median,Std Dev,");
			for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
				ArrayList<Integer> arr = entry.getValue().numNodes;
				System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
						Helper.Median(arr)+","+Helper.StandardDeviation(arr)+","+
						arr.size()+",");
			}
			
			System.out.println(",\n,");
			System.out.println("Number of Edges,,Number of Results,,");
			System.out.print  (",,Our Technique,,,");
			System.out.println(",,Base Case,,,");
			System.out.print  (",Mean,Median,Std Dev,");
			System.out.println(",Mean,Median,Std Dev,");
			for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
				ArrayList<Integer> arr = entry.getValue().numResults;
				System.out.print  (entry.getKey()+","+Helper.Mean(arr)+","+
						Helper.Median(arr)+","+Helper.StandardDeviation(arr)+",");
				
				//basecase
				arr = entry.getValue().numResultsBC;
				System.out.println(","+Helper.Mean(arr)+","+
						Helper.Median(arr)+","+Helper.StandardDeviation(arr)+",");
			}
		}

		
		private void AddStatToList(Result result, ArrayList<Integer> numEdges,
				ArrayList<Long> elapsedTime,
				ArrayList<Integer> numResults) {
			numEdges.add	(result.numEdges);
			numResults.add	(result.numResults);
			elapsedTime.add	(result.elapsedTime);
		}
	}
	
	static private class KeyValue {
		String key;
		String value;

		public KeyValue(String key, String value) {
			this.key 	= key;
			this.value 	= value;
		}
	}

	static class Result {
		Integer numNodes;
		Integer numEdges;
		Integer numResults;
		Integer k;
		RAQ.IndexingScheme indexingScheme;
		Long elapsedTime;
		Integer para; // different parameters

		public Result(int numNodes, int numEdges, 
				RAQ.IndexingScheme indexingScheme,
				int k, long elapsedTime,
				int numResults, int para) {
			this.numNodes 		= numNodes;
			this.numEdges 		= numEdges;
			this.indexingScheme = indexingScheme;
			this.k 				= k;
			this.elapsedTime 	= elapsedTime;
			this.numResults 	= numResults;
			this.para 			= para;
		}

		public Result(JSONObject jo) {
			numNodes 	= Integer.parseInt((String)jo.get("numNodes"));
			numEdges 	= Integer.parseInt((String)jo.get("numEdges"));
			numResults  = Integer.parseInt((String)jo.get("numResults"));
			indexingScheme = RAQ.IndexingScheme.valueOf((String)jo.get("indexingScheme"));
			k 			= Integer.parseInt((String)jo.get("k"));
			elapsedTime = Long.parseLong((String)jo.get("elapsedTime"));
			para  		= Integer.parseInt((String)jo.get("para"));
		}

		public ArrayList<KeyValue> GetKeyValuePairs() {
			ArrayList<KeyValue> ret = new ArrayList<>();

			ret.add(new KeyValue("numNodes",numNodes.toString()));
			ret.add(new KeyValue("numEdges",numEdges.toString()));
			ret.add(new KeyValue("numResults",numResults.toString()));
			ret.add(new KeyValue("indexingScheme",indexingScheme.toString()));
			ret.add(new KeyValue("k",k.toString()));
			ret.add(new KeyValue("elapsedTime",elapsedTime.toString()));
			ret.add(new KeyValue("para",para.toString()));

			return ret;
		}
	}
	
	private static class ResultComparatorNumEdges implements Comparator<Result> {
		@Override
		public int compare(Result o1, Result o2) {
			return Integer.compare(o1.numEdges, o2.numEdges);
		}
	}

	
	static public class Para_vs_Runtime {
		final String fName;
		final Experiments.AllExperimentsParameters parameter;
		ArrayList<Result> results = new ArrayList<>();
		TreeMap<Integer, ResultAggregate> resultMap = new TreeMap<>();

		private class ResultComparator implements Comparator<Result> {
			@Override
			public int compare(Result o1, Result o2) {
				return Integer.compare(o1.para, o2.para);
			}
		}
		
		private class ResultAggregate {
			ArrayList<Integer> numResults;
			ArrayList<Integer> numEdges;
			ArrayList<Integer> numNodes;
			ArrayList<Long> elapsedTime;

			public ResultAggregate(ArrayList<Integer> numEdges,
					ArrayList<Integer> numNodes,
					ArrayList<Long> elapsedTime,
					ArrayList<Integer> numResults) {
				this.numEdges 	= numEdges;
				this.numNodes 	= numNodes;
				this.elapsedTime= elapsedTime;
				this.numResults = numResults;
			}
		}
		
		public Para_vs_Runtime (String fname, Experiments.AllExperimentsParameters parameter) {
			fName = fname;
			this.parameter = parameter;
		}

		/**
		 * temporarily store the result, so that we can write all the results after all experiments done 
		 * @param numNodes
		 * @param numEdges
		 * @param indexingScheme
		 * @param k
		 * @param elapsedTime
		 * @param numResults 
		 * @param para
		 */
		public void Log(int numNodes, int numEdges, RAQ.IndexingScheme indexingScheme,
				int k, long elapsedTime, int numResults, int para) {
			results.add(new Result(numNodes, numEdges, indexingScheme,
					k, elapsedTime, numResults, para));
		}
		
		/**
		 * Add the result to the file.
		 * We call it after the experiment is over
		 * @throws IOException 
		 */
		public void Flush() throws IOException {
			DumpToFile (fName, results);
			results.clear();
		}

		public void Clear() {
			results.clear();
		}

		public void PrintAllResults() {
			boolean minimal = false;
			PrintAllResults (minimal);
		}
		public void PrintAllResults(boolean minimal) {
			results = GetAllResults(fName+".json");

			//all the results are in results arraylist
			Collections.sort(results, new ResultComparator());

			Iterator<Result> it = results.iterator();
			if (it.hasNext()) {
				//first result
				Result result = it.next();
				int para = result.para;
				ArrayList<Integer> numEdges 	= new ArrayList<>();
				ArrayList<Integer> numResults 	= new ArrayList<>();
				ArrayList<Integer> numNodes		= new ArrayList<>();
				ArrayList<Long> elapsedTime 	= new ArrayList<>();

				AddStatToList(result, numEdges, numNodes, elapsedTime, numResults);

				while (it.hasNext()) {
					result = it.next();
					if (result.para != para) {
						resultMap.put(para, new ResultAggregate(numEdges, numNodes, elapsedTime, numResults));
						para  		= result.para;
						numEdges 	= new ArrayList<>();
						numResults 	= new ArrayList<>();
						numNodes 	= new ArrayList<>();
						elapsedTime = new ArrayList<>();
						AddStatToList(result, numEdges, numNodes, elapsedTime, numResults);
					} else {
						AddStatToList(result, numEdges, numNodes, elapsedTime, numResults);
					}
				}
				resultMap.put(para, new ResultAggregate(numEdges, numNodes, elapsedTime, numResults));
				PrintAllResultsHelper(minimal);
			}
		}
		
		private void PrintAllResultsHelper(boolean minimal) {
			int numExp=-1;
			for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
				ArrayList<Long> arr = entry.getValue().elapsedTime;
				numExp = arr.size();
				break;
			}
			
			String paraId = null;
			
			switch (parameter) {
			case BeamWidth:
				paraId = "Beam Width";
				break;
			case K_vs_Runtime:
			case K_vs_Runtime_Uniform:
				paraId = "K";
				break;
			case WidthThreshold:
				paraId = "Width Threshold";
				break;
			case BranchingFactor:
				paraId = "Branching Factor";
				break;			
			}
			
			if (minimal) {
				System.out.println(",\n,");
				System.out.println(paraId+",,Elapsed Time (Milli Sec),,");
				System.out.println(",Mean,Mean(Stable),");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Long> arr = entry.getValue().elapsedTime;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.MeanStable(arr, -1)+",");
				}

				System.out.println(",\n,");
	
			} else {
				System.out.println(fName+",");
				System.out.println("Number of experiments,"+numExp+",");
				System.out.println(",\n,");
				System.out.println(paraId+",,Elapsed Time (Milli Sec),,");
				System.out.println(",Mean,Mean(Stable),Median,Std Dev,");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Long> arr = entry.getValue().elapsedTime;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.MeanStable(arr, 30)+","+
							Helper.Median(arr)+","+Helper.StandardDeviation(arr)+",");
				}

				System.out.println(",\n,");
				System.out.println(paraId+",,Number of Edges,,");
				System.out.println(",Mean,Median,Std Dev,");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Integer> arr = entry.getValue().numEdges;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.Median(arr)+","+Helper.StandardDeviation(arr)+",");
				}

				System.out.println(",\n,");
				System.out.println(paraId+",,Number of Results,,");
				System.out.println(",Mean,Median,Std Dev,");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Integer> arr = entry.getValue().numResults;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.Median(arr)+","+Helper.StandardDeviation(arr)+",");
				}

				System.out.println(",\n,");
				System.out.println(paraId+",,Number of Nodes,,");
				System.out.println(",Mean,Median,Std Dev,");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Integer> arr = entry.getValue().numNodes;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.Median(arr)+","+Helper.StandardDeviation(arr)+",");
				}
			}
		}

		private void AddStatToList(Result result, ArrayList<Integer> numEdges,
				ArrayList<Integer> k,
				ArrayList<Long> elapsedTime,
				ArrayList<Integer> numResults) {
			numEdges.add	(result.numEdges);
			numResults.add	(result.numResults);
			k.add			(result.k);
			elapsedTime.add	(result.elapsedTime);
		}

	}

	static public class Graphsize_vs_Runtime{
		final String fName;
		ArrayList<Result> resultsFile 		= new ArrayList<>();
		ArrayList<Result> resultsDisplay 	= new ArrayList<>();
		TreeMap<Integer, ResultAggregate> resultMap = new TreeMap<>();
				
		private class ResultAggregate {
			ArrayList<Integer> numResults;
			ArrayList<Integer> numNodes;
			ArrayList<Integer> k;
			ArrayList<Long> elapsedTime;

			public ResultAggregate(ArrayList<Integer> numNodes,
					ArrayList<Integer> k,
					ArrayList<Long> elapsedTime,
					ArrayList<Integer> numResults) {
				this.numNodes 	= numNodes;
				this.k 			= k;
				this.elapsedTime= elapsedTime;
				this.numResults = numResults;
			}
		}
		
		public Graphsize_vs_Runtime(String fname) {
			fName = fname;
		}

		/**
		 * temporarily store the result, so that we can write all the results after all experiments done 
		 * @param numNodes
		 * @param numEdges
		 * @param indexingScheme
		 * @param k
		 * @param elapsedTime
		 * @param numResults 
		 * @param para 
		 */
		public void Log(int numNodes, int numEdges, RAQ.IndexingScheme indexingScheme,
				int k, long elapsedTime, int numResults, int para) {
			resultsFile.add(new Result(numNodes, numEdges, indexingScheme,
					k, elapsedTime, numResults, para));
		}

		/**
		 * Add the result to the file.
		 * We call it after the experiment is over
		 * @throws IOException 
		 */
		public void Flush() throws IOException {
			DumpToFile (fName, resultsFile);
			resultsFile.clear();
		}

		public void Clear() {
			resultsFile.clear();
		}
		
		public void PrintAllResults(int numExp, boolean minimal) {
			resultsDisplay = GetAllResults(fName+".json");

			//all the results are in results arraylist
			Collections.sort(resultsDisplay, new ResultComparatorNumEdges());

			Iterator<Result> it = resultsDisplay.iterator();
			if (it.hasNext()) {
				//first result
				Result result = it.next();
				int numEdges  = result.numEdges;
				ArrayList<Integer> numNodes 	= new ArrayList<>();
				ArrayList<Integer> numResults 	= new ArrayList<>();
				ArrayList<Integer> k 			= new ArrayList<>();
				ArrayList<Long> elapsedTime 	= new ArrayList<>();

				AddStatToList(result, numNodes, k, elapsedTime, numResults);

				while (it.hasNext()) {
					result = it.next();
					if (result.numEdges != numEdges) {
						resultMap.put(numEdges, new ResultAggregate(numNodes, k, elapsedTime, numResults));
						numEdges  	= result.numEdges;
						numNodes 	= new ArrayList<>();
						numResults 	= new ArrayList<>();
						k 			= new ArrayList<>();
						elapsedTime = new ArrayList<>();
						AddStatToList(result, numNodes, k, elapsedTime, numResults);
					} else {
						AddStatToList(result, numNodes, k, elapsedTime, numResults);
					}
				}
				resultMap.put(numEdges, new ResultAggregate(numNodes, k, elapsedTime, numResults));
				PrintAllResultsHelper(numEdges, minimal);
			}
		}

		private void PrintAllResultsHelper(int numEdges, boolean minimal) {
			/*
			System.out.println("numNodes : "+ numNodes);
			System.out.println("numEdges : "+ Helper.Mean(numEdges)+
					" : "+Helper.Median(numEdges)+" : "+Helper.StandardDeviation(numEdges));
			System.out.println(paraId+" : "+ Helper.Mean(k)+
					" : "+Helper.Median(k)+" : "+Helper.StandardDeviation(k));
			System.out.println("numResults : "+ Helper.Mean(numResults)+
					" : "+Helper.Median(numResults)+" : "+Helper.StandardDeviation(numResults));
			System.out.println("elapsedTime : "+ Helper.Mean(elapsedTime)+
					" : "+Helper.Median(elapsedTime)+" : "+Helper.StandardDeviation(elapsedTime));
			 */
			if (minimal) {
				System.out.println(",\n,");
				System.out.println("Query graph size,,Elapsed Time (Milli Sec),,");
				System.out.println(",Mean(Stable),Median, # experiments,");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Long> arr = entry.getValue().elapsedTime;
					System.out.println(entry.getKey()+","+
							Helper.MeanStable(arr, numEdges)+","+
							Helper.Median(arr)+","+","+
							arr.size()+"");
				}				
			} else {
				System.out.println(fName+",");
				System.out.println(",\n,");
				System.out.println("Number of Edges,,Elapsed Time (Milli Sec),,");
				System.out.println(",Mean,Mean(Stable),Median,Std Dev, # experiments, #>"+CGQTimeOut.shortDelay);
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Long> arr = entry.getValue().elapsedTime;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.MeanStable(arr, numEdges)+","+
							Helper.Median(arr)+","+Helper.StandardDeviation(arr)+","+
							arr.size()+","+Helper.rightTail(arr, CGQTimeOut.shortDelay));
				}

				System.out.println(",\n,");
				System.out.println("Number of Edges,,Number of Nodes,,");
				System.out.println(",Mean,Median,Std Dev,");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Integer> arr = entry.getValue().numNodes;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.Median(arr)+","+Helper.StandardDeviation(arr)+","+
							arr.size()+",");
				}

				System.out.println(",\n,");
				System.out.println("Number of Edges,,Number of Results,,");
				System.out.println(",Mean,Median,Std Dev,");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Integer> arr = entry.getValue().numResults;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.Median(arr)+","+Helper.StandardDeviation(arr)+","+
							arr.size()+",");
				}

				System.out.println(",\n,");
				System.out.println("Number of Edges,,K,,");
				System.out.println(",Mean,Median,Std Dev,");
				for (Map.Entry<Integer, ResultAggregate> entry : resultMap.entrySet()) {
					ArrayList<Integer> arr = entry.getValue().k;
					System.out.println(entry.getKey()+","+Helper.Mean(arr)+","+
							Helper.Median(arr)+","+Helper.StandardDeviation(arr)+","+
							arr.size()+",");
				}
			}
		}

		private void AddStatToList(Result result, ArrayList<Integer> numNodes,
				ArrayList<Integer> k,
				ArrayList<Long> elapsedTime,
				ArrayList<Integer> numResults) {
			numNodes.add	(result.numNodes);
			numResults.add	(result.numResults);
			if (result.k == Experiments.K) {
				//we had initially use a random K so we are removing it
				k.add			(result.k);
			}
			elapsedTime.add	(result.elapsedTime);
		}
	}

	@SuppressWarnings("unchecked")
	static void LogResultAll (Integer graphSizeNode,
			Integer graphSizeEdge,
			ArrayList<Long> runTime,
			boolean multiThreaded,
			ArrayList<Integer> 	depth,
			ArrayList<Long> 	numDFS,
			ArrayList<Long> 	numEdgesTouched,
			ArrayList<String> 	indexingScheme,
			int K,
			ArrayList<Integer> 	numResultsFound,
			ArrayList<Double> 	avgDistance,
			String fName) throws IOException {
		JSONObject jo = new JSONObject();

		jo.put("QueryGraphSizeNode", 	graphSizeNode);
		jo.put("QueryGraphSizeEdge", 	graphSizeEdge);
		jo.put("MultithreadingEnabled", multiThreaded);
		jo.put("Runtime", 	runTime);
		jo.put("Depth", 	depth);
		jo.put("NumDFS", 	numDFS);
		jo.put("NumEdgesTouched", 	numEdgesTouched);
		jo.put("Indexing", 			indexingScheme);
		jo.put("K", K);
		jo.put("NumResultsFound", 	numResultsFound);
		jo.put("AvgDistance", 		avgDistance);

		LogResultHelper (jo, fName+K+".json");
	}

	/**
	 * Dump the results into file
	 * @param fName
	 * @param results
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	static void DumpToFile(String fName, ArrayList<Result> results) throws IOException {
		for (Result result : results) {
			JSONObject jo = new JSONObject();
			for (KeyValue keyValue : result.GetKeyValuePairs()) {
				jo.put(keyValue.key, keyValue.value);
			}
			LogResultHelper (jo, fName+".json");
		}
	}


	@SuppressWarnings("unchecked")
	static void LogResult (Integer graphSizeNode,
			Integer graphSizeEdge,
			Long runTime,
			boolean multiThreaded,
			Integer depth,
			Long numDFS,
			Long numEdgesTouched,
			RAQ.IndexingScheme indexingScheme,
			int K,
			Integer numResultsFound,
			Double avgDistance,
			String fName) throws IOException {
		JSONObject jo = new JSONObject();

		jo.put("QueryGraphSizeNode", graphSizeNode);
		jo.put("QueryGraphSizeEdge", graphSizeEdge);
		jo.put("MultithreadingEnabled", multiThreaded);
		jo.put("Runtime", runTime);
		jo.put("Depth", depth);
		jo.put("NumDFS", numDFS);
		jo.put("NumEdgesTouched", numEdgesTouched);
		jo.put("Indexing", indexingScheme);
		jo.put("K", K);
		jo.put("NumResultsFound", numResultsFound);
		jo.put("AvgDistance", avgDistance);

		LogResultHelper (jo, fName+K+".json");
	}

	static void LogResultHelper (JSONObject jo, String fname) throws IOException {
		FileOutputStream outLock = Lock ();
		
		StringWriter out = new StringWriter();
		try {
			jo.writeJSONString(out);
		} catch (IOException e) {
			e.printStackTrace();
		}

		String dataString = out.toString()+"\n";
		byte data[] = dataString.getBytes();

		try (OutputStream buffer = new BufferedOutputStream(
				Files.newOutputStream(Paths.get(fname), StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
			buffer.write(data, 0, data.length);
			buffer.close();
		} catch (IOException x) {
			System.err.println(x);
		}
		
		UnLock (outLock);
	}

	/**
	 * we want to prevent multiple simultaneous modifications to the results file
	 * lets lock the file
	 * @return 
	 * @throws IOException 
	 */
	private static FileOutputStream Lock() throws IOException {
		String lockFname = getLockFname ();
		
		FileOutputStream out=null;
		try {
			out = new FileOutputStream(lockFname);
		} catch (FileNotFoundException e1) {
			//if the file is not found we shall create it
			File f = new File(lockFname);
			
			try {
				f.createNewFile();
				out = new FileOutputStream(lockFname);
			} catch (IOException e) {
				System.err.println("unable to create file "+lockFname);
				e.printStackTrace();
			}
		}

		out.getChannel().lock();
		
		return out;
	}

	private static String getLockFname() {
		return "../results/.lock";
	}

	private static void UnLock(FileOutputStream outLock) throws IOException {
		outLock.close();
	}

	static private class ResultAll  extends Helper{
		//mapping indexing scheme to result
		private HashMap<RAQ.IndexingScheme, Result_V0> map = new HashMap<>();

		void add (	ArrayList<RAQ.IndexingScheme> indexingScheme,	ArrayList<Long> runtime,
				ArrayList<Long> numResultsFound,ArrayList<Double> avgDistance,
				ArrayList<Long> depth, 			ArrayList<Long> numDFS,
				Long querySize, 				ArrayList<Long> numEdgesTouched) {

			for (int i=0; i<indexingScheme.size(); i++) {
				RAQ.IndexingScheme index = indexingScheme.get(i);

				Result_V0 result = map.get(index);
				if (result == null) {
					result = new Result_V0();
					map.put(index, result);
				}

				Long num_edges_touched = null, num_DFS = null;
				if (numEdgesTouched != null) {
					num_edges_touched 	= numEdgesTouched.get(i);
					num_DFS				= numDFS.get(i);
				}

				result.add(runtime.get(i), numResultsFound.get(i),
						avgDistance.get(i), depth.get(i), num_DFS,
						querySize, num_edges_touched);

			}
		}

		void printDisplay (double top, Long size, boolean useEdge) {
			//mean media sd for each indexing scheme
			ArrayList<Double> depth 			= new ArrayList<>();
			ArrayList<Double> numDFS			= new ArrayList<>();
			ArrayList<Double> numEdgesTouched	= new ArrayList<>();
			ArrayList<Double> runtime			= new ArrayList<>();
			ArrayList<Double> numResultsFound	= new ArrayList<>();
			ArrayList<Double> avgDistance 		= new ArrayList<>();
			ArrayList<Double> querySize			= new ArrayList<>();

			ArrayList<RAQ.IndexingScheme> indexing = new ArrayList<>();

			int num=-1;

			for (Map.Entry<RAQ.IndexingScheme, Result_V0> pair : map.entrySet()) {
				//key : indexing scheme
				RAQ.IndexingScheme key = pair.getKey();
				indexing.add(key);

				Result_V0 result = pair.getValue();

				num = (int) ((double)result.num*top);

				printAddHelper(top, depth, 			result.depth);
				printAddHelper(top, numDFS,			result.numDFS);
				printAddHelper(top, numEdgesTouched,result.numEdgesTouched);
				printAddHelper(top, runtime,		result.runtime);
				printAddHelper(top, numResultsFound,result.numResultsFound);
				printAddHelper(top, avgDistance, 	result.avgDistance);
				printAddHelper(top, querySize, 		result.querySize);
			}

			int width   	= 10;
			int numFields 	= 3;

			int l = ((width+1)*numFields*indexing.size())+numFields;
			String star = "";
			for (int i=0; i<l+2; i++)
				star += "*";

			String space = "";
			for (int i=0; i<l-width; i++)
				space += " ";
			space += " *";

			star+="******************************";
			System.out.println(star);

			if (useEdge)
				System.out.println("* Query Graph size Edge     : "+FormatNumber(size, width)+space);
			else
				System.out.println("* Query Graph size node     : "+FormatNumber(size, width)+space);

			System.out.println("* Number of experiments     : "+ FormatNumber(num, width)+space);

			String message = "* Order of results ";
			for (RAQ.IndexingScheme s:indexing) {
				message += "- ";
				message += s;
			}
			int temp = star.length()-message.length()-1;
			for (int i=0; i<temp; i++)
				message += " ";
			message += "*";

			System.out.println(message);
			System.out.println(star);
			System.out.println("*                             "+ printMeanEtc(l));

			if (useEdge)
				System.out.println("* Query Graph size Node     : "+ printMeanEtc(querySize,		width, indexing, numFields));
			else
				System.out.println("* Query Graph size Edge     : "+ printMeanEtc(querySize,		width, indexing, numFields));

			System.out.println("* Depth  (Number of DFS)    : "+ printMeanEtc(depth,			width, indexing, numFields));
			System.out.println("* Number of edges touched   : "+ printMeanEtc(numEdgesTouched,	width, indexing, numFields));
			System.out.println("* Number of DFS				: "+ printMeanEtc(numDFS,			width, indexing, numFields));
			System.out.println("* Runtime (Milli Sec)       : "+ printMeanEtc(runtime,			width, indexing, numFields));
			System.out.println("* Runtime (Sec)             : "+ printMeanEtc1000(runtime,			width, indexing, numFields));
			System.out.println("* Num of results            : "+ printMeanEtc(numResultsFound,	width, indexing, numFields));
			System.out.println("* Avg Distance              : "+ printMeanEtc(avgDistance,		width, indexing, numFields));
			System.out.println(star);
		}
		void printCSV (double top, Long size, boolean useEdge) {
			//mean media sd for each indexing scheme
			ArrayList<Double> depth 			= new ArrayList<>();
			ArrayList<Double> numDFS			= new ArrayList<>();
			ArrayList<Double> numEdgesTouched	= new ArrayList<>();
			ArrayList<Double> runtime			= new ArrayList<>();
			ArrayList<Double> numResultsFound	= new ArrayList<>();
			ArrayList<Double> avgDistance 		= new ArrayList<>();
			ArrayList<Double> querySize			= new ArrayList<>();

			ArrayList<RAQ.IndexingScheme> indexing = new ArrayList<>();

			int num=-1;
			int numFields = 3;

			for (Map.Entry<RAQ.IndexingScheme, Result_V0> pair : map.entrySet()) {
				//key : indexing scheme
				RAQ.IndexingScheme key = pair.getKey();
				indexing.add(key);

				Result_V0 result = pair.getValue();

				num = (int) ((double)result.num*top);

				printAddHelper(top, depth, 			result.depth);
				printAddHelper(top, numDFS,			result.numDFS);
				printAddHelper(top, numEdgesTouched,result.numEdgesTouched);
				printAddHelper(top, runtime,		result.runtime);
				printAddHelper(top, numResultsFound,result.numResultsFound);
				printAddHelper(top, avgDistance, 	result.avgDistance);
				printAddHelper(top, querySize, 		result.querySize);
			}

			String star = ",";

			System.out.println(star);

			int width=5;
			if (useEdge)
				System.out.println("Query Graph size Edge     , "+FormatNumber(size, width)+",");
			else
				System.out.println("Query Graph size node     , "+FormatNumber(size, width)+",");

			System.out.println("Number of experiments     , "+ FormatNumber(num, width)+",");
			System.out.println(",");

			String experiments = "";
			for (RAQ.IndexingScheme s:indexing) {
				experiments += s+",";
			}
			int numExperiments = indexing.size();

			System.out.println(printMeanEtcCSV(numExperiments));
			System.out.print("Technique,");
			for (int i=0; i<numFields; i++) {
				System.out.print(experiments+",");
			}
			System.out.println();

			if (useEdge)
				System.out.println("Query Graph size Node     ,"+ printMeanEtcCSV(querySize,width, indexing, numFields));
			else
				System.out.println("Query Graph size Edge     ,"+ printMeanEtcCSV(querySize,width, indexing, numFields));

			System.out.println("Depth  (Number of DFS)    ,"+ printMeanEtcCSV(depth,width, indexing, numFields));
			System.out.println("Number of DFS             ,"+ printMeanEtcCSV(numDFS,width, indexing, numFields));
			System.out.println("Number of edges touched   ,"+ printMeanEtcCSV(numEdgesTouched,width, indexing, numFields));
			System.out.println("Runtime (Milli Sec)       ,"+ printMeanEtcCSV(runtime,width, indexing, numFields));
			System.out.println("Runtime (Sec)             ,"+ printMeanEtcCSV1000(runtime, width, indexing, numFields));
			System.out.println("Speed up                  ,"+ printMeanEtcSpeedUpCSV(runtime,width, indexing, numFields));
			System.out.println("Num of results            ,"+ printMeanEtcCSV(numResultsFound,width, indexing, numFields));
			System.out.println("Avg Distance              ,"+ printMeanEtcCSV(avgDistance,width, indexing, numFields));
			System.out.println(star);
		}

		<T extends Number> void printAddHelper (double top, ArrayList<Double> values, ArrayList<T> arr) {
			values.add(Mean(top, arr));
			values.add(Median(top, arr).doubleValue());
			values.add(StandardDeviation(top, arr));
		}

		private String printMeanEtc(int l) {
			//return "           Mean          Median  *";
			int width = (l-1)/3;
			String s = FormatString("Mean", width)+"|"+FormatString("Median", width)+"|"+
					FormatString("Std Dev", width);

			for (int i=0; i<(l-s.length()); i++)
				s += " ";

			return s+" *";
		}

		private String printMeanEtcCSV(int numExperiments) {
			//return "           Mean          Median  *";
			String s = ",";
			s += AddWordCSV ("Mean", numExperiments);
			s += AddWordCSV ("Median", numExperiments);
			s += AddWordCSV ("Std Dev", numExperiments);

			return s;
		}
		private String AddWordCSV (String message, int num) {
			int a = num/2;
			int b = num-a;

			String s = "";

			for (int i=0;i<a;i++){
				s+=",";
			}
			s+=message;
			for (int i=0;i<b;i++){
				s+=",";
			}

			return s+",";
		}

		private String printMeanEtcSpeedUpCSV(ArrayList<Double> arr, int width,
				ArrayList<RAQ.IndexingScheme> indexing, int numFields) {
			String message = "";

			//mean-median-stdev
			for (int startingIndex=0; startingIndex<numFields; startingIndex++) {

				//find max
				double max = -1;
				int j = startingIndex;
				for (int i=0; i<indexing.size(); i++) {
					if (arr != null)
						if (max < arr.get(j))
							max = arr.get(j);

					j += numFields;
				}

				j = startingIndex;
				for (int i=0; i<indexing.size(); i++) {
					if (arr != null)
						message += FormatNumber(max/arr.get(j), width)+",";
					else
						message += FormatNumber(-1, width)+",";
					j += numFields;
				}
				message += ",";
			}

			return message+ " ,";
		}

		private String printMeanEtcCSV(ArrayList<Double> arr, int width,
				ArrayList<RAQ.IndexingScheme> indexing, int numFields) {
			String message = "";

			//mean-median-stdev
			for (int startingIndex=0; startingIndex<numFields; startingIndex++) {
				int j = startingIndex;

				for (int i=0; i<indexing.size(); i++) {
					if (arr != null)
						message += FormatNumber(arr.get(j), width)+",";
					else
						message += FormatNumber(-1, width)+",";
					j += numFields;
				}
				message += ",";
			}

			return message+ " ,";
		}
		private String printMeanEtcCSV1000(ArrayList<Double> arr, int width,
				ArrayList<RAQ.IndexingScheme> indexing, int numFields) {
			String message = "";

			//mean-median-stdev
			for (int startingIndex=0; startingIndex<numFields; startingIndex++) {
				int j = startingIndex;

				for (int i=0; i<indexing.size(); i++) {
					if (arr != null)
						message += FormatNumber(arr.get(j)/1000, width)+",";
					else
						message += FormatNumber(-1, width)+",";
					j += numFields;
				}
				message += ",";
			}

			return message+ " ,";
		}

		private String printMeanEtc(ArrayList<Double> arr, int width,
				ArrayList<RAQ.IndexingScheme> indexing, int numFields) {
			String message = "";

			//mean-median-stdev
			for (int startingIndex=0; startingIndex<numFields; startingIndex++) {
				int j = startingIndex;

				for (int i=0; i<indexing.size(); i++) {
					message += FormatNumber(arr.get(j), width)+" ";
					j += numFields;
				}
				message += "|";
			}

			return message+ " *";
		}	
		private String printMeanEtc1000(ArrayList<Double> arr, int width,
				ArrayList<RAQ.IndexingScheme> indexing, int numFields) {
			String message = "";

			//mean-median-stdev
			for (int startingIndex=0; startingIndex<numFields; startingIndex++) {
				int j = startingIndex;

				for (int i=0; i<indexing.size(); i++) {
					message += FormatNumber(arr.get(j)/1000, width)+" ";
					j += numFields;
				}
				message += "|";
			}

			return message+ " *";
		}	
	}

	static private class Result_V0 extends Helper{
		private int  num		= 0;
		ArrayList<Long> 	depth 			= new ArrayList<>();
		ArrayList<Long> 	numDFS 			= new ArrayList<>();
		ArrayList<Long> 	runtime			= new ArrayList<>();
		ArrayList<Long> 	numResultsFound	= new ArrayList<>();
		ArrayList<Double>  	avgDistance 	= new ArrayList<>();
		ArrayList<Long> 	querySize		= new ArrayList<>();
		ArrayList<Long> 	numEdgesTouched = new ArrayList<>();

		void add (Long runtime,		Long numResultsFound, 
				Double avgDistance, Long depth, Long numDFS,
				Long querySize, 	Long num_edges_touched) {
			num++;
			this.depth.add			(depth);
			this.numDFS.add			(numDFS);
			this.runtime.add		(runtime);
			this.querySize.add		(querySize);
			this.avgDistance.add	(avgDistance);
			this.numResultsFound.add(numResultsFound);
			this.numEdgesTouched.add(num_edges_touched);
		}

		void print (Long size, boolean useEdge) {
			if (num == 0)
				System.out.println("No data available ");
			else {
				int width   	= 15;
				int width_2 	= 5;
				int numFields 	= 3;

				int l = (width*numFields)+(2*(numFields-1));
				String star = "";
				for (int i=0; i<l+2; i++)
					star += "*";

				String space = "";
				for (int i=0; i<l-width_2; i++)
					space += " ";
				space += " *";

				System.out.println("******************************"+star);

				if (useEdge)
					System.out.println("* Query Graph size Edge     : "+FormatNumber(size, width_2)+space);
				else
					System.out.println("* Query Graph size node     : "+FormatNumber(size, width_2)+space);

				System.out.println("* Number of experiments     : "+FormatNumber(num, width_2)+space);
				System.out.println("******************************"+star);
				System.out.println("*                             "+ printMeanEtc(width));

				if (useEdge)
					System.out.println("* Query Graph size Edge     : "+ printMeanEtc(querySize,width));
				else
					System.out.println("* Query Graph size Node     : "+ printMeanEtc(querySize,width));

				System.out.println("* Depth  (Number of DFS)    : "+ printMeanEtc(depth,width));
				System.out.println("* Number of edges touched   : "+ printMeanEtc(numEdgesTouched,width));
				System.out.println("* Runtime (Milli Sec)       : "+ printMeanEtc(runtime,width));
				System.out.println("* Num of results            : "+ printMeanEtc(numResultsFound,width));
				System.out.println("* Avg Distance              : "+ printMeanEtc(avgDistance,width));
				System.out.println("******************************"+star);
			}
		}

		private String printMeanEtc(int width) {
			//return "           Mean          Median  *";
			return FormatString("Mean", width)+"  "+FormatString("Median", width)+
					"  "+FormatString("Std Dev", width)+" *";
		}

		private <T extends Number> String printMeanEtc (ArrayList<T> arr, int width) {
			String str;
			if (arr != null)
				str =  FormatNumber(Mean(arr), width)+"  "+FormatNumber(Median(arr), width)+
				"  "+FormatNumber(StandardDeviation(arr), width)+" *";
			else
				str =  FormatNumber(-1, width)+"  "+FormatNumber(-1, width)+"  "+FormatNumber(-1, width)+" *";

			return str;
		}
	}

	public static void ShowResults(int K, boolean useEdges, String fName) {
		JSONParser parser = new JSONParser();
		//String fname = System.getProperty("user.dir")+"/_results_"+K+".json";
		String fname = fName+K+".json";
		File f = new File(fname);

		HashMap<RAQ.IndexingScheme, HashMap<Long, Result_V0>> mapIndexToResultWOThreading 	= new HashMap<>();
		HashMap<RAQ.IndexingScheme, HashMap<Long, Result_V0>> mapIndexToResultWithThreading= new HashMap<>();

		try {
			Scanner sc = new Scanner(f);
			System.out.println("######### K : "+K+" #########");
			while (sc.hasNext()) {
				JSONObject jo = (JSONObject) parser.parse(sc.next());

				if (jo == null) {
					continue;
				}

				Long querySizeNode				= (Long) 	jo.get("QueryGraphSizeNode");
				Long querySizeEdge				= (Long) 	jo.get("QueryGraphSizeEdge");
				Long runtime   					= (Long)	jo.get("Runtime");

				RAQ.IndexingScheme indexingScheme = RAQ.IndexingScheme.valueOf((String)jo.get("Indexing"));

				Boolean multithreadingEnabled 	= (Boolean) jo.get("MultithreadingEnabled");
				//Long _K							= (Long) 	jo.get("K");
				Long numResultsFound 			= (Long) 	jo.get("NumResultsFound");
				Double avgDistance 				= (Double) 	jo.get("AvgDistance");
				Long depth			 			= (Long) 	jo.get("Depth");
				Long numDFS			 			= (Long) 	jo.get("NumDFS");
				Long numEdgesTouched			= (Long) 	jo.get("NumEdgesTouched");

				HashMap<RAQ.IndexingScheme, HashMap<Long, Result_V0>> mapMap;

				if (multithreadingEnabled)
					mapMap = mapIndexToResultWithThreading;
				else
					mapMap = mapIndexToResultWOThreading;

				HashMap<Long, Result_V0> map = mapMap.get(indexingScheme);
				if (map == null) {
					//first time we are seeing this indexing scheme
					map = new HashMap<>();
					mapMap.put(indexingScheme, map);
				}
				Long size, sizeOther;
				if (useEdges) {
					size 		= querySizeEdge;
					sizeOther 	= querySizeNode;
				} else {
					size 		= querySizeNode;
					sizeOther 	= querySizeEdge;
				}
				Result_V0 result = map.get(size);
				if (result == null) {
					//first time we are seeing the result for this indexing scheme
					result = new Result_V0();
					map.put(size, result);
				}

				result.add(runtime, numResultsFound, avgDistance,
						depth, numDFS, sizeOther, numEdgesTouched);
			}

			System.out.println("********** WO Threading ************");
			printHelper (mapIndexToResultWOThreading, useEdges);

			System.out.println("$$$$$$$$$$$$$$$$$$$$$$");
			System.out.println("********** With Threading ************");
			printHelper (mapIndexToResultWithThreading, useEdges);

			sc.close();

		}catch (FileNotFoundException e) {
			//we have not run our experiments for this K
		}catch(ParseException pe){
			System.out.println("position: " + pe.getPosition());
			System.out.println(pe);
		}
	}

	private static void printHelper (HashMap<RAQ.IndexingScheme, HashMap<Long, Result_V0>> mapMap, boolean useEdges) {
		for (RAQ.IndexingScheme scheme : RAQ.IndexingScheme.values()) {
			HashMap<Long, Result_V0> map;
			map = mapMap.get(scheme);
			if (map != null) {
				System.out.println("********** Indexing Scheme : "+scheme+" ************");
				for (Map.Entry<Long, Result_V0> pair : map.entrySet()) {
					pair.getValue().print(pair.getKey(), useEdges);
				}
			}
		}
	}

	static ArrayList<RAQ.IndexingScheme> GetJsonArrayIndexingScheme (JSONObject jo, String key) {
		ArrayList<RAQ.IndexingScheme> ret = new ArrayList<>();

		JSONArray jArray = (JSONArray) jo.get(key);

		for (Object o:jArray){
			ret.add(RAQ.IndexingScheme.valueOf((String)o));
		}

		return ret;
	}

	static ArrayList<Long> GetJsonArrayLong (JSONObject jo, String key) {
		ArrayList<Long> ret = new ArrayList<>();

		JSONArray jArray = (JSONArray) jo.get(key);

		for (Object o:jArray){
			ret.add(new Long((Long)o));
		}

		return ret;
	}
	static ArrayList<Double> GetJsonArrayDouble (JSONObject jo, String key) {
		ArrayList<Double> ret = new ArrayList<>();

		JSONArray jArray = (JSONArray) jo.get(key);

		for (Object o:jArray){
			ret.add(new Double((Double)o));
		}

		return ret;
	}

	public static void ShowResultsAll(int K, boolean useEdges, String fName) {
		double TOP = 1.0;

		JSONParser parser = new JSONParser();
		//String fname = System.getProperty("user.dir")+"/_results_"+K+".json";
		String fname = fName+K+".json";
		File f = new File(fname);

		HashMap<Long, ResultAll> mapSizeToResultWOThreading 	= new HashMap<>();
		HashMap<Long, ResultAll> mapSizeToResultWithThreading 	= new HashMap<>();

		try {
			Scanner sc = new Scanner(f);
			System.out.println("######### K : "+K+" #########");
			while (sc.hasNext()) {
				JSONObject jo = (JSONObject) parser.parse(sc.next());

				if (jo == null) {
					continue;
				}


				Long querySizeNode					= (Long) jo.get("QueryGraphSizeNode");
				Long querySizeEdge					= (Long) jo.get("QueryGraphSizeEdge");
				ArrayList<Long> runtime   			= GetJsonArrayLong (jo, "Runtime");

				ArrayList<RAQ.IndexingScheme> indexingScheme = GetJsonArrayIndexingScheme(jo, "Indexing");

				Boolean multithreadingEnabled 		= (Boolean) jo.get("MultithreadingEnabled");
				//Long _K								= (Long) jo.get("K");
				ArrayList<Long> numResultsFound 	= GetJsonArrayLong  (jo, "NumResultsFound");
				ArrayList<Double> avgDistance 		= GetJsonArrayDouble(jo, "AvgDistance");
				ArrayList<Long> depth			 	= GetJsonArrayLong  (jo, "Depth");
				ArrayList<Long> numDFS			 	= GetJsonArrayLong  (jo, "NumDFS");
				ArrayList<Long> numEdgesTouched		= GetJsonArrayLong  (jo, "NumEdgesTouched");

				HashMap<Long, ResultAll> map;

				if (multithreadingEnabled)
					map = mapSizeToResultWithThreading;
				else
					map = mapSizeToResultWOThreading;

				Long size, sizeOther;
				if (useEdges) {
					size 		= querySizeEdge;
					sizeOther 	= querySizeNode;
				} else {
					size 		= querySizeNode;
					sizeOther 	= querySizeEdge;
				}
				ResultAll result = map.get(size);
				if (result == null) {
					//first time we are seeing the result for this indexing scheme
					result = new ResultAll();
					map.put(size, result);
				}

				result.add(indexingScheme, runtime, numResultsFound,
						avgDistance, depth, numDFS, sizeOther, numEdgesTouched);
			}

			System.out.println("********** WO Threading ************");
			printHelperAll (1, mapSizeToResultWOThreading, useEdges);
			System.out.println("################   "+(TOP*100)+"%   ####################");
			printHelperAll (TOP, mapSizeToResultWOThreading, useEdges);

			System.out.println("$$$$$$$$$$$$$$$$$$$$$$");
			System.out.println("********** With Threading ************");
			printHelperAll (1, mapSizeToResultWithThreading, useEdges);

			sc.close();

		}catch (FileNotFoundException e) {
			//we have not run our experiments for this K
			System.out.println("Working Directory = " +
					System.getProperty("user.dir"));
		}catch(ParseException pe){
			System.out.println("position: " + pe.getPosition());
			System.out.println(pe);
		}
	}

	private static void printHelperAll (double top, HashMap<Long, ResultAll> map, boolean useEdges) {
		for (Map.Entry<Long, ResultAll> pair : map.entrySet()) {
			//key : size
			pair.getValue().printDisplay(top, pair.getKey(), useEdges);
		}
		for (Map.Entry<Long, ResultAll> pair : map.entrySet()) {
			//key : size
			pair.getValue().printCSV(top, pair.getKey(), useEdges);
		}
	}

	
	/**
	 * convert the entire json file into an arraylist
	 * @param fname
	 * @return
	 */
	private static ArrayList<Result> GetAllResults (String fname) {
		ArrayList<Result> resultsDisplay = new ArrayList<>();
		
		JSONParser parser = new JSONParser();
		File f = new File(fname);
		try {
			Scanner sc = new Scanner(f);
			while (sc.hasNext()) {
				JSONObject jo = (JSONObject) parser.parse(sc.next());

				resultsDisplay.add(new Result(jo));
			}

			sc.close();

		}catch (FileNotFoundException e) {
			//we have not run our experiments for this K
			System.out.println("Working Directory = " +
					System.getProperty("user.dir"));
			System.out.println("File name         = " + fname);
		}catch(ParseException pe){
			System.out.println("position: " + pe.getPosition());
			System.out.println(pe);
		}
		
		return resultsDisplay;
	}
}