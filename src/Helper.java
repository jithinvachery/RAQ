import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

public class Helper {
	static boolean useHeuristics = true;
	static Random random = new Random(System.currentTimeMillis());

	static void MessagePrint (boolean yes, String message) {
		if (yes)
			System.out.print(message);
	}
	
	static void MessagePrintln (boolean yes, String message) {
		if (yes)
			System.out.println(message);
	}
	
	static class ObjectDoublePair <T>  implements Comparable<ObjectDoublePair <T>>{
		public ObjectDoublePair() {};
		
		public ObjectDoublePair(T e, Double v) {
			element = e;
			value 	= v;
		}
		
		T 		element;
		Double 	value;
		
		/**
		 * Reset the value field to null
		 */
		void reset () {
			value=null;
		}
		/**
		 * 
		 * @param points : arraylist of type <T>
		 * @return arraylist of ObjectDoublePair<T>, with values set to null
		 */
		public ArrayList<ObjectDoublePair<T>> getArray(HashSet<T> points) {
			ArrayList<ObjectDoublePair<T>> ret = new ArrayList<>();
			for (T point : points)
				ret.add(new ObjectDoublePair<T> (point, null));
			
			return ret;
		}

		@Override
		public int compareTo(ObjectDoublePair<T> o) {
			return Double.compare(value, o.value);
		}

	}
	
	/**
	 * We will find the relative index among the non-categorical features
	 * @param arr
	 * @param threshold
	 * @param numCategoricalFeatures
	 * @return
	 */
	public static Integer [] GetSortedIndexNonCategorical1 (final double[] arr,
			double threshold, int numCategoricalFeatures) {
		ArrayList<Integer> indexList = new ArrayList<>();
		
		for (int i=0; i<arr.length; i++) {
			indexList.add(i);
		}
		
		class IndexComparator implements Comparator<Integer>{

			@Override
			public int compare(Integer o1, Integer o2) {
				return -(Double.compare(arr[o1], arr[o2]));
			}
		}
		Collections.sort(indexList, new IndexComparator());
		
		double cummulative=0;
		
		ArrayList<Integer> ret = new ArrayList<>();
		for (Integer i : indexList) {
			if (cummulative > threshold)
				break;
			cummulative += arr[i];
			//we are only interested in non categorical features
			if (i >= numCategoricalFeatures)
				ret.add(i-numCategoricalFeatures);
		}
		
		Integer []r = new Integer[ret.size()];
		return ret.toArray(r);
	}
	
	public static Integer [] GetSortedIndex (final double[] arr, double threshold) {
		ArrayList<Integer> indexList = new ArrayList<>();
		
		for (int i=0; i<arr.length; i++) {
			indexList.add(i);
		}
		
		class IndexComparator implements Comparator<Integer>{

			@Override
			public int compare(Integer o1, Integer o2) {
				return -(Double.compare(arr[o1], arr[o2]));
			}
		}
		Collections.sort(indexList, new IndexComparator());
		
		double cummulative=0;
		
		ArrayList<Integer> ret = new ArrayList<>();
		for (Integer i : indexList) {
			if (cummulative > threshold)
				break;
			cummulative += arr[i];
			ret.add(i);
		}
		
		Integer []r = new Integer[ret.size()];
		return ret.toArray(r);
	}
	
	static class CallByReference <T> {
		T element;
		
		public CallByReference (T t) {
			element = t;
		}
		
		public CallByReference () {
			
		}
	}

	static class CallByReferenceDouble {
		double element;
		
		public CallByReferenceDouble() {
			element = 0;
		}
		
		void increment (double d) {
			element += d;
		}
	}
	
	static  <T extends Number> double  Mean (List<T> arr) {
		double sum=0;
		
		for (T a: arr)
			sum += a.doubleValue();
		
		return sum/arr.size();
	}
	
	static private <T extends Number> List<T> GetTop (double top, ArrayList<T> arr) {
		List<T> temp = new ArrayList<>(arr);
		
		class CompNumber implements Comparator<Number> {
			public int compare(Number a, Number b){
		        return Double.compare(a.doubleValue(), b.doubleValue());
		    }
		}

		Collections.sort (temp, new CompNumber());
		//remove largest elements
		long size = Math.round((double)temp.size()*top);
		temp = temp.subList(0, (int)size-1);
		return temp;
	}
	
	static  <T extends Number> double  Mean (double top, ArrayList<T> arr) {
		return Mean (GetTop(top, arr));
	}
	
	static  <T extends Number> T  Median (double top, ArrayList<T> arr) {
		return Median(GetTop(top, arr));
	}
	
	@SuppressWarnings("unchecked")
	static  <T extends Number> T  Median (Collection<T> arr) {
		// we do not want to modify the given array
		ArrayList<T> temp = new ArrayList<>(arr);
		
		class MyComparator implements Comparator<T> {

			@Override
			public int compare(T o1, T o2) {
				return Double.compare(o1.doubleValue(), o2.doubleValue());
			}
			
		}
		Collections.sort(temp, new MyComparator());
		
		Number ret = -1;
		
		if (arr.size() == 0)
			return (T)ret;
		return temp.get(arr.size()/2);
	}
	
	static  <T extends Number> double StandardDeviation (double top, ArrayList<T> arr) {
		return StandardDeviation(GetTop(top, arr));
	}
	
	static  <T extends Number> double StandardDeviation (List<T> arr) {
		if (arr.size() == 0)
			return 0;
		
		double mean = Mean (arr);
		double sum = 0;
 
        for (T i : arr)
            sum += Math.pow((i.doubleValue() - mean), 2);
        
        return Math.sqrt( sum / ( arr.size() - 1 ) );
	}
	
	static <T extends Number> String FormatNumber (T num, int width) {
		DecimalFormat dFormat = new DecimalFormat("#.##");
		
		String numString = dFormat.format(num);
		
		int pad = width - numString.length();
		
		String ret = "";
		for (int i=0; i<pad; i++)
			ret+=" ";
		
		ret += numString;
		
		return ret;
	}
	
	static String FormatString (String s, int width) {
		int pad = (width-s.length())/2;
		
		String ret = "";
		for (int i=0; i<pad; i++)
			ret+=" ";
		
		ret += s;
		
		for (int i=0; i<pad; i++)
			ret+=" ";
		
		if (ret.length() < width)
			ret+=" ";

		return ret;
	}
	
	/**
	 * return an arraylist of possibly the best processing order
	 * @param edgeSet
	 * @return
	 */
	static <T1 extends NodeFeatures>
	ArrayList<Graph<T1>.Edge> BestEdgeForProcessing (HashSet<Graph<T1>.Edge> edgeSet) {

		class EdgeComparator implements Comparator<Graph<T1>.Edge> {

			@Override
			public int compare(Graph<T1>.Edge o1, Graph<T1>.Edge o2) {
				int degree1 = o1.TotalDegree();
				int degree2 = o2.TotalDegree();

				return Integer.compare(degree1, degree2);
			}

		}

		ArrayList<Graph<T1>.Edge> edgeArr       	= new ArrayList<>();
		HashSet  <Graph<T1>.Edge> edgesConsidered	= new HashSet<>();
		PriorityQueue<Graph<T1>.Edge> pq        	= new PriorityQueue<>(5, new EdgeComparator());
		Graph<T1>.Edge firstEdge					= Collections.max(edgeSet, new EdgeComparator());

		pq.add(firstEdge);

		while (pq.size() > 0) {
			Graph<T1>.Edge cEdge = pq.poll();

			if (edgesConsidered.contains(cEdge)) {
				continue;
			}
			
			edgeArr.add(cEdge);
			edgesConsidered.add(cEdge);

			//Add all incoming and out going edges
			HashSet<Graph<T1>.Edge> edgesToBeConsidered = new HashSet<>();

			edgesToBeConsidered.addAll 		(cEdge.IncidentEdges());
			edgesToBeConsidered.removeAll	(edgesConsidered);

			pq.addAll(edgesToBeConsidered);
		}

		return edgeArr;
	}
	
	/**
	 * Function would increment only if RAQ.gatherStat is set to true
	 * @param cr
	 */
	static void GatherStatIncrementLong (CallByReference<Long> cr) {
		if (RAQ.gatherStat)
			cr.element++;
	}
	static void GatherStatIncrementInt (CallByReference<Integer> cr) {
		if (RAQ.gatherStat)
			cr.element++;
	}

	static <T1 extends NodeFeatures> void PrintTopK(
			ArrayList<Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>>> topK) {
		int i=1;
		for (Helper.ObjectDoublePair<ArrayList<Graph<T1>.Node>> odPair : topK) {
			ArrayList<Graph<T1>.Node> arr = odPair.element;
			
			System.out.println("**********************"+(i++)+"**********************");
			for (Graph<T1>.Node node : arr) {
				if (node == null) {
					System.out.println("Node id : null");
				} else {
					System.out.print("Node id : "+node.nodeID+" ");
					node.features.Print();
				}
			}
		}
	}

	/**
	 * Sort the given arraylist and display "Mean" "Max" "Median"
	 * @param degrees
	 */
	public static void SortAndPrintStat(ArrayList<Integer> degrees) {
		int size=degrees.size();
		int sum=0;
		
		for (Integer d : degrees)
			sum += d;
		
		Collections.sort(degrees);
		System.out.println("sum : "+sum);
		System.out.println("Mean   : "+((double)sum)/size);
		System.out.println("Max    : "+degrees.get(size-1));
		System.out.println("Median : "+degrees.get(size/2));		
	}

	/**
	 * Get a random number <=kMax and >=kMin
	 * @param kMin
	 * @param kMax
	 * @return
	 */
	static int RandomInt(int kMin, int kMax) {
		int ret=random.nextInt(kMax);
		
		while (ret < kMin) {
			ret = random.nextInt(kMax);
		}
		
		return ret;
	}


	/**
	 * Find the median of list after removing top 20% and bottom 20%
	 * @param arr
	 * @param sizeActual : size to be considered (if < than array size do not consider it)
	 * @return
	 */
	public static double MeanStable(ArrayList<Long> arr, int sizeActual) {
		ArrayList<Long> temp = new ArrayList<>(arr);
		
		Collections.sort(temp);
		
		int diff = 0;
		int size = temp.size();
		if (size < sizeActual) {
			diff = sizeActual - size;
			size = sizeActual;
		}
		
		//remove the top and bottom
		int s2 = size/5;
		int s1 = s2-diff;
		if (s1 < 0)
			s1=0;
		
		//remove bottom s elements
		
		double ret;
		
		try {
			ret = Mean (temp.subList(s1, temp.size()-s2));
		} catch (Exception e) {
			ret = Double.NaN;
		}
		
		return ret;
	}

	public static void UnImplemented(String msg) {
		System.err.println("unImplemented "+msg);
		System.exit(-1);		
	}


	/**
	 * Get the file name for the results to be saved
	 * @param experiment
	 * @param frac
	 * @param dataSet
	 * @return
	 */
	static String GetFname(Experiments.AllExperiments experiment,
			double frac, String dataSet) {
		String ret = "results/"+dataSet+"_"+experiment+"_test_"+RAQ.test;
		
		//I am adding a switch case so that I am always forced
		//to visit this function each time a new experiment is added.
		switch (experiment) {
		case BaseCase:
			break;
		case BeamWidth:
			break;
		case Exit:
			break;
		case Graphsize_vs_Runtime:
			break;
		case Graphsize_vs_Runtime_uniform:
			break;
		case K_vs_Runtime:
			break;
		case K_vs_Runtime_uniform:
			break;
		case TargetGraphSize:
			ret += "_frac_"+frac;
			break;
		case WidthThreshold:
			break;		
		case BeamWOIndex:
			break;
		case BranchingFactor:
			break;
		case Qualitative:
			break;
		case QualitativeQuantitative:
			break;
		case Heuristicts:
			break;
		}
		return ret;
	}

	
	/**
	 * find the number of elements > t
	 * @param arr
	 * @param t
	 * @return
	 */
	public static int rightTail(ArrayList<Long> arr, int t) {
		int ret = 0;
		
		for (Long l : arr)
			if (l>=t)
				ret++;
		
		return ret;
	}

	public static void searchInterruptMesseage() {
		System.out.println("**********************************************************************");
		System.out.println("***   We have been interrupted, we shall still add the result    *****");
		System.out.println("**********************************************************************");
	}

	/**
	 * Sort 2 strings to get a unique id
	 * @param s1
	 * @param s2
	 * @return
	 */
	public static String getID(String s1, String s2) {
		String id;
		
		if (s1.compareTo(s2) < 0)
			id = s1+" # "+s2;
		else
			id = s2+" # "+s1;
		
		return id;
	}

	/**
	 * Increment the map value
	 * @param map
	 * @param string
	 * @param string2
	 */
	public static void incrementMap(HashMap<String, Integer> map, String s1, String s2) {
		String id = getID(s1, s2);
		
		Integer n = 1+map.getOrDefault(id, 0);
		
		map.put(id, n);
	}

	/**
	 * Increment the map value
	 * @param map
	 * @param string
	 */
	public static void incrementMap(HashMap<String, Integer> map, String id) {
		Integer n = 1+map.getOrDefault(id, 0);
		map.put(id, n);
	}
}
