import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

abstract class NodeFeatures{
	//this is to avoid division by zero
	//and also the case when we will have a no match
	//even when shapes of query graph and subgraph match
	static final double EPSILONve = 0.0001;
	
	/**
	 * function to get all the features in the node
	 * This arraylist should not be modified.
	 * @return
	 */
	abstract String[] AllFeatures();
	/**
	 * return all pairs of feature values
	 * @param features2
	 * @param diGraph 
	 * @return
	 */
	abstract ArrayList<ArrayList<String>> AllFeatureValues(NodeFeatures features2);
	
	abstract int NumFeatures();
	/**
	 * Features should be ordered in such a way that
	 * we have initially all the categorical features
	 * followed by real values/non-categorical features 
	 */
	abstract int NumCategoricalFeatures();
	abstract int NumNonCategoricalFeatures();
	/**
	 * get the summary of the nodes fetaure
	 */
	abstract int[][] getSummaryVector(double[] ve);
	static int veToBinIndex(double ve) {
		int j = (int) (ve*BFSQuery.HeuristicsNumBin);
		if (j == BFSQuery.HeuristicsNumBin)
			j--;
		return j;
	}

	/**
	 * Function to find the vector ve between two features
	 * @param features
	 * @return
	 */
	abstract double[] GetVE (NodeFeatures features);
	/**
	 * Print the details of the nodes
	 */
	abstract public void Print();
	abstract public void PrintCSV();
	abstract public void PrintCSVHeader();
	abstract public void PrintCSVToFile(FileWriter fooWriter) throws IOException;
	abstract public void PrintCSVHeaderToFile(FileWriter fooWriter) throws IOException;

	/**
	 * Find traditional distance
	 * 
	 * @param tFeatures
	 */
	abstract public double Distance (NodeFeatures tFeatures);

	/**
	 * the nodes are compared based on a unique identifier which need not be its feature
	 * @param tFeatures
	 * @return
	 */
	abstract public boolean NodeIsSameAs (NodeFeatures tFeatures);
	
	/**
	 * Does this feature meet the filtering criterion
	 * @return true if the feature can be filtered out
	 */
	abstract public boolean Filter ();

	/**
	 * Does this pair of edges meet the filtering criterion
	 * @return true if the map between the 2 edges can be filtered out
	 */
	abstract public boolean Filter (Graph.Edge qEdge, Graph.Edge tEdge);

	/**
	 * sets the min/max value for the ve vector
	 * @param ve
	 * @param a
	 * @param b
	 * @param index
	 */
	static void GetVeHelper(double[] ve, double a, double b, int index) {
		boolean zeroNotGood = false;
		ve[index] = MinByMax (a,b, zeroNotGood);
	}

	/**
	 * sets the min/max value for the ve vector, zero is not a good value
	 * @param ve
	 * @param a
	 * @param b
	 * @param index
	 */
	static void GetVeHelperZeroNotGood(double[] ve, double a, double b, int index) {
		boolean zeroNotGood = true;
		ve[index] = MinByMax (a,b, zeroNotGood);
	}

	/**
	 * finds min/max
	 * @param a
	 * @param b
	 */
	static double MinByMax (double a, double b){
		boolean zeroNotGood = false;
		
		return MinByMax(a, b, zeroNotGood);
	}
	/**
	 * finds min/max
	 * @param a
	 * @param b
	 */
	static double MinByMax (double a, double b, boolean zeroNotGood){
		double min, max;
		if (a>b){
			min=b;
			max=a;
		} else {
			min=a;
			max=b;
		}
		
		if (max == 0) {
			if (zeroNotGood)
				return 0;
			else
				return 1;
		} else
			return min/max;
	}

	/**
	 * Convert integers into a concated unique string 
	 * @param a
	 * @param b
	 * @return
	 */
	static String getString (int a, int b, boolean diGraph) {
		String ret;
		
		if (diGraph)
			ret = a+":"+b;
		else {
			if (a < b)
				ret = a+":"+b;
			else
				ret = b+":"+a;
		}
		
		return ret;
	}
}
