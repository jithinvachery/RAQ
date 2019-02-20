import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The probability of all the feature value pairs are maintained in this
 * @author jithin
 *
 */
public class Probability {
	static double[][] prob;
	int numFeatures;
	ArrayList<HashMap<String, Integer>> featurePairMaps;
	
	public Probability (int numFeatures, int[] size) {
		this.numFeatures = numFeatures;
		prob = new double[numFeatures][];
		
		for (int i=0; i<numFeatures; i++) {
			int S = size[i]*size[i];
			System.out.println("Size : "+size[i]+"S"+S);
			prob[i]	= new double [S];
		}
		
		featurePairMaps = new ArrayList<>(numFeatures);
		for (int i=0; i<numFeatures; i++) {
			HashMap<String, Integer> featurepairMap = new HashMap<>();
			featurePairMaps.add(featurepairMap);
		}
	}

	/**
	 * Updates the statistics based on each edge
	 * @param edge
	 */
	public <T1 extends NodeFeatures>  
	void UpdateProbability(HashSet<Graph<T1>.Edge> edgeSet) {
		int numP = Runtime.getRuntime().availableProcessors();
		final class Worker implements Runnable {
			Graph<T1>.Edge _edge;
			Worker (Graph<T1>.Edge edge) {
				_edge = edge;
			}

			@Override
			public void run() {
				_UpdateProbHelper(_edge);
			}
		}

		ExecutorService executor = Executors.newFixedThreadPool(numP);

		for (Graph<T1>.Edge edge : edgeSet) {
			Runnable worker = new Worker(edge);
			executor.submit(worker);						
		}
		executor.shutdown();

		while (!executor.isTerminated()) {   }
		System.err.println(" : done");
	}

	private <T1 extends NodeFeatures> 
	void _UpdateProbHelper(Graph<T1>.Edge edge) {
		ArrayList<ArrayList<String>> featurePairsList = edge.AllFeatureValues();

		int i=-1;
		for (ArrayList<String> featurePairs : featurePairsList) {
			i++;

			HashMap<String, Integer> featurePairMap = featurePairMaps.get (i);

			synchronized (featurePairMap) {			
				for (String featurePair : featurePairs) {
					int j = getIndexOfFeaturePair (featurePairMap, featurePair);
					prob[i][j]++;
				}
			}
		}
	}
	
	/**
	 * Each feature pair has a unique index find it
	 * @param HashMap<String, Integer> featurePairMap
	 * @param featurePair : the feature pair value
	 * @return
	 */
	private int getIndexOfFeaturePair(HashMap<String, Integer> featurePairMap, String featurePair) {
		
		Integer index = featurePairMap.get(featurePair);
		
		if (index == null) {
			index = featurePairMap.size();
			featurePairMap.put(featurePair, index);
		}
		
		return index;
	}

	/**
	 * Convert the statistics into probability
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	void UpdateProbability() throws InterruptedException, ExecutionException {
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
					prob[_f][d] /= _sum;
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
					_sum += prob[_f][d];
				}
				
				return _sum;
			}
		}
		
		for (int f=0; f<numFeatures; f++) {
			double sum = 0.0;
			int len  = prob[f].length;
			int jump = len/numP;

			if (jump > 1) {
				ExecutorService executor = Executors.newFixedThreadPool(numP);
				ArrayList<Future<Double>> future = new ArrayList<>();
				System.err.print("P: calculating sum");
				for (int i=0; i< numP; i++) {
					int s = i*jump;
					int e = (i+1)*jump;

					if (i == (numP-1))
						e = prob[f].length;

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
						e = prob[f].length;

					Runnable worker = new Worker(s,e,f,sum);
					executor.submit(worker);						
				}
				executor.shutdown();

				while (!executor.isTerminated()) {   }
				System.err.println(" : done");
			} else {
				
				for (int j=0; j<prob[f].length; j++) {
					sum += prob[f][j];	
				}
				for (int j=0; j<prob[f].length; j++) {
					prob[f][j] /= sum;
				}	
			}
		}
	}

	/**
	 * Return the probability of the featurePair for the featureIndex
	 * @param featureIndex
	 * @param featurePair
	 * @return
	 */
	public double getProbability(int featureIndex, String featurePair) {
		HashMap<String, Integer> featurePairMap = featurePairMaps.get(featureIndex);
		
		int index = featurePairMap.get(featurePair);
		
		return prob[featureIndex][index];
	}
}
