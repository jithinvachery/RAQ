import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.PriorityQueue;

public class CGQHierarchicalIndex <T1 extends NodeFeatures> {
	HashMap<String, Bin> categoricalToBinMap;
	int numCategoricalFeatures;
	int numNonCategoricalFeatures;
	int branchingFactor;
	
	/**
	 * We will bin edges based on the VE vector and then created a
	 * hierarchical index in each of the bins
	 * @param points : set of points to be added to our list
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public CGQHierarchicalIndex(HashSet<Graph <T1>.Edge> edges, int numCategoricalFeatures,
			int numNonCategoricalFeatures, int branchingFactor) throws InterruptedException, ExecutionException {
		CGQHierarchicalIndexInit (edges, numCategoricalFeatures, numNonCategoricalFeatures,
				branchingFactor, true, false);
	}
	
	public CGQHierarchicalIndex(HashSet<Graph <T1>.Edge> edges, int numCategoricalFeatures,
			int numNonCategoricalFeatures, int branchingFactor, boolean withHeuristics) throws InterruptedException, ExecutionException {
		CGQHierarchicalIndexInit (edges, numCategoricalFeatures, numNonCategoricalFeatures,
				branchingFactor, withHeuristics, false);
	}
	
	public CGQHierarchicalIndex(HashSet<Graph <T1>.Edge> edges, int numCategoricalFeatures,
			int numNonCategoricalFeatures, int branchingFactor,
			boolean withHeuristics, boolean showProgress) throws InterruptedException, ExecutionException {
		CGQHierarchicalIndexInit (edges, numCategoricalFeatures, numNonCategoricalFeatures,
				branchingFactor, withHeuristics, showProgress);
	}
	
	private void CGQHierarchicalIndexInit(HashSet<Graph <T1>.Edge> edges, int numCategoricalFeatures,
			int numNonCategoricalFeatures, final int branchingFactor,
			final boolean withHeuristics, boolean showProgress) throws InterruptedException, ExecutionException {
		TicToc.Tic();
		
		this.numNonCategoricalFeatures 	= numNonCategoricalFeatures;
		this.numCategoricalFeatures    	= numCategoricalFeatures;
		this.branchingFactor 			= branchingFactor;
		
		HashMap<String, HashSet<Graph<T1>.Edge>> veToEdges = new HashMap<>();
		
		if (numCategoricalFeatures == 0) {
			//we have no categorical features
			veToEdges.put(":", edges);
		} else {
			int numP = Runtime.getRuntime().availableProcessors();
			final class Worker implements Runnable {
				Graph<T1>.Edge _edge;
				HashMap<String, HashSet<Graph<T1>.Edge>> _veToEdges; 
				Worker (HashMap<String, HashSet<Graph<T1>.Edge>> veToEdges, Graph<T1>.Edge edge) {
					_edge		= edge;
					_veToEdges 	= veToEdges;
				}

				@Override
				public void run() {
					_CGQHierarchicalIndexInit_Helper(_veToEdges, _edge);
				}
			}

			ExecutorService executor = Executors.newFixedThreadPool(numP);
			for (Graph<T1>.Edge edge : edges) {
				Runnable worker = new Worker(veToEdges, edge);
				executor.submit(worker);						
			}
			executor.shutdown();

			while (!executor.isTerminated()) {   }
		}
		
		//Now that we have bined the edges,
		//we shall create an hierarchical index
		categoricalToBinMap = new HashMap<>();
		{
			class Worker implements Callable<Bin>{
				HashSet<Graph<T1>.Edge> _set;
				public Worker (HashSet<Graph<T1>.Edge> set) {
					_set = set;
				}

				@Override
				public Bin call() throws Exception {
					return (new Bin(_set, branchingFactor, withHeuristics));
				}
			}

			int numP = Runtime.getRuntime().availableProcessors();
			ExecutorService executor = Executors.newFixedThreadPool(numP);
			ArrayList<Future<Bin>> future = new ArrayList<>();

			ArrayList<String> keys = new ArrayList<>();
			for (Entry<String, HashSet<Graph<T1>.Edge>> entry: veToEdges.entrySet()) {
				keys.add(entry.getKey());
				
				Worker worker 			= new Worker(entry.getValue());				
				Future<Bin> result 	= executor.submit(worker);

				future.add(result);

				//categoricalToBinMap.put(entry.getKey(), new Bin(entry.getValue(), branchingFactor, withHeuristics));
			}			
			
			int i=0;
			for (Future<Bin> result : future) {
				Bin bin 	= result.get();
				String key 	= keys.get(i);
				categoricalToBinMap.put(key, bin);
				i++;
			}

			executor.shutdown();
			while (!executor.isTerminated()) {   }
		}
		TicToc.Toc();
	}

	private void _CGQHierarchicalIndexInit_Helper(HashMap<String, HashSet<Graph<T1>.Edge>> veToEdges,
			Graph<T1>.Edge edge) {
		String ve = edge.GetVeStringCategorical();

		HashSet<Graph<T1>.Edge> set = veToEdges.get(ve);

		if (set == null) {
			set = new HashSet<>();
			synchronized (veToEdges) {				
				veToEdges.put(ve, set);
			}
			set.add(edge);
		} else {
			synchronized (set) {				
				set.add(edge);
			}
		}
	}
	
	/**
	 * Bins are sorted based only on the categorical features
	 * @param qEdge
	 * @return
	 */
	ArrayList<Helper.ObjectDoublePair<Bin>> GetSortedBins (Graph<T1>.Edge qEdge) {
		ArrayList<Helper.ObjectDoublePair<Bin>> arr = new ArrayList<>();
		
		for (Entry<String, Bin> entry: categoricalToBinMap.entrySet()) {
			double sim = qEdge.CGQSimilarityEdgesMaxPOssibleConsideringCategorical(entry.getValue().canonicalEdge);
			
			arr.add(new Helper.ObjectDoublePair<Bin>(entry.getValue(), sim));
		}
		
		Collections.sort(arr, Collections.reverseOrder());
		return arr;
	}
	
	class Bin {

		static final int LEAF_SIZE = 10;
		
		int size;
		
		double[] veUpper, veLower; //the veVector Bin range
		//if we are leaf we maintain a sorted list for each feature
		boolean leaf;
		ArrayList<Graph <T1>.Edge> allEdges;
		Graph <T1>.Edge canonicalEdge;
		
		//if we are not a leaf we maintain left and right
		ArrayList<Bin> branches;
		
		private class BinSplitter implements Comparable<BinSplitter>,Comparator<BinSplitter>{
			double[] veUpper, veLower; //the veVector Bin range
			//ArrayList<HashSet<Double>> featureValues;
			ArrayList<ArrayList<Double>> featureValues;
			HashSet<Graph <T1>.Edge> edges;
			int _size;
			
			//the place were to split
			int splitFeatureSize 		=-1;
			int splitIndex	=-1;
			Double splitValue = null;
			
			BinSplitter (HashSet<Graph <T1>.Edge> edges) {
				this.edges = edges;
				_size = edges.size();
				
				//find the different values taken by features
				featureValues = new ArrayList<>();
				for (int i=0; i<numNonCategoricalFeatures; i++) {
					//featureValues.add(new HashSet<Double>());
					featureValues.add(new ArrayList<Double>());
				}
				
				for (Graph <T1>.Edge edge : edges) {
					double []ve = edge.GetVe();
					
					for (int i=0; i<numNonCategoricalFeatures; i++) {
						int j = numCategoricalFeatures+i;
						featureValues.get(i).add(ve[j]);
					}
				}
				
				//update the ve-vector of the bin
				veUpper = new double[numNonCategoricalFeatures];
				veLower = new double[numNonCategoricalFeatures];
				for (int i=0; i<numNonCategoricalFeatures; i++) {
					veUpper[i] = Collections.max(featureValues.get(i));
					veLower[i] = Collections.min(featureValues.get(i));
				}
				
				//Find the feature to split on
				
				boolean useVariance = true;
				
				if (useVariance ) {
					double max = -1;
					for (int i=0; i<numNonCategoricalFeatures; i++) {
						HashSet<Double> set = new HashSet<>(featureValues.get(i));
						int s = set.size();
						
						ArrayList<Double> arr = featureValues.get(i);
						double sd 			  = Helper.StandardDeviation(arr);
						
						if (sd > max) {
							max 			= sd;
							splitIndex 		= i;
							splitFeatureSize= s;
							splitValue 		= Helper.Median(set);
						}
					}					
				} else {
					for (int i=0; i<numNonCategoricalFeatures; i++) {
						HashSet<Double> set = new HashSet<>(featureValues.get(i));
						int s = set.size();

						if (s>splitFeatureSize) {
							splitIndex 		= i;
							splitFeatureSize= s;
							splitValue 		= Helper.Median(set);
						}
					}
				}
			}

			@Override
			public int compare(CGQHierarchicalIndex<T1>.Bin.BinSplitter o1,
					CGQHierarchicalIndex<T1>.Bin.BinSplitter o2) {
				return -1*Integer.compare(o1._size, o2._size);
			}

			@Override
			public int compareTo(CGQHierarchicalIndex<T1>.Bin.BinSplitter o) {
				return compare(this, o);
			}
		}
		
		Bin (HashSet<Graph <T1>.Edge> edgesPara, int branchingFactor, boolean withHeuristics) {
			BinSplitter binSplitter = new BinSplitter(edgesPara);

			Init (binSplitter, branchingFactor, withHeuristics);
		}
		
		private Bin(BinSplitter binSplitter, int branchingFactor, boolean withHeuristics) {
			Init (binSplitter, branchingFactor, withHeuristics);
		}
		
		private void Init (BinSplitter binSplitterPara, int branchingFactor, boolean withHeuristics) {
			size = binSplitterPara.edges.size();
			
			//here we are concerned only about non-categorical features
			for (Graph<T1>.Edge edge : binSplitterPara.edges) {
				canonicalEdge = edge;
				break;
			}

			veUpper = binSplitterPara.veUpper;
			veLower = binSplitterPara.veLower;
			
			if ((binSplitterPara.edges.size() <= LEAF_SIZE) ||
					(binSplitterPara.splitFeatureSize == 1)) {
				//we have found enough edges so no more going down
				PopulateLeaf(binSplitterPara.edges, withHeuristics);
			} else {
				leaf = false;

				int numBins = 1;
				PriorityQueue<BinSplitter> bsPQ = new PriorityQueue<>();
				bsPQ.add(binSplitterPara);

				branches = new ArrayList<>(branchingFactor);

				while ((numBins < branchingFactor) &&
						(!bsPQ.isEmpty())) {
					BinSplitter binSplitter = bsPQ.poll();

					if ((binSplitter.edges.size() <= LEAF_SIZE) ||
							(binSplitter.splitFeatureSize == 1)) {
						branches.add(new Bin(binSplitter, branchingFactor, withHeuristics));
					} else {
						//we have to form a hierarchy
						//we shall do a two way split on the feature with most variation

						//we have to split on splitIndex
						HashSet<Graph <T1>.Edge> leftEdges  = new HashSet<>();
						HashSet<Graph <T1>.Edge> rightEdges = new HashSet<>();

						Double split;
						if (binSplitter.splitFeatureSize == 2) {
							//median may not give correct results
							split = Collections.max(binSplitter.featureValues.get(binSplitter.splitIndex));
						} else {
							split = binSplitter.splitValue;
						}

						for (Graph <T1>.Edge edge : binSplitter.edges) {
							if (edge.GetVe(numCategoricalFeatures + binSplitter.splitIndex) < split)
								leftEdges.add(edge);
							else
								rightEdges.add(edge);
						}

						BinSplitter left = new BinSplitter(leftEdges);
						BinSplitter right= new BinSplitter(rightEdges);

						bsPQ.add(left);
						bsPQ.add(right);
						numBins++;
					}
				}
				
				//add the remaining elements
				while (!bsPQ.isEmpty()) {
					BinSplitter binSplitter = bsPQ.poll();
					branches.add(new Bin(binSplitter, branchingFactor, withHeuristics));
				}
			}
		}
				

		/**
		 * function to see if the bin contains only one type of ve vector
		 * @return
		 */
		boolean containsOnlyOneVEvector () {
			boolean ret = true;
			
			for (int i=0; i<veUpper.length; i++) {
				if (veLower[i] != veUpper[i]) {
					ret = false;
					break;
				}
			}
			
			return ret;
		}

		private void PopulateLeaf (HashSet<Graph <T1>.Edge> edges, boolean withHeuristics) {
			leaf 		= true;
			allEdges 	= new ArrayList<>(edges);
			
		}
		
		/**
		 * Find the maximum similarity possible for the query edge to any edge in the bin
		 * based only on the non-categorical features
		 * @param qEdge
		 * @return
		 */
		double MaxNonCategoricalsimilarity (Graph<T1>.Edge qEdge) {
			return qEdge.MaxNonCategoricalsimilarity (veUpper, veLower);
		}
		

		/**
		 * To be called only in non leaf nodes, sort bins based on similarity
		 * @param qEdge
		 * @return
		 */
		public Iterator<CGQHierarchicalIndex<T1>.Bin> iterator(Graph<T1>.Edge qEdge,
				boolean reverse) {
			class BinIterator implements Iterator<CGQHierarchicalIndex<T1>.Bin> {
				PriorityQueue<Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>> pq =
						new PriorityQueue<>();
				
				public BinIterator(Graph<T1>.Edge qEdge, boolean reverse) {
					for (CGQHierarchicalIndex<T1>.Bin bin : branches) {
						//we want to sort based on higher values of similarity
						double sim = bin.MaxNonCategoricalsimilarity(qEdge);
						if (!reverse)
							sim *= -1;
						
						Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> od;
						od = new Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>(bin, sim);
						pq.add(od);
					}
				}

				@Override
				public boolean hasNext() {
					return (!pq.isEmpty());
				}

				@Override
				public CGQHierarchicalIndex<T1>.Bin next() {
					Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> od = pq.poll();
					
					return od.element;
				}
				
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			}
			
			return new BinIterator(qEdge, reverse);
		}

		/**
		 * iterate over the edges based on their proximity to qEdge,
		 * Proximity is calculated based on the summary of neighbourhood. 
		 * @param qEdge
		 * @return
		 */
		ArrayList<Graph<T1>.Edge> iterationOrder; // arraylist associated with the heuristics
		public Iterator<Graph<T1>.Edge> heuristicsIterator(Graph<T1>.Edge qEdge) {
			if (!leaf) {
				//we should not be calling this function
				System.err.println("Calling the heuristicsIterator in non-leaf bin");
				return null;
			}

			ArrayList<Helper.ObjectDoublePair<Graph<T1>.Edge>> edgeList = new ArrayList<>(this.size);
			
			//add each of the edge into the new list
			for (Graph<T1>.Edge edge : this.allEdges){
				Double dist = qEdge.heuristicsDistance(edge) + Helper.random.nextGaussian();
				
				Helper.ObjectDoublePair<Graph<T1>.Edge> od = 
						new Helper.ObjectDoublePair<Graph<T1>.Edge>(edge, dist);
				
				edgeList.add(od);
			}
			
			//sort the list and return store the arraylist of edges
			Collections.sort(edgeList);
			iterationOrder = new ArrayList<>(this.size);
			
			for (Helper.ObjectDoublePair<Graph<T1>.Edge> od : edgeList)
				iterationOrder.add(od.element);
			
			return iterationOrder.iterator();
		}
	}
	
	/**
	 * Start processing in order of smallest bin size first
	 * @param edgeSet
	 * @return
	 */
	public ArrayList<Graph<T1>.Edge> BestEdgeForProcessing (HashSet<Graph<T1>.Edge> edgeSet) {
		ArrayList<Graph<T1>.Edge> ret = new ArrayList<>();
		ArrayList<Helper.ObjectDoublePair<Graph<T1>.Edge>> odArr = new ArrayList<>();
		
		for (Graph<T1>.Edge edge : edgeSet) {
			double size = LeafSize (edge);
			odArr.add(new Helper.ObjectDoublePair<Graph<T1>.Edge>(edge, size));
		}
		
		Helper.ObjectDoublePair<Graph<T1>.Edge> bestEdge = Collections.min(odArr);
		
		PriorityQueue<Helper.ObjectDoublePair<Graph<T1>.Edge>> pq = new PriorityQueue<>();
		
		HashSet<Graph<T1>.Edge> edgesConsidered = new HashSet<>();
		edgesConsidered.add(bestEdge.element);
		
		do {
			Graph<T1>.Edge best = bestEdge.element;
			ret.add(best);
			
			//add the neighbouring edges
			for (Graph<T1>.Edge edge : best.node1.AllEdges()) {
				if (edgesConsidered.contains(edge))
					continue;
				
				//find the object double pair for edge
				pq.add(BestEdgeForProcessingHelper(edge, odArr));
				edgesConsidered.add(edge);
			}
			for (Graph<T1>.Edge edge : best.node2.AllEdges()) {
				if (edgesConsidered.contains(edge))
					continue;
				
				//find the object double pair for edge
				pq.add(BestEdgeForProcessingHelper(edge, odArr));				
				edgesConsidered.add(edge);
			}
		} while ((bestEdge = pq.poll()) != null);
		
		
		if (RAQ.slowAndSafe)
			if ((ret.size() != edgeSet.size()) || 
					(new HashSet<>(ret).size() != edgeSet.size())){
				System.err.println("BestEdgeForProcessing : fatal error ret : "+ret.size()+" edgeset : "+edgeSet.size());
				System.exit(-1);
			}

		return ret;
	}

	private Helper.ObjectDoublePair<Graph<T1>.Edge> BestEdgeForProcessingHelper (Graph<T1>.Edge edge,
			ArrayList<Helper.ObjectDoublePair<Graph<T1>.Edge>> odArr) {
		Helper.ObjectDoublePair<Graph<T1>.Edge> ret = null;
		for (Helper.ObjectDoublePair<Graph<T1>.Edge> od : odArr) {
			if (od.element == edge) {
				ret = od;
				break;
			}
		}
		
		return ret;
	}

	/**
	 * Find the size of the first leaf which will be explored
	 * @param edge
	 * @return
	 */
	private Integer LeafSize(Graph<T1>.Edge edge) {
		Integer size = null;
		//sort the bins in the main index
		ArrayList<Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin>> sortedBins = 
				GetSortedBins(edge);

		for (Helper.ObjectDoublePair<CGQHierarchicalIndex<T1>.Bin> odBin : sortedBins) {
			CGQHierarchicalIndex<T1>.Bin bin = odBin.element;
			while (!bin.leaf) {
				//since we are not at the leaf we need to go deeper
				Iterator<CGQHierarchicalIndex<T1>.Bin> it = bin.iterator(edge, false);
				
				bin = it.next();
			}
			//now the bin is the leaf
			size = bin.size;
			break;
		}
		
		return size;
	}

}
