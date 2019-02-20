import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * A priority queue maintaining the Top-K elements added to it
 * @author jithin
 *
 */
public class KPriorityQueue <E> extends Helper{
	PriorityQueue<PQElement<E>> PQ;
	private int comp;	//passed to PQElement to be used with comparator, it is set to -1 for max heap
	private int K;
	private boolean thresholdSet = false;
	private double threshold;
	
	private ArrayList<E> 	elementList;
	private ArrayList<Double> valueList;
	private Double leastValue;
	
	/**
	 * 
	 * @param k : what is the size of the queue
	 * @param largest : set to true if we need the highest k values
	 */
	KPriorityQueue (int k, boolean largest) {
		K  = k;
		PQ = new PriorityQueue<>(k);
		if (largest) {
			// this is a min heap;
			comp = 1;
		} else {
			// this is a max heap;
			comp = -1;
		}
	}
	
	/**
	 * 
	 * @param k : what is the size of the queue
	 * @param largest : set to true if we need the highest k values
	 * @param threshold: The priority queue will saturate itself if 
	 * 		the number of element is K and the value of last element 
	 * 		is threshold
	 */
	KPriorityQueue (int k, boolean largest, double threshold) {
		K  = k;
		PQ = new PriorityQueue<>(k);
		if (largest) {
			// this is a min heap;
			comp = 1;
		} else {
			// this is a max heap;
			comp = -1;
		}
		
		thresholdSet=true;
		this.threshold = threshold;
	}
	
	/**
	 * Add element and maintain top K, we do not add null elements
	 * @param o : the object to be added
	 * @param v : the value on which sorting is done
	 * @return : boolean, true if the element was added
	 */
	boolean add (E o, double v) {
		boolean ret=true;

		if (o == null) {
			ret = false;
		} else {
			PQElement<E> element = new PQElement<E>(o, v, comp);

			if (PQ.size() < K) {
				PQ.add (element);
				leastValue = PQ.peek().value;
			}
			else {
				//see if our element is worthy of being added
				PQElement<E> top = PQ.peek();

				if (top.compareTo(element) < 0) {
					//our top element has to be removed
					PQ.poll();
					PQ.add(element);
					leastValue = PQ.peek().value;
				} else
					ret = false;
			}
		}

		return ret;
	}

	/**
	 * Return the elements in sorted order, NOTE: the queue will be emptied
	 * @return
	 */
	ArrayList<ObjectDoublePair<E>> toArrayListObjectDoublePair () {
		if (elementList==null)
			toArray();
		
		ArrayList<ObjectDoublePair<E>> elementValuePair = new ArrayList<>();
		
		Iterator<Double> vList = valueList.iterator();
		for (E e : elementList) {
			Double v = vList.next();
			elementValuePair.add(new ObjectDoublePair<E>(e, v));
		}
		
		return elementValuePair;
	}

	/**
	 * Return the elements in sorted order, NOTE: the queue will be emptied
	 * @return
	 */
	ArrayList<E> toArrayListElements () {
		if (elementList==null)
			toArray();
		return elementList;
	}
	
	/**
	 * Return the values in sorted order, NOTE: the queue will be emptied
	 * @return
	 */
	ArrayList<Double> toArrayListValues () {
		if (valueList==null)
			toArray();
		return valueList;
	}
	
	private void toArray () {
		elementList = new ArrayList<>(PQ.size());
		valueList 	= new ArrayList<>(PQ.size());
		
		//add elements in the reverse order
		while(!PQ.isEmpty()) {
			PQElement<E> p = PQ.poll();
			elementList.add(p.element);
			valueList.add(p.value);
		}

		Collections.reverse(elementList);
		Collections.reverse(valueList);
	}

	public int size() {
		return PQ.size();
	}

	/**
	 * Retrieves, but does not remove, value of the last element of this queue,
	 * or returns null if this queue is empty.
	 * @return
	 */
	public Double LeastValue() {
		//FIXME Sayan
		Double ret;
		if (leastValue < 1E-4)
			ret = 0.0;
		else
			ret = leastValue;
		
		return ret;
	}
	
	/**
	 * function returns the kth smallest value and Double.MAX_VALUE is size is less than K
	 * @return
	 */
	public Double LeastValueK () {
		if (size() < K)
			return Double.MAX_VALUE;
		else
			return LeastValue();
	}
	
	/**
	 * This functions returns true if we can stop inserting any more elements
	 * @return
	 */
	boolean ThresholdAchieved () {
		boolean ret = false;
		
		if (thresholdSet)
			if (size()==K)
				if (LeastValue() <= threshold)
					ret = true;
		
		return ret;
	}


	/**
	 * modify the threshold set
	 * @param dist
	 * @ return : true if the threshold is achieved
	 */
	public boolean UpdateThreshold(double dist) {
		threshold = dist;
		return ThresholdAchieved();
	}
}

/**
 * Each element in the priority queue is saved as a pair of the "element" and "value"
 * @author jithin
 *
 */
class PQElement <E> implements Comparable<PQElement <E>>, Comparator<PQElement <E>> {
	E element;		//any object can be use as a value
	double value;	//each element is associated with this value and sorted based on this
	private int comp;	//passed to PQElement to be used with comparator, it is set to -1 for max heap
	
	PQElement (E e, double v, int c) {
		element = e;
		value	= v;
		comp	= c;
	}

	@Override
	public int compare(PQElement<E> o1, PQElement<E> o2) {
		return comp * Double.compare(o1.value, o2.value);
	}

	@Override
	public int compareTo(PQElement<E> o) {
		return compare(this, o);
	}
}
