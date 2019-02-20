import java.util.Date;

/**
 * Class to implement Tic Toc feature of Matlab
 * @author jithin
 *
 */
public class TicToc {
	public static long startTime;

	   public static void Tic() {
		   boolean loud = true;
		   Tic (loud);
	   }
	   
	   public static void Tic (boolean loud) {
    	startTime = System.currentTimeMillis();
    	
    	// Instantiate a Date object
        Date date = new Date();
         
        // display time and date using toString()
        if (loud)
        	System.out.println(date.toString());
    }

    public static long Toc() {
    	long elapsedTime = System.currentTimeMillis() - startTime;
    	System.out.println(String.format("Elapsed time: %d.%03dsec",
    			elapsedTime / 1000, elapsedTime % 1000));
    	return elapsedTime;
    }
}
