
public class CGQTimeOut implements Runnable {
	static final int longDelay	= 100000; // 100 sec
	static final int shortDelay = 10000; // 10 sec
	
	final int durationMilliSec;
	public void run() {
		try {
			Thread.sleep(durationMilliSec);
			//we should interrupt the search now
			InterruptSearchSignalHandler.setInterruptFlag();
		} catch (InterruptedException e) {
			//e.printStackTrace();
			//search need not be timed out
		}
	}
	
	/**
	 * @param durationMilliSec : interrupt after how much time
	 */
	public CGQTimeOut(int durationMilliSec) {
		this.durationMilliSec = durationMilliSec;
	}
		
	static Thread thread;
	static boolean free = true;
	
	static void startTimeOut () {
		startTimeOut(shortDelay); // 10 sec sleep
	}
	static void startTimeOutLong () {
		startTimeOut(longDelay); // 10 sec sleep
	}
	static private void startTimeOut(int t) {
			// TODO Auto-generated method stub
		if (!free) {
			//fatal error
			System.err.println("Thread created twice");
			System.exit(-1);
		}
		free = false;
		thread = new Thread(new CGQTimeOut(t));
		thread.start();
	}
	
	static void stopTimeOut () {
		thread.interrupt();
		if (free) {
			//fatal error
			System.err.println("Thread killed twice");
			System.exit(-1);
		}
		free = true;
	}

}
