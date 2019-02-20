import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import sun.misc.Signal;
import sun.misc.SignalHandler;

@SuppressWarnings("restriction")
public class InterruptSearchSignalHandler implements SignalHandler {
	private static boolean interrupt=false;
	static boolean Interrupt () {
		return interrupt;
	}
	static void setInterruptFlag () {
		interrupt = true;
	}
	
	public static void listenTo(String name) {
		Signal signal = new Signal(name);
		Signal.handle(signal, new InterruptSearchSignalHandler());
	}
	@Override
	public void handle(Signal signal) {
		System.out.println();
		System.out.println("******************");
		System.out.println("Signal: " + signal);
		try {
			System.out.println("Pid   : " + getPid());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("******************");
		System.out.println();
		
		System.out.println("exiting from search if needed");
		interrupt=true;
	}

	static public <T1 extends NodeFeatures>
		void ResetFlag (Graph<T1> targetGraph) {
		interrupt = false;
		targetGraph.DisplayPreviousRandomSubgraphArrayNode();
	}
	
	private String getPid() throws IOException,InterruptedException {

		  Vector<String> commands=new Vector<String>();
		  commands.add("/bin/bash");
		  commands.add("-c");
		  commands.add("echo $PPID");
		  ProcessBuilder pb=new ProcessBuilder(commands);

		  java.lang.Process pr=pb.start();
		  pr.waitFor();
		  if (pr.exitValue()==0) {
		    BufferedReader outReader=new BufferedReader(new InputStreamReader(pr.getInputStream()));
		    return outReader.readLine().trim();
		  } else {
		    System.out.println("Error while getting PID");
		    return "";
		  }
		}
}
