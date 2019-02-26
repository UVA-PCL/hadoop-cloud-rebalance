package org.apache.hadoop.hdfs.server.chord;
import java.net.InetSocketAddress;

/**
 * Ask predecessor thread that periodically asks for predecessor's keep-alive,
 * and delete predecessor if it's dead.
 * @author Chuan Xia
 *
 */
public class AskPredecessor extends Thread {
	
	private Node local;
	private boolean alive;
	
	public AskPredecessor(Node _local) {
		local = _local;
		alive = true;
	}
	
	@Override
	public void run() {
		while (alive) {
			InetSocketAddress predecessor = local.getPredecessor();
			if (predecessor != null) {
				String response = Helper.sendRequest(predecessor, "KEEP");
				if (response == null || !response.equals("ALIVE")) {
					//if(response == null)
					//	System.out.println("response is NULL");
					//else
					//	System.out.println("response is "+response);
					//System.out.println("clear predecessor: "+predecessor.getHostName());
					//if(!local.balancing)
					local.clearPredecessor();	
				}

			}
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void toDie() {
		alive = false;
	}
}


