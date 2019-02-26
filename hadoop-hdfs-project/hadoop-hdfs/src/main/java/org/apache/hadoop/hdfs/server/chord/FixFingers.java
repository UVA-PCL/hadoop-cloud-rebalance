package org.apache.hadoop.hdfs.server.chord;
import java.net.InetSocketAddress;
import java.util.Random;

/**
 * Fixfingers thread that periodically access a random entry in finger table 
 * and fix it.
 * @author Chuan Xia
 *
 */

public class FixFingers extends Thread{

	private Node local;
	Random random;
	boolean alive;

	public FixFingers (Node node) {
		local = node;
		alive = true;
		random = new Random();
	}

	@Override
	public void run() {
		while (alive) {
			for(int i=2;i<=32;i++) {
				InetSocketAddress ithfinger = local.find_successor(Helper.ithStart(local.getId(), 1));
				//if((ithfinger != null && local.balancing) || !local.balancing)
				local.updateFingers(1, ithfinger);
				//else {
					//no update
				//}
				ithfinger = local.find_successor(Helper.ithStart(local.getId(), i));
				local.updateFingers(i, ithfinger);
			}
			try {
				Thread.sleep(30);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void toDie() {
		alive = false;
	}

}
