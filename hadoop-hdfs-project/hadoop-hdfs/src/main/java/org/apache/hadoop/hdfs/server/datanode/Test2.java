package org.apache.hadoop.hdfs.server.datanode;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Calendar;

import org.apache.hadoop.hdfs.server.chord.Helper;
import org.apache.hadoop.hdfs.server.chord.Listener;
import org.apache.hadoop.hdfs.server.chord.Node;

public class Test2 {
	public enum Node_State{
		Idle,Shed_transferring,Shed_receiving,Trans_transferring,Trans_receiving,Initializing
	}
	public static void main(String[] args) throws InterruptedException {
		
		//InetSocketAddress a = Helper.createSocketAddress("10.0.2.15:23333");
		//InetSocketAddress b = Helper.createSocketAddress("10.0.2.15:23333");
		//Node node = new Node(a, null, a);
		//node.join(a);
		//Listener listener = new Listener(node);
		//listener.start();
		//String s = Helper.sendRequest(a, "YOURINFO");
		try {
			System.out.println(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
