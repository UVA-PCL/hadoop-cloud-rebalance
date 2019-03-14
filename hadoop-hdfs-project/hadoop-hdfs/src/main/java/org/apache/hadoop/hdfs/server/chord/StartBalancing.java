package org.apache.hadoop.hdfs.server.chord;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;



public class StartBalancing {

	public static void main(String[] args) throws InterruptedException {
            for (String ip_port : args) {
                InetSocketAddress server = Helper.createSocketAddress(ip_port);
                System.out.println(Helper.sendRequest(server, "BALANCING"));
            }
	}

}
