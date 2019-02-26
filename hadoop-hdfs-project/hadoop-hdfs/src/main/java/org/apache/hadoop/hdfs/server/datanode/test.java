package org.apache.hadoop.hdfs.server.datanode;

import java.net.InetSocketAddress;

import org.apache.hadoop.hdfs.server.chord.*;


public class test {

	public static void main(String[] args) {
		
		
		String filename = "/home/zl4dc//args/order";
		String portfile = "/home/zl4dc/args/ports";
		
		for(int i=0;i<10;i++) {
		
			System.out.println("loop: "+ (i+1));	
			
			String content = readfile.readFile(filename);
			if (content.equals("0")) {
				System.out.println("first node");
				String ports = readfile.readFile(portfile);
				String[] port_s = ports.split("\n");
				int port_n = Integer.parseInt(port_s[0]);

				writeFile.writeFile(filename, "1");
				System.out.println("port = " + port_n + "\n");
			}
			// join, contact is another node
			else{			
				int num = Integer.parseInt(content);
				String ports = readfile.readFile(portfile);
				String[] port_s = ports.split("\n");
				int port_cont = Integer.parseInt(port_s[0]);
				int port_its = Integer.parseInt(port_s[num]);
				System.out.println((num+1)+"th node");
				System.out.println("contact node: "+port_cont);
				System.out.println("itself: "+port_its+"\n");
				int new_port = Integer.parseInt(port_s[num]) + 1;
				num++;
				content = String.valueOf(num);
				writeFile.writeFile(filename, content);
				
				ports += "\n"+new_port;
				writeFile.writeFile(portfile, ports);

			}
		}
		writeFile.writeFile(filename, "0");

	}

}
