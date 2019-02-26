package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class readfile {
	
	public static void main(String[] args) {
		String content = null;
		String fileName = "/home/zl4dc/order";
		content = readFile(fileName);
		if(content.equals("0"))
			System.out.println(content);
	}
	
	public static String readFile(String fileName) {
		String content = "";
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
        
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            content = reader.readLine();
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                //content.concat(tempString);
            	//System.out.println(tempString);
            	content = content + "\n" + tempString;
            	

            }
            reader.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                   
                } catch (IOException e1) {
                }
            }
            
        }
		return content;
    }
}
