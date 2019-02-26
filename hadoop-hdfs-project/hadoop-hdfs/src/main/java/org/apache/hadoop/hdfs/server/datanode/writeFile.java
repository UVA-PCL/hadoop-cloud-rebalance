package org.apache.hadoop.hdfs.server.datanode;
import org.apache.hadoop.hdfs.server.datanode.readfile.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class writeFile {
	
	public static void main(String[] args) {
		String content = "1";
		String fileName = "/home/zl4dc/order";
		writeFile(fileName,content);
		content = readfile.readFile(fileName);
		if(content.equals("1"))
			System.out.println("correct "+content);
	}
	
	public static void writeFile(String fileName, String content) {
		
        File file = new File(fileName);
        BufferedWriter Writer = null;
        try {
        
            Writer = new BufferedWriter(new FileWriter(file,false));
            String tempString = null;
            Writer.write(content);

            Writer.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (Writer != null) {
                try {
                    Writer.close();
                    
                } catch (IOException e1) {
                }
            }
            
        }
		
    }
}
