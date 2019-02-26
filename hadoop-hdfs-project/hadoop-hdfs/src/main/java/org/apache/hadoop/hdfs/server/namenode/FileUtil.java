package org.apache.hadoop.hdfs.server.namenode;
import java.io.File;
import java.io.IOException;

public class FileUtil {

	
	public static boolean createFile(String destFileName) {
        File file = new File(destFileName);
        if(file.exists()) {
            System.out.println("create single file" + destFileName + " dest file already exists!");
            return false;
        }
        if (destFileName.endsWith(File.separator)) {
            System.out.println("create single file" + destFileName + "fail, dest file cannot be dir");
            return false;
        }
        
        if(!file.getParentFile().exists()) {
            
            System.out.println(" dir of dest file not exists, prepare to create it");
            if(!file.getParentFile().mkdirs()) {
                System.out.println("create dir of dest file fail");
                return false;
            }
        }
        
        try {
            if (file.createNewFile()) {
                System.out.println("create single file" + destFileName + "success");
                return true;
            } else {
                System.out.println("create single file" + destFileName + "fail");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("create single file" + destFileName + "fail" + e.getMessage());
            return false;
        }
    }
   
   
    public static boolean createDir(String destDirName) {
        File dir = new File(destDirName);
        if (dir.exists()) {
            System.out.println("create dir" + destDirName + "fail,the dest dir already exists");
            return false;
        }
        if (!destDirName.endsWith(File.separator)) {
            destDirName = destDirName + File.separator;
        }
        
        if (dir.mkdirs()) {
            System.out.println("create dir" + destDirName + "success");
            return true;
        } else {
            System.out.println("create dir" + destDirName + "fail");
            return false;
        }
    }
 

	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String dirName = "C:\\Users\\zl4dc\\Desktop\\research\\temp1";
        FileUtil.createDir(dirName);

        String fileName = dirName + "\\temp2\\tempFile.txt";
        FileUtil.createFile(fileName);

	}

}
