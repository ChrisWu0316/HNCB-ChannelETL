package com.sas.channel.commapi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * 檔案處理
 * @author Chris
 *
 */
public class FileProcess {
	static Logger log = Logger .getLogger(FileProcess.class);

	/**
	 * 搜尋檔案
	 * @param folderPath
	 * @return
	 */
	public List<String> getFileList(String folderPath) {
		List<String> fileList = new ArrayList<String>();
		try {
			java.io.File folder = new java.io.File(folderPath);
			String[] list = folder.list();
			for (int i = 0; i < list.length; i++) {
				if (list[i].contains(".sas7bdat")) {
					fileList.add(list[i]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileList;
	}
	
	/**
	 * 複製檔案到 work or done or error 下 ，並刪除原始檔案
	 * @param srFile 來源檔案路徑
	 * @param dtFile 目的檔案路徑
	 * @param isDelete 是否要刪除
	 */
	public boolean copyfile(String srFile, String dtFile , boolean isDelete) {
		try {
			//複製檔案
			File f1 = new File(srFile);
			File f2 = new File(dtFile);
			InputStream in = new FileInputStream(f1);
			OutputStream out = new FileOutputStream(f2);
			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
			in.close();
			out.close();

			if(isDelete){
				// 刪除檔案
				if (f1.delete()) {
					return true;
				} else {
					log.info("刪除下列檔案失敗 ["+srFile+"]");
					return false;
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return false;
	}
	
}
