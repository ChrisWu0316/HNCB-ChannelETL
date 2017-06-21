package com.sas.channel.business;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sas.channel.commapi.CommonProcess;
import com.sas.channel.commapi.FileProcess;

public class SMSMain {
	static Logger log = Logger.getLogger(SMSMain.class);

	// 實體檔案路徑
	// D:\SAS_CM\NHFHC\Channel\SMS
	private static String smsPath = "D:\\SAS_CM\\NHFHC\\Channel\\SMS\\";
	private static String smsPathWork = "D:\\SAS_CM\\NHFHC\\Channel\\SMS\\work\\";
	private static String smsPathDone = "D:\\SAS_CM\\NHFHC\\Channel\\SMS\\done\\";
	private static String smsPathError = "D:\\SAS_CM\\NHFHC\\Channel\\SMS\\error\\";
	private static String smsEDWPath = "D:\\SAS_CM\\NHFHC\\Channel\\SMS\\EDW\\";
	
	private static String channel = "SMS";
	
	private static String[] smsColumn = {
			"party_id", "customerid", "message", "senddate", "sendTime", "sysid", "servicetype"
			};

	public static void main(String args[]) {		
		log.info("------------------------Start SMSMain -------------------------------");
		// 掃描SMS資料夾
		List<String> fileList = new FileProcess().getFileList(smsPath);
		for (String fileTmp : fileList) {
			// 搬移至work區處理，並把work區的檔案刪除
			if (fileTmp.indexOf("sms") > -1) {
				new FileProcess().copyfile(smsPath + fileTmp, smsPathWork + fileTmp, true);
			}
		}
		// 再掃瞄一次work區，開始處理
		fileList = new FileProcess().getFileList(smsPathWork);
		for (String fileTmp : fileList) {
			try {
				fileTmp = fileTmp.substring(0, fileTmp.indexOf(".")); // 去掉副檔名
				List<Map<String, Object>> campInfoListMap = new CommonProcess().queryCampaignInfo(fileTmp, channel, smsColumn);
				String[] fileArr = fileTmp.split("_");
				output(campInfoListMap, fileArr[2]);
				//把work區的檔案搬到Done區
				new FileProcess().copyfile(smsPathWork + fileTmp + ".sas7bdat", smsPathDone + fileTmp + ".sas7bdat", true);
				log.info("  檔案寫入到done資料夾成功 " + fileTmp + ".sas7bdat");	
			} catch (Exception e) {
				//把work區的檔案搬到Error區
				new FileProcess().copyfile(smsPathWork + fileTmp + ".sas7bdat", smsPathError + fileTmp + ".sas7bdat", true);
				log.info("  檔案寫入到error資料夾成功 " + fileTmp + ".sas7bdat");	
				log.info(e);
				e.printStackTrace();
			}
		}
		log.info("------------------------End SMSMain -------------------------------");
	}

	/**
	 * 產生SMS所需檔案
	 * @param campInfoListMap
	 * @throws Exception
	 */
	private static void output(List<Map<String, Object>> campInfoListMap, String campcd) throws Exception {
		// 輸出 1.名單檔 
		DateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
		DateFormat HHmmSS = new SimpleDateFormat("HHmmSS");
		Calendar cal = Calendar.getInstance();
		try{
			// 匯出檔命名規則:CM_SMS_[CAMPCD]_[YYYYMMDD]_[HHMMSS].txt
			String yyyymmdd = yyyyMMdd.format(cal.getTime());
			String hhmmss = HHmmSS.format(cal.getTime());
			String outputfileName = "CM_SMS_" + campcd.toUpperCase() + "_" + yyyymmdd + "_" + hhmmss;
			
			String outputFilePath = smsEDWPath + "/" + outputfileName + ".txt";
			
			smsOutput(campInfoListMap, outputFilePath);
			log.info("SMS EDW 串個資用文字檔產製： " + outputFilePath);	
		}catch(Exception ex){
			ex.printStackTrace();
			log.info(ex);
			throw ex;
		}
		
	}
	
	/**
	 * SMS EDW 串個資用文字檔
	 * @param columnMapList
	 * @param outputFilePath
	 * @return
	 */
	private static void smsOutput(List<Map<String, Object>> columnMapList, String outputFilePath) throws Exception {
		try {			
			File outFile = new File(outputFilePath);
			BufferedWriter fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
			
			String heard = "";
			for (String colmunName : smsColumn) {
				heard+=colmunName+",";
			}
			heard = heard.substring(0, heard.length()-1);
			fw.write(heard);
			fw.newLine();
			
			for (Map<String, Object> columnMap : columnMapList) {
				String dataLine = "";
				for (String columnName : smsColumn) {
					if (null != columnMap.get(columnName) && !"".equals((String)columnMap.get(columnName))) {
						dataLine+=(String)columnMap.get(columnName)+",";
					} else {
						dataLine+=",";
					}
				}
				dataLine = dataLine.substring(0, dataLine.length()-1);
				fw.write(dataLine);
				fw.newLine();
			}

			fw.flush();
			fw.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			log.info(ex);
			throw ex;
		}
	}
}