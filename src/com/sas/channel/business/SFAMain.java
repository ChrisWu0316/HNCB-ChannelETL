package com.sas.channel.business;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sas.channel.commapi.CommonProcess;
import com.sas.channel.commapi.FileProcess;

public class SFAMain {
	static Logger log = Logger.getLogger(SFAMain.class);

	// 實體檔案路徑
	private static String sfaPath = "D:\\SAS_CM\\NHFHC\\Channel\\SFA\\";
	private static String sfaPathWork = "D:\\SAS_CM\\NHFHC\\Channel\\SFA\\work\\";
	private static String sfaPathDone = "D:\\SAS_CM\\NHFHC\\Channel\\SFA\\done\\";
	private static String sfaPathError = "D:\\SAS_CM\\NHFHC\\Channel\\SFA\\error\\";
	private static String smsEDWPath = "D:\\SAS_CM\\NHFHC\\Channel\\SFA\\cmdm\\Encrypt\\";
	private static String smsParameterPath = "D:\\SAS_CM\\NHFHC\\Channel\\SFA\\cmdm\\Export\\SFA\\HB\\sfa\\file\\";
	
	private static String channel = "SFA";
	
	private static String[] sfaColumn = {
			"party_id", "campaign_cd", "communication_cd", "camp_subsidiary", "camp_business_unit", "channel", 
			"channel_team", "comm_collateral_reference", "communication_start_date", "communication_end_date", 
			"contact_creation_dttm", "score_txt", "sfa_cpn_comm_cd", "sfa_cpn_lot", "org_code", "add_info_desc", 
			"add_info_data", "dept_no"
			};

	public static void main(String args[]) {		
		log.info("------------------------Start SFAMain -------------------------------");
		// 掃描SMS資料夾
		List<String> fileList = new FileProcess().getFileList(sfaPath);
		for (String fileTmp : fileList) {
			// 搬移至work區處理，並把work區的檔案刪除
			if (fileTmp.indexOf("sfa") > -1) {
				new FileProcess().copyfile(sfaPath + fileTmp, sfaPathWork + fileTmp, true);
			}
		}
		// 再掃瞄一次work區，開始處理
		fileList = new FileProcess().getFileList(sfaPathWork);
		for (String fileTmp : fileList) {
			try {
				fileTmp = fileTmp.substring(0, fileTmp.indexOf(".")); // 去掉副檔名
				List<Map<String, Object>> campInfoListMap = new CommonProcess().queryCampaignInfo(fileTmp, channel, sfaColumn);
				String[] fileArr = fileTmp.split("_");
				output(campInfoListMap, fileArr[2]);
				//把work區的檔案搬到Done區
				new FileProcess().copyfile(sfaPathWork + fileTmp + ".sas7bdat", sfaPathDone + fileTmp + ".sas7bdat", true);
				log.info("  檔案寫入到done資料夾成功 " + fileTmp + ".sas7bdat");	
			} catch (Exception e) {
				//把work區的檔案搬到Error區
				new FileProcess().copyfile(sfaPathWork + fileTmp + ".sas7bdat", sfaPathError + fileTmp + ".sas7bdat", true);
				log.info("  檔案寫入到error資料夾成功 " + fileTmp + ".sas7bdat");	
				log.info(e);
				e.printStackTrace();
			}
		}
		log.info("------------------------End SFAMain -------------------------------");
	}

	/**
	 * 產生SMS所需檔案
	 * @param campInfoListMap
	 * @throws Exception
	 */
	private static void output(List<Map<String, Object>> campInfoListMap, String campcd) throws Exception {
		// 輸出 1.名單檔 
		try{
			String campaignCd = (String)campInfoListMap.get(0).get("campaign_cd");
			String[] campaignCdArr = campaignCd.split("_");
			campaignCd = campaignCdArr[0]+"_"+campaignCdArr[1]; // CAMPAIGN_CD 1筆資料第2個底線(含底線)前資料
			String deptNo = (String)campInfoListMap.get(0).get("dept_no"); // 部門別
			
			// 名單檔：HB_SFA_campaignCd_01_EX.txt
			String outputFileName = "HB_SFA_"+campaignCd+"_01_EX.txt";
			// 附加資訊檔：HB_SFA_campaignCd_01_EX_ADD_INFO_DATA.txt
			String outputAttachFileName = "HB_SFA_"+campaignCd+"_01_EX_ADD_INFO_DATA.txt";
			// 參數檔：HB_SFA_campaignCd_01.properties
			String outputParameterFileName = "HB_SFA_"+campaignCd+"_01.properties";
			
			String filePath = "";
			log.info("Dept No:" + deptNo.trim());
			if ("FHC".equals(deptNo.trim())) {
				filePath = smsEDWPath + "FHCMRK\\";
			} else {
				filePath = smsEDWPath + "HB\\"+deptNo.trim()+"\\";
				File file = new File(filePath);
				if (!file.exists()) {
					file.mkdir();
				}
			}
			
			// 產出名單檔
			sfaOutput(campInfoListMap, filePath+outputFileName);
			log.info("產出名單檔:"+filePath+outputFileName);
			// 產出附加資訊檔
			sfaOutputAttach(campInfoListMap, filePath+outputAttachFileName);
			log.info("產出附加資訊檔:"+filePath+outputAttachFileName);
			// 產出參數檔
			sfaOutputParameter(campInfoListMap, smsParameterPath+outputParameterFileName, outputFileName, outputAttachFileName);
			log.info("產出參數檔:"+smsParameterPath+outputParameterFileName);
			
		}catch(Exception ex){
			ex.printStackTrace();
			log.info(ex);
			throw ex;
		}
		
	}
	
	/**
	 * SFA 名單檔 EDW 串個資用文字檔
	 * @param columnMapList
	 * @param outputFilePath
	 * @return
	 */
	private static void sfaOutput(List<Map<String, Object>> columnMapList, String outputFilePath) throws Exception{
		try {			
			File outFile = new File(outputFilePath);
			BufferedWriter fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
			
			String[] column = {"campaign_cd", "communication_cd", "camp_subsidiary", "camp_business_unit", "party_id", 
					"channel", "channel_team", "comm_collateral_reference", "communication_start_date", "communication_end_date", 
					"contact_creation_dttm", "score_txt", "sfa_cpn_comm_cd", "sfa_cpn_lot", "org_code"};
			
			
			for (Map<String, Object> columnMap : columnMapList) {
				String dataLine = "";
				for (String columnName : column) {
					if (null != columnMap.get(columnName)) {
						if ("score_txt".equals(columnName)) {
							dataLine+=(Integer)columnMap.get(columnName)+"\t";
						} else {
							dataLine+=(String)columnMap.get(columnName)+"\t";
						}
					} else {
						dataLine+="\t";
					}
				}
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
	
	/**
	 * SFA 附加資訊檔 EDW 串個資用文字檔
	 * @param columnMapList
	 * @param outputFilePath
	 * @return
	 */
	private static void sfaOutputAttach(List<Map<String, Object>> columnMapList, String outputFilePath) throws Exception{
		try {			
			File outFile = new File(outputFilePath);
			BufferedWriter fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
			
			String[] column = {"campaign_cd", "communication_cd", "camp_business_unit", "sfa_cpn_comm_cd", "sfa_cpn_lot", 
					"party_id", "add_info_desc", "add_info_data"};
			//"display_seqno"
			
			int index = 1;
			for (Map<String, Object> columnMap : columnMapList) {
				String dataLine = "";
				for (String columnName : column) {
					if (null != columnMap.get(columnName)) {
						dataLine+=(String)columnMap.get(columnName)+"\t";
					} else {
						dataLine+="\t";
					}
				}
				dataLine+=index+"\t";
				fw.write(dataLine);
				fw.newLine();
				index++;
			}

			fw.flush();
			fw.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			log.info(ex);
			throw ex;
		}
	}
	
	/**
	 * SFA 參數檔 EDW 串個資用文字檔
	 * @param columnMapList
	 * @param outputFilePath
	 * @return
	 */
	private static void sfaOutputParameter(List<Map<String, Object>> columnMapList, 
			String outputFilePath, String fileName, String attachFileName) throws Exception{
		try {			
			File outFile = new File(outputFilePath);
			BufferedWriter fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
			
			String[] column = {"campaign_cd", "communication_cd"};
//					"cpn_list_file_name", "cpn_list_file_cnt", "list_extra_info_file_name", "list_extra_info_file_cnt"};
			
			Map<String, Object> columnMap = columnMapList.get(0);
			String dataLine = "";
			for (String columnName : column) {
				dataLine+=(String)columnMap.get(columnName)+"\t";
			}
			dataLine+=fileName+"\t";
			dataLine+=columnMapList.size()+"\t";
			dataLine+=attachFileName+"\t";
			dataLine+=columnMapList.size();
			
			fw.write(dataLine);
			fw.newLine();

			fw.flush();
			fw.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			log.info(ex);
			throw ex;
		}
	}
}