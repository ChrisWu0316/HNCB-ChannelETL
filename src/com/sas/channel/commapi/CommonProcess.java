package com.sas.channel.commapi;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sas.channel.dao.ddimSASDAO;

/**
 * Channel共用Function
 * @author Chris
 *
 */
public class CommonProcess {
	static Logger log = Logger.getLogger(CommonProcess.class);
	/**
	 * 取得 SAS dataset
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> queryCampaignInfo(String fileName, String channel, String[] column) throws Exception {
		List<Map<String, Object>> campInfoListMap = new ddimSASDAO().querySASDataSetInfo(fileName, channel, column);
		return campInfoListMap;
	}
}
