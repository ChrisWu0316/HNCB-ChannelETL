package com.sas.channel.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.sas.iom.SAS.ILanguageService;
import com.sas.iom.SAS.IWorkspace;
import com.sas.iom.SAS.IWorkspaceHelper;
import com.sas.rio.MVAConnection;
import com.sas.services.connection.BridgeServer;
import com.sas.services.connection.ConnectionFactoryConfiguration;
import com.sas.services.connection.ConnectionFactoryInterface;
import com.sas.services.connection.ConnectionFactoryManager;
import com.sas.services.connection.ConnectionInterface;
import com.sas.services.connection.ManualConnectionFactoryConfiguration;
import com.sas.services.connection.Server;

/**
 * 與SAS相關的DAO
 * @author work
 *
 */
public class ddimSASDAO {
	static Logger log = Logger.getLogger(ddimSASDAO.class);

	// 連線SAS參數設定
	String classID = Server.CLSID_SAS;

	String host = "10.8.220.190";
	int port = 8591;
	String userName = "sasdemo";
	String password = "P@ssw0rd";
	
	/**
	 * 取得 SAS dataset
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> querySASDataSetInfo(String fileName, String channel, String[] column) throws Exception {		
		StringBuilder sb = new StringBuilder();
		sb.append(" libname ").append(channel).append(" 'D:\\SAS_CM\\NHFHC\\Channel\\").append(channel).append("\\work\\'; ");
		
		sb.append(" PROC SQL; ");
		sb.append(" CREATE TABLE WORK.outContents AS ");
		sb.append(" SELECT ");
		String columnStr = "";
		for (String columnName : column) {
			columnStr+=columnName+",";
		}
		columnStr = columnStr.substring(0, columnStr.length()-1);
		sb.append(columnStr);
		sb.append(" FROM ").append(channel).append(".").append(fileName);
		sb.append(" ; ");
		sb.append(" run; ");
		
		log.debug(sb.toString());

		ConnectionInterface cx = null;
		IWorkspace sasWorkspace = null;
		Connection conn = null;
		Statement state = null;
		ResultSet rs = null;
		try {
			Server server = new BridgeServer(classID, host, port);
			ConnectionFactoryConfiguration cxfConfig = new ManualConnectionFactoryConfiguration(server);
			ConnectionFactoryManager cxfManager = new ConnectionFactoryManager();
			ConnectionFactoryInterface cxf = cxfManager.getFactory(cxfConfig);
			cx = cxf.getConnection(userName, password);
			sasWorkspace = IWorkspaceHelper.narrow(cx.getObject());
			ILanguageService sasLanguage = sasWorkspace.LanguageService();
			sasLanguage.Submit(sb.toString());
			sasLanguage.FlushList(Integer.MAX_VALUE);

			conn = new MVAConnection(sasWorkspace, new Properties());
			state = conn.createStatement();
			
			rs = state.executeQuery(" select * from work.outContents ");
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			List<Map<String, Object>> campInfoListMap = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> campInfoMap = new HashMap<String, Object>();
				for (int i = 1; i <= columnCount; i++) {
					for (String columnName : column) {
						if (columnName.equals(rsmd.getColumnName(i).toLowerCase().trim())) {
							if ("score_txt".equals(columnName)) {
								campInfoMap.put(columnName, rs.getInt(rsmd.getColumnName(i)));
							} else {
								campInfoMap.put(columnName, rs.getString(rsmd.getColumnName(i)).trim());
							}
						}
					}
				}		
				campInfoListMap.add(campInfoMap);
			}

			return campInfoListMap;
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (state != null) {
					state.close();
				}
				if (conn != null) {
					conn.close();
				}
				if (sasWorkspace != null) {
					sasWorkspace.Close();
				}
				if (cx != null) {
					cx.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
