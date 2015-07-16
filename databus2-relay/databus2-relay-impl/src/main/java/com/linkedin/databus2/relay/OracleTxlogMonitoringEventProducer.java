package com.linkedin.databus2.relay;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.OracleTriggerMonitoredSourceInfo;
import com.linkedin.databus2.util.DBHelper;

import javax.management.MBeanServer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class OracleTxlogMonitoringEventProducer extends MonitoringEventProducer {

	public OracleTxlogMonitoringEventProducer(String name, String dbname, String uri, List<OracleTriggerMonitoredSourceInfo> sources, MBeanServer mbeanServer) {
		super(name, dbname, uri, sources, mbeanServer);
	}

	@Override
	public void run() {
		//check state and behave accordingly
		if (createDataSource() && openDbConn()) {
			do {
				PreparedStatement pstmt = null;
				ResultSet rs = null;
				try {
				  long maxDBScn = getMaxTxlogSCN(_con);
				  _log.info("Max DB Scn =  " + maxDBScn);
				  _dbStats.setMaxDBScn(maxDBScn);
					for (OracleTriggerMonitoredSourceInfo source: _sources) {
						String eventQuery = _monitorQueriesBySource.get(source.getSourceId());
						pstmt = _con.prepareStatement(eventQuery);
						pstmt.setFetchSize(10);
						//get max scn - exactly one row;
						rs = pstmt.executeQuery();
						if (rs.next())
						{
							long maxScn = rs.getLong(1);
							_log.info("Source: " + source.getSourceId() + " Max Scn=" + maxScn);
							_dbStats.setSrcMaxScn(source.getSourceName(), maxScn);
						}
						DBHelper.commit(_con);
						DBHelper.close(rs,pstmt, null);
						if (_state != MonitorState.SHUT) {
							Thread.sleep(PER_SRC_MAX_SCN_POLL_TIME);
						}
					}
					if (_state != MonitorState.SHUT) {
						Thread.sleep(MAX_SCN_POLL_TIME);
					}
				} catch (InterruptedException e) {
				    _log.error("Exception trace", e);
					shutDown();
				} catch (SQLException e) {
                   try { 
                        DBHelper.rollback(_con); 
                   } catch (SQLException s){}
				   _log.error("Exception trace", e);
				   shutDown();
				}
				finally
			    {
			      DBHelper.close(rs, pstmt, null);
			    }
			} while (_state != MonitorState.SHUT);
			_log.info("Shutting down dbMonitor thread");
			DBHelper.close(_con);

		}
	}

	protected String generateEventQuery(OracleTriggerMonitoredSourceInfo sourceInfo)
	{
	  /*
	  select scn from sy$txlog where txn = (select max(txn) from sy$member_account);
	  */
		StringBuilder sql = new StringBuilder();

		sql.append("select  scn from ").append(_schema).append("sy$txlog ");
		sql.append("where txn = ");
		sql.append (" ( select max(txn) from ").append(_schema).append("sy$").append(sourceInfo.getEventView()).append(" )");
		_log.info("Monitoring Query: " + sql.toString());

		return sql.toString();
	}

	/**
	 *
	   * Returns the max SCN from the sy$txlog table
	   * @param db
	   * @return the max scn
	   * @throws SQLException
	   */
	  protected long getMaxTxlogSCN(Connection db) throws SQLException
	  {
	    long maxScn = EventReaderSummary.NO_EVENTS_SCN;


	    String sql = "select " +
	                 "max(" + _schema + "sync_core.getScn(scn,ora_rowscn)) " +
	                 "from " + _schema + "sy$txlog where " +
	                 "scn >= (select max(scn) from " + _schema + "sy$txlog)";

	    PreparedStatement pstmt = null;
	    ResultSet rs = null;

	    try
	    {
	      pstmt = db.prepareStatement(sql);
	      rs = pstmt.executeQuery();

	      if(rs.next())
	      {
	        maxScn = rs.getLong(1);
	      }
	    }
	    finally
	    {
            DBHelper.close(rs, pstmt, null);
	    }

	    return maxScn;
	  }
}
