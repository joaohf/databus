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


import com.linkedin.databus.monitoring.mbean.DBStatistics;
import com.linkedin.databus.monitoring.mbean.DBStatisticsMBean;
import com.linkedin.databus.monitoring.mbean.SourceDBStatistics;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.OracleTriggerMonitoredSourceInfo;
import com.linkedin.databus2.util.DBHelper;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class OracleJournalMonitoringEventProducer extends MonitoringEventProducer {

//	private final	List<OracleTriggerMonitoredSourceInfo> _sources ;
//	private final String _name;
//	private final String _dbname;
//	protected enum MonitorState {INIT,RUNNING, PAUSE,SHUT}
//	MonitorState _state;
//	private final HashMap<Short, String> _monitorQueriesBySource;
//	static protected final long MAX_SCN_POLL_TIME = 10*60*1000;
//	static protected final long PER_SRC_MAX_SCN_POLL_TIME = 30*1000;
//	private Thread _curThread;
//	private final String _uri;
//	/** Logger for error and debug messages. */
//	private final Logger _log = Logger.getLogger(getClass());
//	private String _schema;
//
//	//stored as state ; as it may be required for graceful shutdown ; in case  of exception
//	private Connection _con;
//	private DataSource _dataSource;
//	private final DBStatistics _dbStats;
//	private final MBeanServer _mbeanServer;


	public OracleJournalMonitoringEventProducer(String name, String dbname, String uri, List<OracleTriggerMonitoredSourceInfo> sources, MBeanServer mbeanServer) {
		super(name, dbname, uri, sources, mbeanServer);
//		_sources = sources;
//		_name = name;
//		_dbname = dbname;
//		_state = MonitorState.INIT;
//		_con = null;
//		_dataSource = null;
//		_uri  = uri;
//		_schema = null;
//		_mbeanServer = mbeanServer;
//
//		// Generate the event queries for each source
//		_monitorQueriesBySource = new  HashMap<Short, String>();
//		_dbStats = new DBStatistics(dbname);
//		for(OracleTriggerMonitoredSourceInfo sourceInfo : sources)
//		{
//		  if (null==_schema)
//		  {
//		    //all logical sources have same schema
//		    _schema = sourceInfo.getEventSchema()==null ? "" : sourceInfo.getEventSchema()+".";
//		    _log.info("Reading source: _schema =  |" + _schema + "|");
//
//		  }
//		  _dbStats.addSrcStats(new SourceDBStatistics(sourceInfo.getSourceName()));
//		   String eventQuery = generateEventQuery(sourceInfo);
//		   _monitorQueriesBySource.put(sourceInfo.getSourceId(), eventQuery);
//		}
//        _dbStats.registerAsMbean(_mbeanServer);
//		_log.info("Created " + name + " producer ");
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

       select NR_OGG_SRC_TRNS_CSN as scn, (select count(*) from oggwrk.j$table_xwom_customer) as journal_count
       from eltggr_wom.table_xwom_customer
       where NR_OGG_SRC_TRNS_CSN = (select max(NR_OGG_SRC_TRNS_CSN) from eltggr_wom.table_xwom_customer)

	  */

		StringBuilder sql_count = new StringBuilder();
		sql_count.append(" (select count(*) from ").append(sourceInfo.getJournalTable()).append(") ");

		StringBuilder sql = new StringBuilder();

		sql.append("select NR_OGG_SRC_TRNS_CSN as scn, ").append(sql_count).append(" as journal_count");
		sql.append(" from ").append(_schema).append(sourceInfo.getEventView());
		sql.append(" where NR_OGG_SRC_TRNS_CSN = (select max(NR_OGG_SRC_TRNS_CSN) from ").append(_schema).append(sourceInfo.getEventView()).append(")");

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
	  protected long getMaxTxlogSCN(Connection db)  throws SQLException
	  {
	    return EventReaderSummary.NO_EVENTS_SCN;


//	    String sql = "select " +
//	                 "max(" + _schema + "sync_core.getScn(scn,ora_rowscn)) " +
//	                 "from " + _schema + "sy$txlog where " +
//	                 "scn >= (select max(scn) from " + _schema + "sy$txlog)";
//
//	    PreparedStatement pstmt = null;
//	    ResultSet rs = null;
//
//	    try
//	    {
//	      pstmt = db.prepareStatement(sql);
//	      rs = pstmt.executeQuery();
//
//	      if(rs.next())
//	      {
//	        maxScn = rs.getLong(1);
//	      }
//	    }
//	    finally
//	    {
//            DBHelper.close(rs, pstmt, null);
//	    }
//
//	    return maxScn;
	  }
}
