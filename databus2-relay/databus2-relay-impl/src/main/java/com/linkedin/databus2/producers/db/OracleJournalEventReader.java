package com.linkedin.databus2.producers.db;
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


import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNWriter;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig.ChunkingType;
import com.linkedin.databus2.util.DBHelper;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Class which can read events from the Oracle databus transaction log (sy$txlog) starting from
 * a given SCN. Each call to the readEventsFromAllSources will perform one event cycle against all
 * sources. That is, it will fetch any existing events starting at the given SCN and then return.
 * <p/>
 * Semantics : If a connection / PreparedStmt is a created within a method, and not cached in a member variable,
 * the method is responsible for closing it. If not, it should NOT close it.
 */
public class OracleJournalEventReader
        implements SourceDBEventReader {
    public static final String MODULE = OracleJournalEventReader.class.getName();

    private final String _name;
    private final List<OracleTriggerMonitoredSourceInfo> _sources;
    private final String _selectSchema;

    private final DataSource _dataSource;

    private static final int DEFAULT_STMT_FETCH_SIZE = 100;
    private static final int MAX_STMT_FETCH_SIZE = 1000;

    private final DbusEventBufferAppendable _eventBuffer;
    private final boolean _enableTracing;

    private final Map<Short, String> _eventQueriesBySource;
//    private final Map<Short, String> _eventChunkedScnQueriesBySource;
//    private final Map<Short, String> _eventChunkedTxnQueriesBySource;

    static private final String NR_OGG_SRC_TRNS_CSN = "NR_OGG_SRC_TRNS_CSN";
    static private final String TS_OGG_SRC_TRNS_COMMIT = "TS_OGG_SRC_TRNS_COMMIT";

//    private PreparedStatement _txnChunkJumpScnStmt;

    /**
     * Logger for error and debug messages.
     */
    private final Logger _log;
    private final Logger _eventsLog;

    private Connection _eventSelectConnection;
    private final DbusEventsStatisticsCollector _relayInboundStatsCollector;
    private final MaxSCNWriter _maxScnWriter;
    private long _lastquerytime;
//    private long _lastMaxScnTime;
    private final long _slowQuerySourceThreshold;

    private final ChunkingType _chunkingType;
    private final long _txnsPerChunk;
    private final long _scnChunkSize;
    private final long _chunkedScnThreshold;
    private final long _maxScnDelayMs;

    private long _lastSeenEOP = EventReaderSummary.NO_EVENTS_SCN;

    //private volatile boolean _inChunkingMode = false;
    //private volatile long _catchupTargetMaxScn = -1L;


    public OracleJournalEventReader(String name,
                                    List<OracleTriggerMonitoredSourceInfo> sources,
                                    DataSource dataSource,
                                    DbusEventBufferAppendable eventBuffer,
                                    boolean enableTracing,
                                    DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
                                    MaxSCNWriter maxScnWriter,
                                    long slowQuerySourceThreshold,
                                    ChunkingType chunkingType,
                                    long txnsPerChunk,
                                    long scnChunkSize,
                                    long chunkedScnThreshold,
                                    long maxScnDelayMs) {
        List<OracleTriggerMonitoredSourceInfo> sourcesTemp = new ArrayList<OracleTriggerMonitoredSourceInfo>();
        sourcesTemp.addAll(sources);
        _name = name;
        _sources = Collections.unmodifiableList(sourcesTemp);
        _dataSource = dataSource;
        _eventBuffer = eventBuffer;
        _enableTracing = enableTracing;
        _relayInboundStatsCollector = dbusEventsStatisticsCollector;
        _maxScnWriter = maxScnWriter;
        _slowQuerySourceThreshold = slowQuerySourceThreshold;
        _log = Logger.getLogger(getClass().getName() + "." + _name);
        _eventsLog = Logger.getLogger("com.linkedin.databus2.producers.db.events." + _name);
        _chunkingType = chunkingType;
        _txnsPerChunk = txnsPerChunk;
        _scnChunkSize = scnChunkSize;
        _chunkedScnThreshold = chunkedScnThreshold;
        _maxScnDelayMs = maxScnDelayMs;
        _lastquerytime = System.currentTimeMillis();

        // Make sure all logical sources come from the same database schema.
        // Note that Oracle treats quoted names as case-sensitive, but we
        // don't quote ours, so a case-insensitive comparison is fine.
        for (OracleTriggerMonitoredSourceInfo source : sourcesTemp) {
            if (!source.getEventSchema().equalsIgnoreCase(sourcesTemp.get(0).getEventSchema())) {
                throw new IllegalArgumentException("All logical sources must have the same Oracle schema:\n   " +
                        source.getSourceName() + " (id " + source.getSourceId() +
                        ") schema = " + source.getEventSchema() + ";\n   " +
                        sourcesTemp.get(0).getSourceName() + " (id " +
                        sourcesTemp.get(0).getSourceId() + ") schema = " +
                        sourcesTemp.get(0).getEventSchema());
            }
        }
        _selectSchema = sourcesTemp.get(0).getEventSchema() == null ? "" : sourcesTemp.get(0).getEventSchema() + ".";

        // Generate the event queries for each source
        _eventQueriesBySource = new HashMap<Short, String>();
        for (OracleTriggerMonitoredSourceInfo sourceInfo : sources) {
            String eventQuery = generateEventQuery(sourceInfo);
            _log.info("Generated events query. source: " + sourceInfo + " ; eventQuery: " + eventQuery);
            _eventQueriesBySource.put(sourceInfo.getSourceId(), eventQuery);
        }

//        _eventChunkedTxnQueriesBySource = new HashMap<Short, String>();
//        for (OracleTriggerMonitoredSourceInfo sourceInfo : sources) {
//            String eventQuery = generateTxnChunkedQuery(sourceInfo, _selectSchema);
//            _log.info("Generated Chunked Txn events query. source: " + sourceInfo + " ; chunkTxnEventQuery: " + eventQuery);
//            _eventChunkedTxnQueriesBySource.put(sourceInfo.getSourceId(), eventQuery);
//        }

//        _eventChunkedScnQueriesBySource = new HashMap<Short, String>();
//        for (OracleTriggerMonitoredSourceInfo sourceInfo : sources) {
//            String eventQuery = generateScnChunkedQuery(sourceInfo);
//            _log.info("Generated Chunked Scn events query. source: " + sourceInfo + " ; chunkScnEventQuery: " + eventQuery);
//            _eventChunkedScnQueriesBySource.put(sourceInfo.getSourceId(), eventQuery);
//        }
    }

    @Override
    public ReadEventCycleSummary readEventsFromAllSources (long sinceSCN)
            throws DatabusException, EventCreationException, UnsupportedKeyException {
        boolean eventBufferNeedsRollback = true;
        boolean debugEnabled = _log.isDebugEnabled();
        List<EventReaderSummary> summaries = new ArrayList<EventReaderSummary>();
        try {

            long cycleStartTS = System.currentTimeMillis();
            _eventBuffer.startEvents();

            // Open the database connection if it is closed (at start or after an SQLException)
            if (_eventSelectConnection == null || _eventSelectConnection.isClosed()) {
                resetConnections();
            }

            if (sinceSCN <= 0) {
                sinceSCN = 1;
                _log.debug("sinceSCN was <= 0. Overriding with the current max SCN=" + sinceSCN);
                _eventBuffer.setStartSCN(sinceSCN);
                try {
                    DBHelper.commit(_eventSelectConnection);
                } catch (SQLException s) {
                    DBHelper.rollback(_eventSelectConnection);
                }
            }

            //_inChunkingMode = false;

            // Get events for each source
            List<OracleTriggerMonitoredSourceInfo> filteredSources = filterSources(sinceSCN);

            long endOfPeriodScn = EventReaderSummary.NO_EVENTS_SCN;
            for (OracleTriggerMonitoredSourceInfo source : _sources) {
                if (filteredSources.contains(source)) {
                    long startTS = System.currentTimeMillis();
                    EventReaderSummary summary = readEventsFromOneSource(_eventSelectConnection, source, sinceSCN);
                    summaries.add(summary);
                    endOfPeriodScn = Math.max(endOfPeriodScn, summary.getEndOfPeriodSCN());
                    long endTS = System.currentTimeMillis();
                    source.getStatisticsBean().addTimeOfLastDBAccess(endTS);

                    if (_eventsLog.isDebugEnabled() || (_eventsLog.isInfoEnabled() && summary.getNumberOfEvents() > 0)) {
                        _eventsLog.info(summary.toString());
                    }

                    // Update statistics for the source
                    if (summary.getNumberOfEvents() > 0) {
                        source.getStatisticsBean().addEventCycle(summary.getNumberOfEvents(), endTS - startTS,
                                summary.getSizeOfSerializedEvents(),
                                summary.getEndOfPeriodSCN());
                    } else {
                        source.getStatisticsBean().addEmptyEventCycle();
                    }
                } else {
                    source.getStatisticsBean().addEmptyEventCycle();
                }
            }

            _lastSeenEOP = Math.max(_lastSeenEOP, Math.max(endOfPeriodScn, sinceSCN));

            // If we did not read any events in this cycle then get the max SCN from the txlog. This
            // is for slow sources so that the endOfPeriodScn never lags too far behind the max scn
            // in the txlog table.
            long curtime = System.currentTimeMillis();
            if (endOfPeriodScn == EventReaderSummary.NO_EVENTS_SCN) {

                //get new start scn for subsequent calls;
                final long maxTxlogSCN = sinceSCN;
                endOfPeriodScn = Math.max(maxTxlogSCN, sinceSCN);
                _log.info("SlowSourceQueryThreshold hit. currScn : " + sinceSCN +
                        ". Advanced endOfPeriodScn to " + endOfPeriodScn +
                        " and added the event to relay");
                if (debugEnabled) {
                    _log.debug("No events processed. Read max SCN from txlog table for endOfPeriodScn. endOfPeriodScn=" + endOfPeriodScn);
                }

                if (endOfPeriodScn != EventReaderSummary.NO_EVENTS_SCN && endOfPeriodScn > sinceSCN) {
                    // If the SCN has moved forward in the above if/else loop, then
                    _log.info("The endOfPeriodScn has advanced from to " + endOfPeriodScn);
                    _eventBuffer.endEvents(endOfPeriodScn, _relayInboundStatsCollector);
                    eventBufferNeedsRollback = false;
                } else {
                    eventBufferNeedsRollback = true;
                }
            } else {
                //we have appended some events; and a new end of period has been found
                _eventBuffer.endEvents(endOfPeriodScn, _relayInboundStatsCollector);
                if (debugEnabled) {
                    _log.debug("End of events: " + endOfPeriodScn + " windown range= "
                            + _eventBuffer.getMinScn() + "," + _eventBuffer.lastWrittenScn());
                }
                //no need to roll back
                eventBufferNeedsRollback = false;
            }

            //save endOfPeriodScn if new one has been discovered
            if (endOfPeriodScn != EventReaderSummary.NO_EVENTS_SCN) {
                if (null != _maxScnWriter && (endOfPeriodScn != sinceSCN)) {
                    _maxScnWriter.saveMaxScn(endOfPeriodScn);
                }
                for (OracleTriggerMonitoredSourceInfo source : _sources) {
                    //update maxDBScn here
                    source.getStatisticsBean().addMaxDBScn(endOfPeriodScn);
                    source.getStatisticsBean().addTimeOfLastDBAccess(System.currentTimeMillis());
                }
            }
            long cycleEndTS = System.currentTimeMillis();

            //check if we should refresh _catchupTargetMaxScn
//            if (_chunkingType.isChunkingEnabled() &&
//                    (_lastSeenEOP >= _catchupTargetMaxScn) &&
//                    (curtime - _lastMaxScnTime >= _maxScnDelayMs)) {
//                //reset it to -1 so it gets refreshed next time around
//                _catchupTargetMaxScn = -1;
//            }

//            boolean chunkMode = _chunkingType.isChunkingEnabled() &&
//                    (_catchupTargetMaxScn > 0) &&
//                    (_lastSeenEOP < _catchupTargetMaxScn);

//            if (!chunkMode && _inChunkingMode)
//                _log.info("Disabling chunking for sources !!");
//
//            _inChunkingMode = chunkMode;
//
//            if (_inChunkingMode && debugEnabled)
//                _log.debug("_inChunkingMode = true, _catchupTargetMaxScn=" + _catchupTargetMaxScn
//                        + ", endOfPeriodScn=" + endOfPeriodScn + ", _lastSeenEOP=" + _lastSeenEOP);

            ReadEventCycleSummary summary = new ReadEventCycleSummary(_name, summaries,
                    Math.max(endOfPeriodScn, sinceSCN),
                    (cycleEndTS - cycleStartTS));
            // Have to commit the transaction since we are in serializable isolation level
            DBHelper.commit(_eventSelectConnection);

            // Return the event summaries
            return summary;
        } catch (SQLException ex) {
            try {
                DBHelper.rollback(_eventSelectConnection);
            } catch (SQLException s) {
                throw new DatabusException(s.getMessage());
            }

            handleExceptionInReadEvents(ex);
            throw new DatabusException(ex);
        } catch (Exception e) {
            handleExceptionInReadEvents(e);
            throw new DatabusException(e);
        } finally {
            // If events were not processed successfully then eventBufferNeedsRollback will be true.
            // If that happens, rollback the event buffer.
            if (eventBufferNeedsRollback) {
                if (_log.isDebugEnabled()) {
                    _log.debug("Rolling back the event buffer because eventBufferNeedsRollback is true.");
                }
                _eventBuffer.rollbackEvents();
            }
        }
    }

    private void handleExceptionInReadEvents(Exception e) {
        DBHelper.close(_eventSelectConnection);

        _eventSelectConnection = null;

//        // If not in chunking mode, resetting _catchupTargetMaxScn may enforce chunking mode to overcome ORA-1555 if this was the reason for exception
//        _catchupTargetMaxScn = -1;

        _log.error("readEventsFromAllSources exception:" + e.getMessage(), e);
        for (OracleTriggerMonitoredSourceInfo source : _sources) {
            //update maxDBScn here
            source.getStatisticsBean().addError();
        }
    }

    private PreparedStatement createQueryStatement(Connection conn,
                                                   OracleTriggerMonitoredSourceInfo source,
                                                   long sinceScn,
                                                   int currentFetchSize)
            throws SQLException {
        boolean debugEnabled = _log.isDebugEnabled();
        String eventQuery = null;
        ChunkingType type = _chunkingType;

        eventQuery = _eventQueriesBySource.get(source.getSourceId());

        if (debugEnabled) _log.debug("source[" + source.getEventView() + "]: " + eventQuery +
                "; skipInfinityScn=" + source.isSkipInfinityScn() + " ; sinceScn=" + sinceScn);

        PreparedStatement pStmt = conn.prepareStatement(eventQuery);

        pStmt.setFetchSize(currentFetchSize);
        pStmt.setLong(1, sinceScn);

        if (!source.isSkipInfinityScn())
            pStmt.setLong(2, sinceScn);

        return pStmt;
    }

    private EventReaderSummary readEventsFromOneSource(Connection con, OracleTriggerMonitoredSourceInfo source, long sinceScn)
            throws SQLException, UnsupportedKeyException, EventCreationException {
        //boolean useChunking = false; // do not use chunking by default

//        if (_chunkingType.isChunkingEnabled()) {
//            // use the upper bound for chunking if not caught up yet
//            useChunking = (sinceScn + _chunkedScnThreshold <= _catchupTargetMaxScn);
//            if (useChunking && !_inChunkingMode)
//                _log.info("Enabling chunking for sources !!");
//
//            _log.debug("SinceScn :" + sinceScn + ", _ChunkedScnThreshold :"
//                    + _chunkedScnThreshold + ", _catchupTargetMaxScn:" + _catchupTargetMaxScn
//                    + ", useChunking :" + useChunking);
//        }

        //_inChunkingMode = _inChunkingMode || useChunking;

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        long endOfPeriodSCN = EventReaderSummary.NO_EVENTS_SCN;

        int currentFetchSize = DEFAULT_STMT_FETCH_SIZE;
        int numRowsFetched = 0;
        try {
            long startTS = System.currentTimeMillis();
            long totalEventSerializeTime = 0;
            pstmt = createQueryStatement(con, source, sinceScn, currentFetchSize);

            long t = System.currentTimeMillis();
            rs = pstmt.executeQuery();
            long queryExecTime = System.currentTimeMillis() - t;
            long totalEventSize = 0;
            long tsWindowStart = Long.MAX_VALUE;
            long tsWindowEnd = Long.MIN_VALUE;
            while (rs.next()) {
                long scn = rs.getLong(1);
                long timestamp = rs.getTimestamp(2).getTime();
                tsWindowEnd = Math.max(timestamp, tsWindowEnd);
                tsWindowStart = Math.min(timestamp, tsWindowStart);

                // Delegate to the source's EventFactory to create the event and append it to the buffer
                // and then update endOfPeriod to the new max SCN
                long tsStart = System.currentTimeMillis();
                long eventSize = source.getFactory().createAndAppendEvent(scn,
                        timestamp,
                        rs,
                        _eventBuffer,
                        _enableTracing,
                        _relayInboundStatsCollector);
                totalEventSerializeTime += System.currentTimeMillis() - tsStart;
                totalEventSize += eventSize;
                endOfPeriodSCN = Math.max(endOfPeriodSCN, scn);

                // Count the row
                numRowsFetched++;

                // If we are fetching a large number of rows, increase the fetch size until
                // we reach MAX_STMT_FETCH_SIZE
                if (numRowsFetched > currentFetchSize && currentFetchSize != MAX_STMT_FETCH_SIZE) {
                    currentFetchSize = Math.min(2 * currentFetchSize, MAX_STMT_FETCH_SIZE);
                    pstmt.setFetchSize(currentFetchSize);
                }
            }
            long endTS = System.currentTimeMillis();

//            if (_inChunkingMode && (ChunkingType.TXN_CHUNKING == _chunkingType)) {
//                _log.info("txn chunking mode: since=" + sinceScn + " eop=" + endOfPeriodSCN);
//            }

            // Build the event summary and return
            EventReaderSummary summary = new EventReaderSummary(source.getSourceId(), source.getSourceName(),
                    endOfPeriodSCN, numRowsFetched,
                    totalEventSize, (endTS - startTS), totalEventSerializeTime, tsWindowStart, tsWindowEnd, queryExecTime);
            return summary;
        } finally {
            DBHelper.close(rs, pstmt, null);
        }
    }

    /**
     * Filtering is disabled !!
     *
     * @param startSCN
     * @return
     * @throws DatabusException
     */
    List<OracleTriggerMonitoredSourceInfo> filterSources(long startSCN)
            throws DatabusException {
        return _sources;
    }

    public void resetConnections()
            throws SQLException {
        _eventSelectConnection = _dataSource.getConnection();
        _log.info("JDBC Version is: " + _eventSelectConnection.getMetaData().getDriverVersion());
        _eventSelectConnection.setAutoCommit(false);
        _eventSelectConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }


    String generateEventQuery(OracleTriggerMonitoredSourceInfo sourceInfo) {
        String sql = generateEventQuery(sourceInfo, _selectSchema);

        _log.info("EventQuery=" + sql);
        return sql;
    }

    static String generateEventQuery(OracleTriggerMonitoredSourceInfo sourceInfo, String selectSchema) {
        StringJoiner pks = new StringJoiner(" and ");
        List<String> primaryKeys = sourceInfo.getPrimaryKeys();
        for (String primaryKey : primaryKeys) {
            pks.add("src." + primaryKey + "=j." + primaryKey + " ");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        sql.append("src." + NR_OGG_SRC_TRNS_CSN + " as scn, ");
        sql.append("src." + TS_OGG_SRC_TRNS_COMMIT + " as event_timestamp, ");
        sql.append("src.* ");
        sql.append("FROM ");
        sql.append(selectSchema).append(sourceInfo.getEventView()).append(" src, ");
        sql.append(sourceInfo.getJournalTable() + " j ");
        sql.append("WHERE ");
        if (pks.length() > 0) {
            sql.append(pks.toString() + " and ");
        }
        sql.append("src." + NR_OGG_SRC_TRNS_CSN + " > ? and src."  + NR_OGG_SRC_TRNS_CSN + " < 9999999999999999999999999999 ");
        sql.append("ORDER BY event_timestamp");

        return sql.toString();
    }


//    /**
//     * TXN Chunking Query
//     * <p/>
//     * Query to select a chunk of rows by txn ids.
//     * This query goes hand-in-hand with getMaxScnSkippedForTxnChunked.
//     * You would need to change the  getMaxScnSkippedForTxnChunked query when you change this query
//     *
//     * @param sourceInfo Source Info for which chunk query is needed
//     * @return
//     */
//    static String generateTxnChunkedQuery(OracleTriggerMonitoredSourceInfo sourceInfo, String selectSchema) {
//        StringBuilder sql = new StringBuilder();
//
//        sql.append("SELECT scn, event_timestamp, src.* ");
//        sql.append("FROM ").append(selectSchema).append("sy$").append(sourceInfo.getEventView()).append(" src, ");
//        sql.append("( SELECT ");
//        String hints = sourceInfo.getEventTxnChunkedQueryHints();
//        sql.append(hints).append(" "); // hint for oracle
//
//        sql.append(selectSchema + "sync_core.getScn(tx.scn, tx.ora_rowscn) scn, tx.ts event_timestamp, ");
//        sql.append("tx.txn, row_number() OVER (ORDER BY TX.SCN) r ");
//        sql.append("FROM ").append(selectSchema + "sy$txlog tx ");
//        sql.append("WHERE tx.scn > ? AND tx.ora_rowscn > ? AND tx.scn < 9999999999999999999999999999) t ");
//        sql.append("WHERE src.txn = t.txn AND r<= ? ");
//        sql.append("ORDER BY r ");
//
//        return sql.toString();
//    }


//    static String generateScnChunkedQuery(OracleTriggerMonitoredSourceInfo sourceInfo) {
//        StringBuilder sql = new StringBuilder();
//        sql.append("SELECT ");
//        String hints = sourceInfo.getEventScnChunkedQueryHints();
//        sql.append(hints).append(" "); // hint for oracle
//        sql.append("sync_core.getScn(tx.scn, tx.ora_rowscn) scn, tx.ts event_timestamp, src.* ");
//        sql.append("FROM sy$").append(sourceInfo.getEventView()).append(" src, sy$txlog tx ");
//        sql.append("WHERE src.txn=tx.txn AND tx.scn > ? AND tx.ora_rowscn > ? AND ");
//        sql.append(" tx.ora_rowscn <= ?");
//
//        return sql.toString();
//    }


//    private long getMaxScnSkippedForTxnChunked(Connection db, long currScn, long txnsPerChunk)
//            throws SQLException {
//        // Generate the PreparedStatement and cache it in a member variable.
//        // Owned by the object, hence do not close it
//        generateMaxScnSkippedForTxnChunkedQuery(db);
//        PreparedStatement stmt = _txnChunkJumpScnStmt;
//        long retScn = currScn;
//        if (_log.isDebugEnabled())
//            _log.debug("Executing MaxScnSkippedForTxnChunked query with currScn :" + currScn + " and txnsPerChunk :" + txnsPerChunk);
//        ResultSet rs = null;
//        try {
//            stmt.setLong(1, currScn);
//            stmt.setLong(2, txnsPerChunk);
//            rs = stmt.executeQuery();
//
//            if (rs.next()) {
//                long scnFromQuery = rs.getLong(1);
//                if (scnFromQuery == 0) {
//                    if (_log.isDebugEnabled())
//                        _log.debug("Ignoring SCN obtained from txn chunked query as there may be no update. currScn = " + currScn + " scnFromQuery = " + scnFromQuery);
//                } else if (scnFromQuery < currScn) {
//                    _log.error("ERROR: SCN obtained from txn chunked query is less than currScn. currScn = " + currScn + " scnFromQuery = " + scnFromQuery);
//                } else {
//                    retScn = rs.getLong(1);
//                }
//            }
//        } finally {
//            DBHelper.close(rs);
//        }
//        return retScn;
//    }


//    /**
//     * Max SCN Query for TXN Chunking.
//     * <p/>
//     * When no rows are returned for a given chunk of txns, this query is used to find the beginning of the next batch.
//     * <p/>
//     * This query goes hand-in-hand with generateTxnChunkedQuery.
//     * You would need to change this query when you change generateTxnChunkedQuery query.
//     *
//     * @param db Connection instance
//     * @return None
//     */
//    private void generateMaxScnSkippedForTxnChunkedQuery(Connection db)
//            throws SQLException {
//        if (null == _txnChunkJumpScnStmt) {
//            StringBuilder sql = new StringBuilder();
//
//            sql.append("SELECT max(t.scn) from (");
//            sql.append("select /*+ index(tx) */ tx.scn, row_number() OVER (ORDER BY tx.scn) r FROM ");
//            sql.append(_selectSchema + "sy$txlog tx ");
//            sql.append("WHERE tx.scn >  ? AND tx.scn < 9999999999999999999999999999) t ");
//            sql.append("WHERE r <= ?");
//
//            _txnChunkJumpScnStmt = db.prepareStatement(sql.toString());
//        }
//
//        return;
//    }

    @Override
    public List<OracleTriggerMonitoredSourceInfo> getSources() {
        return _sources;
    }

    public void close() {
        if (null != _eventSelectConnection) DBHelper.close(_eventSelectConnection);
    }

//    public void setCatchupTargetMaxScn(long catchupTargetMaxScn) {
//        _catchupTargetMaxScn = catchupTargetMaxScn;
//    }
}
