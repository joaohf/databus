/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.databus2.core.container.monitoring.events;
/*
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
 */


@SuppressWarnings("all")
/** Container statistics */
public class ContainerStatsEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ContainerStatsEvent\",\"namespace\":\"com.linkedin.databus2.core.container.monitoring.events\",\"fields\":[{\"name\":\"containerId\",\"type\":\"int\",\"doc\":\"The id of the container that generated the event\"},{\"name\":\"timestampLastResetMs\",\"type\":\"long\",\"doc\":\"unix timestamp of the last reset() call\"},{\"name\":\"ioThreadRate\",\"type\":\"int\",\"doc\":\"number of active (running) I/O threads\"},{\"name\":\"ioThreadMax\",\"type\":\"int\",\"doc\":\"max number of I/O threads seen during a metrics update\"},{\"name\":\"ioTaskCount\",\"type\":\"long\",\"doc\":\"number of scheduled I/O tasks since startup (approximate)\"},{\"name\":\"ioTaskMax\",\"type\":\"int\",\"doc\":\"max number of scheduled I/O tasks in a metric-update interval\"},{\"name\":\"ioTaskQueueSize\",\"type\":\"int\",\"doc\":\"number of I/O tasks currently waiting in queue for a thread\"},{\"name\":\"workerThreadRate\",\"type\":\"int\",\"doc\":\"number of active (running) worker threads\"},{\"name\":\"workerThreadMax\",\"type\":\"int\",\"doc\":\"max number of worker threads seen during a metrics update\"},{\"name\":\"workerTaskCount\",\"type\":\"long\",\"doc\":\"number scheduled worker tasks since startup (approximate)\"},{\"name\":\"workerTaskMax\",\"type\":\"int\",\"doc\":\"max number of scheduled worker tasks in a metric-update interval\"},{\"name\":\"workerTaskQueueSize\",\"type\":\"int\",\"doc\":\"number of worker tasks currently waiting in queue for a thread\"},{\"name\":\"errorCount\",\"type\":\"long\",\"doc\":\"total number of errors\"},{\"name\":\"errorUncaughtCount\",\"type\":\"long\",\"doc\":\"number of uncaught errors\"},{\"name\":\"errorRequestProcessingCount\",\"type\":\"long\",\"doc\":\"number of request processing errors\"}]}");
  /** The id of the container that generated the event */
  public int containerId;
  /** unix timestamp of the last reset() call */
  public long timestampLastResetMs;
  /** number of active (running) I/O threads */
  public int ioThreadRate;
  /** max number of I/O threads seen during a metrics update */
  public int ioThreadMax;
  /** number of scheduled I/O tasks since startup (approximate) */
  public long ioTaskCount;
  /** max number of scheduled I/O tasks in a metric-update interval */
  public int ioTaskMax;
  /** number of I/O tasks currently waiting in queue for a thread */
  public int ioTaskQueueSize;
  /** number of active (running) worker threads */
  public int workerThreadRate;
  /** max number of worker threads seen during a metrics update */
  public int workerThreadMax;
  /** number scheduled worker tasks since startup (approximate) */
  public long workerTaskCount;
  /** max number of scheduled worker tasks in a metric-update interval */
  public int workerTaskMax;
  /** number of worker tasks currently waiting in queue for a thread */
  public int workerTaskQueueSize;
  /** total number of errors */
  public long errorCount;
  /** number of uncaught errors */
  public long errorUncaughtCount;
  /** number of request processing errors */
  public long errorRequestProcessingCount;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return containerId;
    case 1: return timestampLastResetMs;
    case 2: return ioThreadRate;
    case 3: return ioThreadMax;
    case 4: return ioTaskCount;
    case 5: return ioTaskMax;
    case 6: return ioTaskQueueSize;
    case 7: return workerThreadRate;
    case 8: return workerThreadMax;
    case 9: return workerTaskCount;
    case 10: return workerTaskMax;
    case 11: return workerTaskQueueSize;
    case 12: return errorCount;
    case 13: return errorUncaughtCount;
    case 14: return errorRequestProcessingCount;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: containerId = (java.lang.Integer)value$; break;
    case 1: timestampLastResetMs = (java.lang.Long)value$; break;
    case 2: ioThreadRate = (java.lang.Integer)value$; break;
    case 3: ioThreadMax = (java.lang.Integer)value$; break;
    case 4: ioTaskCount = (java.lang.Long)value$; break;
    case 5: ioTaskMax = (java.lang.Integer)value$; break;
    case 6: ioTaskQueueSize = (java.lang.Integer)value$; break;
    case 7: workerThreadRate = (java.lang.Integer)value$; break;
    case 8: workerThreadMax = (java.lang.Integer)value$; break;
    case 9: workerTaskCount = (java.lang.Long)value$; break;
    case 10: workerTaskMax = (java.lang.Integer)value$; break;
    case 11: workerTaskQueueSize = (java.lang.Integer)value$; break;
    case 12: errorCount = (java.lang.Long)value$; break;
    case 13: errorUncaughtCount = (java.lang.Long)value$; break;
    case 14: errorRequestProcessingCount = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
