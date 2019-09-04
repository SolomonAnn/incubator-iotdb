/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public abstract class AbstractMemTable implements IMemTable {

  private long version;

  private List<Modification> modifications = new ArrayList<>();

  private final Map<Long, Map<Long, IWritableMemChunk>> memTableMap;

  private long memSize = 0;

  public AbstractMemTable() {
    this.memTableMap = new HashMap<>();
  }

  public AbstractMemTable(Map<Long, Map<Long, IWritableMemChunk>> memTableMap) {
    this.memTableMap = memTableMap;
  }

  @Override
  public Map<Long, Map<Long, IWritableMemChunk>> getMemTableMap() {
    return memTableMap;
  }

  /**
   * check whether the given seriesPath is within this memtable.
   *
   * @return true if seriesPath is within this memtable
   */
  private boolean checkPath(String deviceId, String measurement) {
    return memTableMap.containsKey(deviceId) && memTableMap.get(deviceId).containsKey(measurement);
  }

  private IWritableMemChunk createIfNotExistAndGet(String device, String measurement,
      TSDataType dataType) throws PathErrorException {
    Long deviceId = MManager.getInstance().getDeviceIdByPath(device);
    Long measurementId = MManager.getInstance().getMeasurementIdByPath(measurement);
    if (!memTableMap.containsKey(deviceId)) {
      memTableMap.put(deviceId, new HashMap<>());
    }
    Map<Long, IWritableMemChunk> memSeries = memTableMap.get(deviceId);
    if (!memSeries.containsKey(measurementId)) {
      memSeries.put(measurementId, genMemSeries(dataType));
    }
    return memSeries.get(measurementId);
  }

  protected abstract IWritableMemChunk genMemSeries(TSDataType dataType);


  @Override
  public void insert(InsertPlan insertPlan) throws PathErrorException {
    for (int i = 0; i < insertPlan.getValues().length; i++) {
      write(insertPlan.getDevice(), insertPlan.getMeasurements()[i],
          insertPlan.getDataTypes()[i], insertPlan.getTime(), insertPlan.getValues()[i]);
    }
    long recordSizeInByte = MemUtils.getRecordSize(insertPlan);
    memSize += recordSizeInByte;
  }

  @Override
  public void insertBatch(BatchInsertPlan batchInsertPlan, List<Integer> indexes) throws PathErrorException {
    write(batchInsertPlan, indexes);
    long recordSizeInByte = MemUtils.getRecordSize(batchInsertPlan);
    memSize += recordSizeInByte;
  }


  @Override
  public void write(String device, String measurement, TSDataType dataType, long insertTime,
      String insertValue) throws PathErrorException {
    IWritableMemChunk memSeries = createIfNotExistAndGet(device, measurement, dataType);
    memSeries.write(insertTime, insertValue);
  }

  @Override
  public void write(BatchInsertPlan batchInsertPlan, List<Integer> indexes) throws PathErrorException {
    for (int i = 0; i < batchInsertPlan.getMeasurements().length; i++) {
      IWritableMemChunk memSeries = createIfNotExistAndGet(batchInsertPlan.getDevice(),
          batchInsertPlan.getMeasurements()[i], batchInsertPlan.getDataTypes()[i]);
      memSeries.write(batchInsertPlan.getTimes(), batchInsertPlan.getColumns()[i], batchInsertPlan.getDataTypes()[i], indexes);
    }
  }


  @Override
  public long size() {
    long sum = 0;
    for (Map<Long, IWritableMemChunk> seriesMap : memTableMap.values()) {
      for (IWritableMemChunk writableMemChunk : seriesMap.values()) {
        sum += writableMemChunk.count();
      }
    }
    return sum;
  }

  @Override
  public long memSize() {
    return memSize;
  }

  @Override
  public void clear() {
    memTableMap.clear();
    modifications.clear();
    memSize = 0;
  }

  @Override
  public boolean isEmpty() {
    return memTableMap.isEmpty();
  }

  @Override
  public ReadOnlyMemChunk query(String deviceId, String measurement, TSDataType dataType,
      Map<String, String> props) {
    TimeValuePairSorter sorter;
    if (!checkPath(deviceId, measurement)) {
      return null;
    } else {
      long undeletedTime = findUndeletedTime(deviceId, measurement);
      IWritableMemChunk memChunk = memTableMap.get(deviceId).get(measurement);
      IWritableMemChunk chunkCopy = new WritableMemChunk(dataType, memChunk.getTVList().clone());
      chunkCopy.setTimeOffset(undeletedTime);
      sorter = chunkCopy;
    }
    return new ReadOnlyMemChunk(dataType, sorter, props);
  }


  private long findUndeletedTime(String deviceId, String measurement) {
    long undeletedTime = Long.MIN_VALUE;
    for (Modification modification : modifications) {
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getDevice().equals(deviceId) && deletion.getMeasurement().equals(measurement)
            && deletion.getTimestamp() > undeletedTime) {
          undeletedTime = deletion.getTimestamp();
        }
      }
    }
    return undeletedTime + 1;
  }

  @Override
  public void delete(String deviceId, String measurementId, long timestamp) {
    Map<Long, IWritableMemChunk> deviceMap = memTableMap.get(deviceId);
    if (deviceMap != null) {
      IWritableMemChunk chunk = deviceMap.get(measurementId);
      if (chunk == null) {
        return;
      }
      chunk.delete(timestamp);
    }
  }

  @Override
  public void delete(Deletion deletion) {
    this.modifications.add(deletion);
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public long getVersion() {
    return version;
  }

  @Override
  public void release() {
    for (Entry<Long, Map<Long, IWritableMemChunk>> entry: memTableMap.entrySet()) {
      for (Entry<Long, IWritableMemChunk> subEntry: entry.getValue().entrySet()) {
        TVListAllocator.getInstance().release(subEntry.getValue().getTVList());
      }
    }
  }
}
