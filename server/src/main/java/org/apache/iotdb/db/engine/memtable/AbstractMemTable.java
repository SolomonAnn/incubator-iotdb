/*
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
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

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
  private boolean checkPath(String devicePath, String measurementPath) {
    return memTableMap.containsKey(devicePath) && memTableMap.get(devicePath).containsKey(measurementPath);
  }

  private IWritableMemChunk createIfNotExistAndGet(String devicePath, String measurementPath,
      TSDataType dataType) throws PathErrorException, StorageGroupException {
    Long deviceId = MManager.getInstance().getDeviceIdByPath(devicePath);
    Long measurementId = MManager.getInstance().getMeasurementIdByPath(devicePath, measurementPath);
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
  public void insert(InsertPlan insertPlan) throws PathErrorException, QueryProcessorException {
    try {
      for (int i = 0; i < insertPlan.getValues().length; i++) {
        write(insertPlan.getDevicePath(), insertPlan.getMeasurementPaths()[i],
            insertPlan.getDataTypes()[i], insertPlan.getTime(), insertPlan.getValues()[i]);
      }
      long recordSizeInByte = MemUtils.getRecordSize(insertPlan);
      memSize += recordSizeInByte;
    } catch (RuntimeException | StorageGroupException e) {
      throw new QueryProcessorException(e);
    }
  }

  @Override
  public void insertBatch(BatchInsertPlan batchInsertPlan, List<Integer> indexes)
      throws PathErrorException, QueryProcessorException {
    try {
      write(batchInsertPlan, indexes);
      long recordSizeInByte = MemUtils.getRecordSize(batchInsertPlan);
      memSize += recordSizeInByte;
    } catch (RuntimeException | StorageGroupException e) {
      throw new QueryProcessorException(e);
    }
  }


  @Override
  public void write(String devicePath, String measurementPath, TSDataType dataType, long insertTime,
      String insertValue) throws PathErrorException, StorageGroupException {
    IWritableMemChunk memSeries = createIfNotExistAndGet(devicePath, measurementPath, dataType);
    memSeries.write(insertTime, insertValue);
  }

  @Override
  public void write(BatchInsertPlan batchInsertPlan, List<Integer> indexes)
      throws PathErrorException, StorageGroupException {
    for (int i = 0; i < batchInsertPlan.getMeasurementPaths().length; i++) {
      IWritableMemChunk memSeries = createIfNotExistAndGet(batchInsertPlan.getDevicePath(),
          batchInsertPlan.getMeasurementPaths()[i], batchInsertPlan.getDataTypes()[i]);
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
  public ReadOnlyMemChunk query(String devicePath, String measurementPath, TSDataType dataType,
      Map<String, String> props, long timeLowerBound) {
    TimeValuePairSorter sorter;
    if (!checkPath(devicePath, measurementPath)) {
      return null;
    } else {
      long undeletedTime = findUndeletedTime(devicePath, measurementPath, timeLowerBound);
      IWritableMemChunk memChunk = memTableMap.get(devicePath).get(measurementPath);
      IWritableMemChunk chunkCopy = new WritableMemChunk(dataType, memChunk.getTVList().clone());
      chunkCopy.setTimeOffset(undeletedTime);
      sorter = chunkCopy;
    }
    return new ReadOnlyMemChunk(dataType, sorter, props);
  }


  private long findUndeletedTime(String devicePath, String measurementPath, long timeLowerBound) {
    long undeletedTime = Long.MIN_VALUE;
    for (Modification modification : modifications) {
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getDevicePath().equals(devicePath) && deletion.getMeasurementPath().equals(measurementPath)
            && deletion.getTimestamp() > undeletedTime) {
          undeletedTime = deletion.getTimestamp();
        }
      }
    }
    return Math.max(undeletedTime + 1, timeLowerBound);
  }

  @Override
  public void delete(String devicePath, String measurementPath, long timestamp) {
    Map<Long, IWritableMemChunk> deviceMap = memTableMap.get(devicePath);
    if (deviceMap != null) {
      IWritableMemChunk chunk = deviceMap.get(measurementPath);
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
