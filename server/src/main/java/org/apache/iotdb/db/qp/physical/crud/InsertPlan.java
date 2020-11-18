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
package org.apache.iotdb.db.qp.physical.crud;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.TSRecord;

public class InsertPlan extends PhysicalPlan {

  private String devicePath;
  private String[] measurementPaths;
  private TSDataType[] dataTypes;
  private String[] values;
  private long time;

  public InsertPlan() {
    super(false, OperatorType.INSERT);
  }

  public InsertPlan(String devicePath, long insertTime, String measurementPath, String insertValue) {
    super(false, OperatorType.INSERT);
    this.time = insertTime;
    this.devicePath = devicePath;
    this.measurementPaths = new String[] {measurementPath};
    this.values = new String[] {insertValue};
  }

  public InsertPlan(TSRecord tsRecord) {
    super(false, OperatorType.INSERT);
    this.devicePath = tsRecord.devicePath;
    this.time = tsRecord.time;
    this.measurementPaths = new String[tsRecord.dataPointList.size()];
    this.dataTypes = new TSDataType[tsRecord.dataPointList.size()];
    this.values = new String[tsRecord.dataPointList.size()];
    for (int i = 0; i < tsRecord.dataPointList.size(); i++) {
      measurementPaths[i] = tsRecord.dataPointList.get(i).getMeasurementPath();
      dataTypes[i] = tsRecord.dataPointList.get(i).getType();
      values[i] = tsRecord.dataPointList.get(i).getValue().toString();
    }
  }

  public InsertPlan(String devicePath, long insertTime, String[] measurementList, String[] insertValues) {
    super(false, Operator.OperatorType.INSERT);
    this.time = insertTime;
    this.devicePath = devicePath;
    this.measurementPaths = measurementList;
    this.values = insertValues;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  @Override
  public List<Path> getPaths() {
    List<Path> ret = new ArrayList<>();

    for (String m : measurementPaths) {
      ret.add(new Path(devicePath, m));
    }
    return ret;
  }

  public String getDevicePath() {
    return this.devicePath;
  }

  public void setDevicePath(String devicePath) {
    this.devicePath = devicePath;
  }

  public String[] getMeasurementPaths() {
    return this.measurementPaths;
  }

  public void setMeasurementPaths(String[] measurementPaths) {
    this.measurementPaths = measurementPaths;
  }

  public String[] getValues() {
    return this.values;
  }

  public void setValues(String[] values) {
    this.values = values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InsertPlan that = (InsertPlan) o;
    return time == that.time && Objects.equals(devicePath, that.devicePath)
        && Arrays.equals(measurementPaths, that.measurementPaths)
        && Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(devicePath, time);
  }

  @Override
  public void serializeTo(ByteBuffer buffer) {
    int type = PhysicalPlanType.INSERT.ordinal();
    buffer.put((byte) type);
    buffer.putLong(time);

    putString(buffer, devicePath);

    buffer.putInt(measurementPaths.length);
    for (String m : measurementPaths) {
      putString(buffer, m);
    }

    buffer.putInt(values.length);
    for (String m : values) {
      putString(buffer, m);
    }
  }

  @Override
  public void deserializeFrom(ByteBuffer buffer) {
    this.time = buffer.getLong();
    this.devicePath = readString(buffer);

    int measurementPathsSize = buffer.getInt();
    this.measurementPaths = new String[measurementPathsSize];
    for (int i = 0; i < measurementPathsSize; i++) {
      measurementPaths[i] = readString(buffer);
    }

    int valueSize = buffer.getInt();
    this.values = new String[valueSize];
    for (int i = 0; i < valueSize; i++) {
      values[i] = readString(buffer);
    }
  }

  @Override
  public String toString() {
    return "devicePath: " + devicePath + ", time: " + time;
  }
}
