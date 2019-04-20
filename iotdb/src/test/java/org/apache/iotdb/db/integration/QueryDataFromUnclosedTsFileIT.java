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

package org.apache.iotdb.db.integration;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;

import java.io.IOException;
import java.util.Collections;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.EngineQueryRouter;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryDataFromUnclosedTsFileIT {

  long bufferWriteFileSize;
  FileNodeManager sgManager;
  MManager mManager;
  EngineQueryRouter queryManager;
  @Before
  public void setUp() throws IOException, FileNodeManagerException, StartupException {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();
    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignJobId();
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
    bufferWriteFileSize = IoTDBDescriptor.getInstance().getConfig().getBufferwriteFileSizeThreshold();
    //IoTDBDescriptor.getInstance().getConfig().setBufferwriteFileSizeThreshold(100);
    sgManager  = FileNodeManager.getInstance();
    mManager = MManager.getInstance();
    queryManager = new EngineQueryRouter();
  }

  @After
  public void tearDown() throws FileNodeManagerException, IOException {
    IoTDBDescriptor.getInstance().getConfig().setBufferwriteFileSizeThreshold(bufferWriteFileSize);;

    EnvironmentUtils.cleanEnv();

  }

  @Test
  public void test()
      throws FileNodeManagerException, IOException, PathErrorException, MetadataArgsErrorException {
    mManager.setStorageLevelToMTree("root.test");
    mManager.addPathToMTree("root.test.d1.s1",  TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY, Collections.emptyMap());
    mManager.addPathToMTree("root.test.d2.s1",  TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY, Collections.emptyMap());
    sgManager.addTimeSeries(new Path("root.test.d1", "s1"), TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY, Collections
        .emptyMap());
    sgManager.addTimeSeries(new Path("root.test.d2", "s1"), TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY, Collections
        .emptyMap());
    long time = System.currentTimeMillis();
    for (int i=0; i < 20000; i++) {
      sgManager.insert(new TSRecord(i, "root.test.d1").addTuple(new IntDataPoint("s1", i)), false);
      sgManager.insert(new TSRecord(i, "root.test.d2").addTuple(new IntDataPoint("s1", i)), false);
    }
    QueryExpression qe = QueryExpression
        .create(Collections.singletonList(new Path("root.test.d1", "s1")), null);
    QueryDataSet result = queryManager.query(qe, TEST_QUERY_CONTEXT);
    while (result.hasNext()) {
      result.next();
      //System.out.println(record.getTimestamp() + "," + record.getFields().get(0).getIntV());
    }

  }

}
