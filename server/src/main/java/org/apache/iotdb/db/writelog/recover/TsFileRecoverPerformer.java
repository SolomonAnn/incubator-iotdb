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

package org.apache.iotdb.db.writelog.recover;

import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.RESOURCE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TsFileRecoverPerformer recovers a SeqTsFile to correct status, redoes the WALs since last crash
 * and removes the redone logs.
 */
public class TsFileRecoverPerformer {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRecoverPerformer.class);

  private String insertFilePath;
  private String logNodePrefix;
  private Schema schema;
  private VersionController versionController;
  private LogReplayer logReplayer;
  private TsFileResource tsFileResource;
  private boolean acceptUnseq;

  public TsFileRecoverPerformer(String logNodePrefix,
      Schema schema, VersionController versionController,
      TsFileResource currentTsFileResource, boolean acceptUnseq) {
    this.insertFilePath = currentTsFileResource.getFile().getPath();
    this.logNodePrefix = logNodePrefix;
    this.schema = schema;
    this.versionController = versionController;
    this.tsFileResource = currentTsFileResource;
    this.acceptUnseq = acceptUnseq;
  }

  /**
   * 1. recover the TsFile by RestorableTsFileIOWriter and truncate the file to remaining corrected
   * data 2. redo the WALs to recover unpersisted data 3. flush and close the file 4. clean WALs
   */
  public void recover() throws ProcessorException, PathErrorException {

    IMemTable recoverMemTable = new PrimitiveMemTable();
    this.logReplayer = new LogReplayer(logNodePrefix, insertFilePath, tsFileResource.getModFile(),
        versionController,
        tsFileResource, schema, recoverMemTable, acceptUnseq);
    File insertFile = new File(insertFilePath);
    if (!insertFile.exists()) {
      logger.error("TsFile {} is missing, will skip its recovery.", insertFilePath);
      return;
    }
    // remove corrupted part of the TsFile
    RestorableTsFileIOWriter restorableTsFileIOWriter;
    try {
      restorableTsFileIOWriter = new RestorableTsFileIOWriter(insertFile);
    } catch (IOException e) {
      throw new ProcessorException(e);
    }

    if (!restorableTsFileIOWriter.hasCrashed() && !restorableTsFileIOWriter.canWrite()) {
      // tsfile is complete
      try {
        if (tsFileResource.fileExists()) {
          // .resource file exists, deserialize it
          recoverResourceFromFile();
        } else {
          // .resource file does not exist, read file metadata and recover tsfile resource
          try (TsFileSequenceReader reader = new TsFileSequenceReader(
              tsFileResource.getFile().getAbsolutePath())) {
            TsFileMetaData metaData = reader.readFileMetadata();
            List<TsDeviceMetadataIndex> deviceMetadataIndexList = new ArrayList<>(
                metaData.getDeviceMap().values());
            for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
              TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
              List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata
                  .getChunkGroupMetaDataList();
              for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
                for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
                  Long deviceId = MManager.getInstance().getDeviceIdByPath(chunkGroupMetaData.getDeviceID());
                  tsFileResource.updateStartTime(deviceId, chunkMetaData.getStartTime());
                  tsFileResource.updateEndTime(deviceId, chunkMetaData.getEndTime());
                }
              }
            }
          }
          // write .resource file
          tsFileResource.serialize();
        }
        return;
      } catch (IOException e) {
        throw new ProcessorException("recover the resource file failed: " + insertFilePath
            + RESOURCE_SUFFIX, e);
      }
    } else {
      // due to failure, the last ChunkGroup may contain the same data as the WALs, so the time
      // map must be updated first to avoid duplicated insertion
      recoverResourceFromWriter(restorableTsFileIOWriter);
    }

    // redo logs
    redoLogs(restorableTsFileIOWriter);

    // clean logs
    try {
      MultiFileLogNodeManager.getInstance()
          .deleteNode(logNodePrefix + new File(insertFilePath).getName());
    } catch (IOException e) {
      throw new ProcessorException(e);
    }
  }

  private void recoverResourceFromFile() throws IOException, PathErrorException {
    try {
      tsFileResource.deSerialize();
    } catch (IOException e) {
      logger.warn("Cannot deserialize TsFileResource {}, construct it using "
          + "TsFileSequenceReader", tsFileResource.getFile(), e);
      recoverResourceFromReader();
    }
  }


  private void recoverResourceFromReader() throws IOException, PathErrorException {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(tsFileResource.getFile().getAbsolutePath(), false)) {
      TsFileMetaData metaData = reader.readFileMetadata();
      List<TsDeviceMetadataIndex> deviceMetadataIndexList = new ArrayList<>(
          metaData.getDeviceMap().values());
      for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
        TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
        for (ChunkGroupMetaData chunkGroupMetaData : deviceMetadata
            .getChunkGroupMetaDataList()) {
          for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
            Long deviceId = MManager.getInstance().getDeviceIdByPath(chunkGroupMetaData.getDeviceID());
            tsFileResource.updateStartTime(deviceId, chunkMetaData.getStartTime());
            tsFileResource.updateEndTime(deviceId, chunkMetaData.getEndTime());
          }
        }
      }
    }
    // write .resource file
    tsFileResource.serialize();
  }

  private void recoverResourceFromWriter(RestorableTsFileIOWriter restorableTsFileIOWriter) throws PathErrorException {
    for (ChunkGroupMetaData chunkGroupMetaData : restorableTsFileIOWriter
        .getChunkGroupMetaDatas()) {
      for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
        Long deviceId = MManager.getInstance().getDeviceIdByPath(chunkGroupMetaData.getDeviceID());
        tsFileResource.updateStartTime(deviceId, chunkMetaData.getStartTime());
        tsFileResource.updateEndTime(deviceId, chunkMetaData.getEndTime());
      }
    }
  }

  private void redoLogs(RestorableTsFileIOWriter restorableTsFileIOWriter)
      throws ProcessorException, PathErrorException {
    IMemTable recoverMemTable = new PrimitiveMemTable();
    this.logReplayer = new LogReplayer(logNodePrefix, insertFilePath, tsFileResource.getModFile(),
        versionController,
        tsFileResource, schema, recoverMemTable, acceptUnseq);
    logReplayer.replayLogs();
    try {
      if (!recoverMemTable.isEmpty()) {
        // flush logs

        MemTableFlushTask tableFlushTask = new MemTableFlushTask(recoverMemTable, schema,
            restorableTsFileIOWriter,
            logNodePrefix);
        tableFlushTask.syncFlushMemTable();
      }
      // close file
      restorableTsFileIOWriter.endFile(schema);
      tsFileResource.serialize();
    } catch (ExecutionException | InterruptedException | IOException e) {
      Thread.currentThread().interrupt();
      throw new ProcessorException(e);
    }
  }

}
