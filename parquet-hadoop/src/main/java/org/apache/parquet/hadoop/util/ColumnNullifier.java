/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.parquet.hadoop.util;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnNullifier {

  private final int pageBufferSize = ParquetProperties.DEFAULT_PAGE_SIZE * 2;
  private byte[] pageBuffer;

  public ColumnNullifier() {
    this.pageBuffer = new byte[pageBufferSize];
  }

  public void processBlocks(TransParquetFileReader reader, ParquetFileWriter writer, ParquetMetadata meta, MessageType schema,
                             String createdBy, CompressionCodecName codecName) throws IOException {
    int blockIndex = 0;
    PageReadStore store = reader.readNextRowGroup();
    while (store != null) {
      writer.startBlock(store.getRowCount());
      BlockMetaData blockMetaData = meta.getBlocks().get(blockIndex);
      List<ColumnChunkMetaData> columnsInOrder = blockMetaData.getColumns();
      Map<ColumnPath, ColumnDescriptor> descriptorsMap = schema.getColumns().stream().collect(
        Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
      for (int i = 0; i < columnsInOrder.size(); i += 1) {
        ColumnChunkMetaData chunk = columnsInOrder.get(i);
        ColumnReadStoreImpl crstore = new ColumnReadStoreImpl(store, new DummyGroupConverter(), schema, createdBy);
        ColumnDescriptor columnDescriptor = descriptorsMap.get(chunk.getPath());
        writer.startColumn(columnDescriptor, crstore.getColumnReader(columnDescriptor).getTotalValueCount(), codecName);
        processChunk(columnDescriptor, writer, chunk, schema);
        writer.endColumn();
      }
      writer.endBlock();
      store = reader.readNextRowGroup();
      blockIndex++;
    }
  }

  private void processChunk(ColumnDescriptor columnDescriptor, ParquetFileWriter writer, ColumnChunkMetaData chunk,
                            MessageType schema) throws IOException {

    long totalChunkValues = chunk.getValueCount();
    ParquetProperties props = ParquetProperties.builder().build(); // TODO: don't hardcoded
    ColumnWriter columnWriter = props.newColumnWriteStore(schema, new DummyPageWriterStore()).getColumnWriter(columnDescriptor);

    for (int i = 0; i < totalChunkValues; i++) {
      columnWriter.writeNull(0, 0);  //TODO: change 0 and 0 later
    }

    BytesInput data = columnWriter.concatWriters();
    Statistics statistics = convertStatisticsNullify(chunk.getPrimitiveType(), totalChunkValues);

    writer.writeDataPage(toIntWithCheck(totalChunkValues),
      toIntWithCheck(data.size()),
      data,
      statistics,
      toIntWithCheck(totalChunkValues),
      Encoding.BIT_PACKED,
      Encoding.RLE,
      Encoding.PLAIN);
  }

  private Statistics convertStatisticsNullify(PrimitiveType type, long rowCount) throws IOException {
    org.apache.parquet.column.statistics.Statistics.Builder statsBuilder = org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);
    statsBuilder.withNumNulls(rowCount);
    return statsBuilder.build();
  }

  public BytesInput readBlock(int length, TransParquetFileReader reader) throws IOException {
    byte[] data;
    if (length > pageBufferSize) {
      data = new byte[length];
    } else {
      data = pageBuffer;
    }
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  private int toIntWithCheck(long size) {
    if ((int)size != size) {
      throw new ParquetEncodingException("size is bigger than " + Integer.MAX_VALUE + " bytes: " + size);
    }
    return (int)size;
  }

  private static final class DummyGroupConverter extends GroupConverter {
    @Override public void start() {}
    @Override public void end() {}
    @Override public Converter getConverter(int fieldIndex) { return new DummyConverter(); }
  }

  private static final class DummyConverter extends PrimitiveConverter {
    @Override public GroupConverter asGroupConverter() { return new DummyGroupConverter(); }
  }

  class DummyPageWriterStore implements PageWriteStore {
    @Override
    public PageWriter getPageWriter(ColumnDescriptor path){
      return new DummyPageWriter();
    }
  }

  class DummyPageWriter implements PageWriter {

    public DummyPageWriter() {}

    @Override
    public void writePage(BytesInput bytesInput, int valueCount, Statistics statistics, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding)
      throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writePage(BytesInput bytesInput, int valueCount, int rowCount, Statistics<?> statistics,
                          Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException {
      writePage(bytesInput, valueCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
    }

    @Override
    public void writePageV2(int rowCount, int nullCount, int valueCount,
                            BytesInput repetitionLevels, BytesInput definitionLevels,
                            Encoding dataEncoding, BytesInput data, Statistics<?> statistics) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getMemSize() {
      throw new UnsupportedOperationException();
    }

    public List<DataPage> getPages() {
      throw new UnsupportedOperationException();
    }

    public DictionaryPage getDictionaryPage() {
      throw new UnsupportedOperationException();
    }

    public long getTotalValueCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long allocatedSize() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String memUsageString(String prefix) {
      throw new UnsupportedOperationException();
    }
  }
}
