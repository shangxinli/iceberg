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
package org.apache.iceberg.parquet.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestTransCompression {
  
  @TempDir
  private Path tempDir;
  
  private Configuration conf;
  private TransCompression command;
  
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "data", Types.StringType.get())
  );
  
  @BeforeEach
  public void setup() {
    conf = new Configuration();
    command = new TransCompression(conf);
  }
  
  @Test
  public void testTranslateCompressionSnappyToGzip() throws IOException {
    File inputFile = createParquetFile("input.parquet", CompressionCodecName.SNAPPY);
    File outputFile = new File(tempDir.toFile(), "output.parquet");
    
    assertThatCode(() -> 
        command.execute(inputFile.getPath(), outputFile.getPath(), "GZIP")
    ).doesNotThrowAnyException();
    
    assertThat(outputFile).exists();
    
    // Verify compression codec changed
    ParquetMetadata metadata = readParquetMetadata(outputFile);
    assertThat(metadata.getBlocks().get(0).getColumns().get(0).getCodec())
        .isEqualTo(CompressionCodecName.GZIP);
    
    // Verify data integrity
    List<Record> originalRecords = readParquetRecords(inputFile);
    List<Record> translatedRecords = readParquetRecords(outputFile);
    assertThat(translatedRecords).isEqualTo(originalRecords);
  }
  
  @Test
  public void testTranslateCompressionGzipToUncompressed() throws IOException {
    File inputFile = createParquetFile("input_gzip.parquet", CompressionCodecName.GZIP);
    File outputFile = new File(tempDir.toFile(), "output_uncompressed.parquet");
    
    assertThatCode(() -> 
        command.execute(inputFile.getPath(), outputFile.getPath(), "UNCOMPRESSED")
    ).doesNotThrowAnyException();
    
    assertThat(outputFile).exists();
    
    // Verify compression codec changed
    ParquetMetadata metadata = readParquetMetadata(outputFile);
    assertThat(metadata.getBlocks().get(0).getColumns().get(0).getCodec())
        .isEqualTo(CompressionCodecName.UNCOMPRESSED);
  }
  
  @Test
  public void testMainMethod() throws IOException {
    File inputFile = createParquetFile("input_main.parquet", CompressionCodecName.SNAPPY);
    File outputFile = new File(tempDir.toFile(), "output_main.parquet");
    
    String[] args = {inputFile.getPath(), outputFile.getPath(), "ZSTD"};
    
    assertThatCode(() -> 
        TransCompression.main(args)
    ).doesNotThrowAnyException();
    
    assertThat(outputFile).exists();
    
    // Verify compression codec changed
    ParquetMetadata metadata = readParquetMetadata(outputFile);
    assertThat(metadata.getBlocks().get(0).getColumns().get(0).getCodec())
        .isEqualTo(CompressionCodecName.ZSTD);
  }
  
  private File createParquetFile(String filename, CompressionCodecName codec) throws IOException {
    File file = new File(tempDir.toFile(), filename);
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(file);
    
    FileAppender<Record> appender = Parquet.write(outputFile)
        .schema(SCHEMA)
        .createWriterFunc(GenericParquetWriter::create)
        .set("write.parquet.compression-codec", codec.name())
        .build();
    
    try (FileAppender<Record> writer = appender) {
      for (int i = 0; i < 100; i++) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", i);
        record.setField("data", "test-" + i);
        writer.add(record);
      }
    }
    
    return file;
  }
  
  private ParquetMetadata readParquetMetadata(File file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.getPath()), conf))) {
      return reader.getFooter();
    }
  }
  
  private List<Record> readParquetRecords(File file) throws IOException {
    List<Record> records = new java.util.ArrayList<>();
    try (CloseableIterable<Record> reader = Parquet.read(org.apache.iceberg.Files.localInput(file))
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
        .build()) {
      for (Record record : reader) {
        records.add(record);
      }
    }
    return records;
  }
}