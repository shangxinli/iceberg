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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;

public class TransCompression {
  private final Configuration conf;
  private final CompressionConverter compressionConverter;

  public TransCompression() {
    this.conf = new Configuration();
    this.compressionConverter = new CompressionConverter();
  }

  public TransCompression(Configuration conf) {
    this.conf = conf;
    this.compressionConverter = new CompressionConverter();
  }

  public void execute(String inputPath, String outputPath, String codecName) throws IOException {
    Path inPath = new Path(inputPath);
    Path outPath = new Path(outputPath);
    CompressionCodecName codec = CompressionCodecName.valueOf(codecName);

    ParquetMetadata metaData;
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(inPath, conf),
        HadoopReadOptions.builder(conf).build())) {
      metaData = reader.getFooter();
    }

    MessageType schema = metaData.getFileMetaData().getSchema();
    ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(outPath, conf), 
        schema, 
        ParquetFileWriter.Mode.CREATE, 
        ParquetWriter.DEFAULT_BLOCK_SIZE, 
        ParquetWriter.DEFAULT_PAGE_SIZE);
    writer.start();

    try (TransParquetFileReader reader = new TransParquetFileReader(
        HadoopInputFile.fromPath(inPath, conf),
        HadoopReadOptions.builder(conf).build())) {
      compressionConverter.processBlocks(reader, writer, metaData, schema, 
          metaData.getFileMetaData().getCreatedBy(), codec);
    } finally {
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      printUsage();
      System.exit(1);
    }

    String inputPath = args[0];
    String outputPath = args[1];
    String codecName = args[2];

    try {
      TransCompression command = new TransCompression();
      command.execute(inputPath, outputPath, codecName);
      System.out.println("Successfully translated compression from " + inputPath + " to " + outputPath + " using " + codecName);
    } catch (Exception e) {
      System.err.println("Error during compression translation: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void printUsage() {
    System.err.println("Usage: TransCompressionCommand <input> <output> <codec_name>");
    System.err.println("where <input> is the source parquet file");
    System.err.println("      <output> is the destination parquet file");
    System.err.println("      <codec_name> is the codec name to be translated to, e.g. SNAPPY, GZIP, ZSTD, LZO, LZ4, BROTLI, UNCOMPRESSED");
  }
}