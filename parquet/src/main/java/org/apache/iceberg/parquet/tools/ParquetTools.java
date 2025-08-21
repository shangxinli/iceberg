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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ParquetTools {
  private static final Map<String, Command> COMMANDS = new HashMap<>();

  static {
    COMMANDS.put("trans-compression", new TransCompressionCommandWrapper());
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }

    String commandName = args[0];
    Command command = COMMANDS.get(commandName);
    
    if (command == null) {
      System.err.println("Unknown command: " + commandName);
      printUsage();
      System.exit(1);
    }

    String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
    try {
      command.execute(commandArgs);
    } catch (Exception e) {
      System.err.println("Error executing command: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void printUsage() {
    System.err.println("Usage: ParquetTools <command> [command-specific-args]");
    System.err.println("Available commands:");
    System.err.println("  trans-compression - Translate compression codec of a Parquet file");
  }

  interface Command {
    void execute(String[] args) throws Exception;
  }

  static class TransCompressionCommandWrapper implements Command {
    @Override
    public void execute(String[] args) throws Exception {
      TransCompression.main(args);
    }
  }
}