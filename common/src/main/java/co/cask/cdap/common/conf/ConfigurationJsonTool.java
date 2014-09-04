
/*
 * Copyright 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.conf;

import com.google.common.collect.Maps;
import com.google.gson.GsonBuilder;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * A tool to save the CConfiguration as Json.
 */
public class ConfigurationJsonTool {

  public static void exportToJson(String outputFile) throws IOException {
    CConfiguration config = CConfiguration.create();
    Map<String, String> map = Maps.newHashMapWithExpectedSize(config.size());
    for (Map.Entry<String, String> entry : config) {
      map.put(entry.getKey(), entry.getValue());
    }
    FileWriter writer = new FileWriter(outputFile);
    try {
      new GsonBuilder().setPrettyPrinting().create().toJson(map, writer);
    } finally {
      writer.close();
    }
  }

  public static void main(String[] args) {

    String programName = System.getProperty("script", "ConfigurationJsonTool");

    if (args.length != 2 || !"--output".equals(args[0])) {
      System.err.println("Usage: " + programName + " --output <file name>");
      System.exit(1);
    }
    String outputFile = args[1];
    try {
      exportToJson(outputFile);
    } catch (IOException e) {
      System.err.println("Error writing to file '" + outputFile + "': " + e.getMessage());
    }
  }
}
