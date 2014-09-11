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
import com.google.common.collect.Sets;
import com.google.gson.GsonBuilder;

import java.util.Map;
import java.util.Set;

/**
 * A tool to save the CConfiguration as Json.
 */
public class ConfigurationJsonTool {

  public static void exportToJson(String configParam, Appendable output) {
    Configuration config;
    if (configParam.equals("--cdap")) {
      config = CConfiguration.create();
    } else if (configParam.equals("--security")) {
      config = SConfiguration.create();
    } else {
      return;
    }

    Map<String, String> map = Maps.newHashMapWithExpectedSize(config.size());
    for (Map.Entry<String, String> entry : config) {
      map.put(entry.getKey(), entry.getValue());
    }
    new GsonBuilder().setPrettyPrinting().create().toJson(map, output);
  }

  public static void main(String[] args) {

    String programName = System.getProperty("script", "ConfigurationJsonTool");
    Set<String> validArgument = Sets.newHashSet();
    validArgument.add("--cdap");
    validArgument.add("--security");
    if (args.length != 1 || !(validArgument.contains(args[0]))) {
      System.err.println("Usage: " + programName + "(--cdap | --security)");
      System.exit(1);
    }
    exportToJson(args[0].trim(), System.out);
  }
}
