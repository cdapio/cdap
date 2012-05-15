package com.continuuity.common.options;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
public final class OptionsParser {

  private OptionsParser(){}

  /**
   * Parses the annotations specified in <code>object</code> and matches them
   * against the command line arguments that are being passed. If the options
   * could not be parsed it prints usage.
   *
   * @param object instance of class that contains @Option annotations.
   * @param args command line arguments.
   * @param out  stream available to dump outputs.
   * @return
   */
  public static List<String> init(Object object, String[] args, PrintStream out) {
    List<String> nonOptionArgs = new ArrayList<String>();
    Map<String, String> parsedOptions = parseArgs(args, nonOptionArgs);
    Map<String, OptionSpec> declaredOptions = extractDeclarations(object);

    if(parsedOptions.containsKey("help") && !declaredOptions.containsKey("help")) {
      printUsage(declaredOptions, out);
      return null;
    }

    for(String name : parsedOptions.keySet()) {
      if(declaredOptions.containsKey(name)) continue;
        throw new UnrecognizedOptionException(name);
    }

    for(Map.Entry<String, String> option : parsedOptions.entrySet()) {
      try {
        declaredOptions.get(option.getKey()).setValue(option.getValue());
      } catch (IllegalAccessException e) {
        throw new IllegalAccessError(e.getMessage());
      }
    }

    for(Map.Entry<String, OptionSpec> declOption : declaredOptions.entrySet()) {
      OptionSpec option = declOption.getValue();
      if(option.getEnvVar().length() > 0 && ! parsedOptions.containsKey(option.getName())) {
        String envVal = System.getenv(option.getEnvVar());
        if(null != envVal) {
          try {
            option.setValue(envVal);
          } catch (IllegalAccessException e) {
            throw new IllegalAccessError(e.getMessage());
          }
        }
      }
    }
    return nonOptionArgs;
  }

  /**
   * Prints usage based on info specified by annotation in the instance
   * of class specified <code>object</code>.
   * @param object instance of class containing annotations for Options.
   * @param out stream to output usage.
   */
  public static void printUsage(Object object, PrintStream out) {
    printUsage(extractDeclarations(object), out);
  }

  /**
   * Investigates the class specified by <code>object</code> and extracts out
   * all the fields in the class that are annotated with @Option attributes.
   *
   * @param object instance of class that is investigated for presence of @Option attributes
   * @return map of options to it's definitions.
   */
  private static Map<String, OptionSpec> extractDeclarations(Object object) {
    Map<String, OptionSpec> options = new TreeMap<String, OptionSpec>();

    // Get the parent class name.
    Class<?> clazz = object.getClass();

    // Iterate through all the fields specified in the main class
    // and find out annotations that have Option and construct a table.
    do {
      for(Field field : clazz.getDeclaredFields()) {
        Option option = field.getAnnotation(Option.class);
        if(option != null) {
          OptionSpec optionSpec = new OptionSpec(field, option, object);
          String name = optionSpec.getName();
          if(options.containsKey(name)) {
            throw new DuplicateOptionException(name);
          }
          String n = optionSpec.getName();
          options.put(n, optionSpec);
        }
      }
    } while (null != (clazz = clazz.getSuperclass()));
    return options;
  }

  private static Map<String, String> parseArgs(String[] args, List<String> nonOptionArgs) {
    Map<String, String> parsedOptions = new TreeMap<String, String>();
    boolean ignoreTheRest = false;
    for(String arg : args) {
      if(arg.startsWith("-") && !ignoreTheRest) {
        if(arg.endsWith("--")) {
          ignoreTheRest = true;
          break;
        }

        String kv = arg.startsWith("--") ? arg.substring(2) : arg.substring(1);
        String [] splitKV = kv.split("=", 2);
        String key = splitKV[0];
        String value = splitKV.length == 2 ? splitKV[1] : "";
        parsedOptions.put(key, value);
      } else {
        nonOptionArgs.add(arg);
      }
    }
    return parsedOptions;
  }

  private static void printUsage(Map<String, OptionSpec> options, PrintStream out) {
    final String FORMAT_STRING = "  --%s=<%s>\n%s\t(Default=%s)\n\n";
    if(!options.containsKey("help")) {
      out.printf(FORMAT_STRING, "help", "boolean", "\tDisplay this help message\n", "false");
    }

    for(OptionSpec option : options.values()) {
      if(option.isHidden()) {
        continue;
      }
      String usage = option.getUsage();
      if(!usage.isEmpty()) {
        usage = "\t" + usage + "\n";
      }
      out.printf(FORMAT_STRING, option.getName(), option.getTypeName(), usage, option.getDefaultValue());
    }
  }



}
