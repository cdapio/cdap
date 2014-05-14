/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 * Licensed to Odiago, Inc.
 */
package com.continuuity.common.options;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * <p>
 *   <code>OptionsParser</code> looks into the class looking for annotations that
 *   specifies options. It then matches them with the command line arguments passed
 *   to it. If there are any options that are specified on command line and not
 *   present in annotated definition an usage message is automatically generated.
 *
 *   Following is an simple example of it's usage.
 *   <pre>
 *     public class MyClass {
 *       {@literal @}Option(name="name", usage="Specifies the name of a flow")
 *       private String flowName;
 *
 *       {@literal @}Option(name="flowlet", usage="Specifies the name of the flowlet")
 *       private String flowletName;
 *
 *       {@literal @}Option(name="class", usage="Specifies the class associated with the flowlet to be loaded")
 *       private String className;
 *
 *       {@literal @}Option(name="jar", usage="Specifies the path to the jar file that contains the class specified")
 *       private String jarPath;
 *
 *       {@literal @}Option(name="instance", usage="Specifies the instance id of the flowlet")
 *       private int instance;
 *
 *       public static void main(String[] args) {
 *        MyClass myclass = new MyClass();
 *        OptionsParser.init(myclass, args, System.out);
 *       }
 *     }
 *   </pre>
 * </p>
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
   * @return List of arguments that were not definied by annotations.
   */
  public static List<String> init(Object object, String[] args, String appName, String appVersion, PrintStream out) {
    List<String> nonOptionArgs = new ArrayList<String>();
    Map<String, String> parsedOptions = parseArgs(args, nonOptionArgs);
    Map<String, OptionSpec> declaredOptions = extractDeclarations(object);

    if (parsedOptions.containsKey("help") && !declaredOptions.containsKey("help")) {
      printUsage(declaredOptions, appName, appVersion, out);
      return null;
    }

    for (String name : parsedOptions.keySet()) {
      if (declaredOptions.containsKey(name)) {
        continue;
      }
      throw new UnrecognizedOptionException(name);
    }

    for (Map.Entry<String, String> option : parsedOptions.entrySet()) {
      try {
        declaredOptions.get(option.getKey()).setValue(option.getValue());
      } catch (IllegalAccessException e) {
        throw new IllegalAccessError(e.getMessage());
      }
    }

    for (Map.Entry<String, OptionSpec> declOption : declaredOptions.entrySet()) {
      OptionSpec option = declOption.getValue();
      if (option.getEnvVar().length() > 0 && !parsedOptions.containsKey(option.getName())) {
        String envVal = System.getenv(option.getEnvVar());
        if (null != envVal) {
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
  public static void printUsage(Object object, String appName, String appVersion, PrintStream out) {
    printUsage(extractDeclarations(object), appName, appVersion, out);
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
      for (Field field : clazz.getDeclaredFields()) {
        Option option = field.getAnnotation(Option.class);
        if (option != null) {
          OptionSpec optionSpec = new OptionSpec(field, option, object);
          String name = optionSpec.getName();
          if (options.containsKey(name)) {
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
    for (String arg : args) {
      if (arg.startsWith("-") && !ignoreTheRest) {
        if (arg.endsWith("--")) {
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

  static final String NON_DEFAULT_FORMAT_STRING = " --%-20s %-20s %-20s\n";
  static final String DEFAULT_FORMAT_STRING = " --%-20s %-20s %-50s\t(Default=%s)\n";

  /**
   * Prints the usage based on declared Options in the class.
   * @param options extracted options from introspecting a class
   * @param out Stream to output the usage.
   */
  private static void printUsage(Map<String, OptionSpec> options, String appName, String appVersion, PrintStream out) {

    out.print(String.format("%s - v%s\n", appName, appVersion));
    out.println("Options:");

    if (!options.containsKey("help")) {
      out.printf(NON_DEFAULT_FORMAT_STRING, "help", "", "\tDisplay this help message");
    }

    for (OptionSpec option : options.values()) {
      if (option.isHidden()) {
        continue;
      }
      String usage = option.getUsage();
      if (!usage.isEmpty()) {
        usage = "\t" + usage;
      }

      String def = option.getDefaultValue();
      if ("null".equals(def)) {
        out.printf(NON_DEFAULT_FORMAT_STRING, option.getName(), "<" + option.getTypeName() + ">", usage);
      } else {
        out.printf(DEFAULT_FORMAT_STRING, option.getName(), "<" + option.getTypeName() + ">",
          usage, option.getDefaultValue());
      }
    }
  }
}
