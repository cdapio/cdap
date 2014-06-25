package com.continuuity.api.common;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Utility class to convert String array to Map<String, String> following POSIX standard.
 */
public final class RuntimeArgumentsUtil {
  public static Map<String, String> toMap(String[] args) {
    Map<String, String> kvMap = Maps.newHashMap();
    for (String arg : args) {
      kvMap.putAll(Splitter.on("--").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(arg));
    }
    return kvMap;
  }

  public static String[] toPosixArray(Map<String, String> kvMap) {
    String[] argArray = new String[kvMap.size()];
    int index = 0;
    for (Map.Entry<String, String> kv : kvMap.entrySet()) {
      argArray[index++] = String.format("--%s=%s", kv.getKey(), kv.getValue());
    }
    return argArray;
  }
}
