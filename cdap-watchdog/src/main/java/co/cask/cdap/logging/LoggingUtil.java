/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A util class to host common log related methods.
 */
public final class LoggingUtil {

  private static final String MDC_NULL_KEY = ".null";
  private static final Comparator<File> FILE_NAME_COMPARATOR = new Comparator<File>() {
    @Override
    public int compare(File o1, File o2) {
      return o1.getName().compareTo(o2.getName());
    }
  };

  private LoggingUtil() {
    // no-op
  }

  /**
   * Returns a list of {@link URL} containing the log extension jars based on the given configuration.
   */
  public static List<File> getExtensionJars(CConfiguration cConf) {
    List<File> libJars = new ArrayList<>();

    // Adds all extra log library jars
    for (String libDir : cConf.getTrimmedStringCollection(Constants.Logging.PIPELINE_LIBRARY_DIR)) {
      List<File> files = new ArrayList<>(DirUtils.listFiles(new File(libDir), "jar"));
      Collections.sort(files, FILE_NAME_COMPARATOR);
      libJars.addAll(files);
    }

    return libJars;
  }

  public static URL[] getExtensionJarsAsURLs(CConfiguration cConf) {
    List<File> extensionJars = getExtensionJars(cConf);
    URL[] urls = new URL[extensionJars.size()];
    int idx = 0;
    for (File jar : extensionJars) {
      try {
        urls[idx++] = jar.toURI().toURL();
      } catch (MalformedURLException e) {
        // This should never happen.
        throw new IllegalArgumentException("Malformed URL occurred from file " + jar, e);
      }
    }
    return urls;
  }

  /**
   * Returns the result of {@link Object#toString()} or {@code null} if the given object is null.
   */
  public static String stringOrNull(@Nullable Object obj) {
    return obj == null ? null : obj.toString();
  }

  /**
   * Creates a new map from the given mdc map by replacing {@code null} key with {@link #MDC_NULL_KEY}.
   */
  public static Map<String, String> encodeMDC(Map<String, String> mdc) {
    Map<String, String> encodeMap = new HashMap<>(mdc.size());
    for (Map.Entry<String, String> entry : mdc.entrySet()) {
      encodeMap.put(entry.getKey() == null ? MDC_NULL_KEY : entry.getKey(), entry.getValue());
    }
    return encodeMap;
  }

  /**
   * Creates a new map from the given map by reverting the {@link #encodeMDC(Map)} replacement.
   */
  public static Map<String, String> decodeMDC(@Nullable Map<?, ?> map) {
    if (map == null) {
      // Returns an empty map as ILoggingEvent guarantees MDC never null.
      return new HashMap<>();
    }

    Map<String, String> stringMap = new HashMap<>(map.size());
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      // AVRO does not allow null map keys.
      Object key = entry.getKey();
      Object value = entry.getValue();
      stringMap.put(key == null || MDC_NULL_KEY.equals(key.toString()) ? null : key.toString(),
                    value == null ? null : value.toString());
    }
    return stringMap;
  }
}
