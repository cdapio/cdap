/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.common.conf;

import com.google.common.base.Preconditions;

import java.io.File;
import java.net.URI;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Utilities for {@link CConfiguration}.
 */
public class CConfigurationUtil extends Configuration {

  private CConfigurationUtil() { }

  /**
   * Returns a {@link Map} that is backed by the given {@link CConfiguration}. Updates to the returned Map
   * will be reflected in the {@link CConfiguration}.
   */
  public static Map<String, String> asMap(CConfiguration cConf) {
    return new AbstractMap<String, String>() {
      @Override
      public Set<Entry<String, String>> entrySet() {
        return new AbstractSet<Entry<String, String>>() {

          @Override
          public boolean add(Entry<String, String> entry) {
            String oldValue = cConf.get(entry.getKey());
            if (Objects.equals(oldValue, entry.getValue())) {
              return false;
            }
            cConf.set(entry.getKey(), entry.getValue());
            return true;
          }

          @Override
          public Iterator<Entry<String, String>> iterator() {
            return cConf.iterator();
          }

          @Override
          public int size() {
            return cConf.size();
          }
        };
      }

      @Override
      public String put(String key, String value) {
        String oldValue = cConf.get(key);
        cConf.set(key, value);
        return oldValue;
      }

      @Override
      public boolean containsKey(Object key) {
        return get(key) != null;
      }

      @Override
      public String get(Object key) {
        return cConf.get(key.toString());
      }

      @Override
      public int size() {
        return cConf.size();
      }
    };
  }

  public static void copyTxProperties(CConfiguration cConf, org.apache.hadoop.conf.Configuration destination) {
    Properties props = cConf.getProps();
    for (String property : props.stringPropertyNames()) {
      if (property.startsWith("data.tx") || property.startsWith("tx.persist")) {
        destination.set(property, cConf.get(property));
      }
    }
  }

  /**
   * Copies the prefixed properties from {@link CConfiguration} into {@link org.apache.hadoop.conf.Configuration}.
   * @param prefix the prefix for the property which need to be copied
   * @param cConf source config
   * @param destination destination config
   */
  public static void copyPrefixedProperties(String prefix, CConfiguration cConf,
                                            org.apache.hadoop.conf.Configuration destination) {
    Properties props = cConf.getProps();
    for (String property : props.stringPropertyNames()) {
      if (property.startsWith(prefix)) {
        destination.set(property, cConf.get(property));
      }
    }
  }

  /**
   * Get extra jars set in {@link CConfiguration} as a list of {@link URI}.
   *
   * @param cConf {@link CConfiguration} containing the extra jars
   * @return a list of {@link File} created from the extra jars set in cConf.
   */
  public static List<URI> getExtraJars(CConfiguration cConf) {
    String[] extraJars = cConf.getStrings(Constants.AppFabric.PROGRAM_CONTAINER_DIST_JARS);
    List<URI> jarURIs = new ArrayList<>();
    if (extraJars == null) {
      return jarURIs;
    }
    for (String jarPath : extraJars) {
      URI uri;
      try {
        uri = URI.create(jarPath);
      } catch (IllegalArgumentException e) {
        // Assume that it is local path and try to create it with File
        uri = new File(jarPath).toURI();
      }
      if (uri.getScheme() == null) { // no scheme, assume it's local file
        uri = new File(jarPath).toURI();
      }
      jarURIs.add(uri);
    }
    return jarURIs;
  }

  /**
   * Asserts that the given CConfiguration has valid properties.
   * @param cConf the CConfiguration object to check
   * @throws IllegalArgumentException if the given cConf is invalid.
   */
  public static void verify(CConfiguration cConf) {
    // Checks to ensure that certain keys (e.g. "root.prefix") are valid as expected by CDAP.
    assertAlphanumeric(cConf, Constants.ROOT_NAMESPACE);
    assertAlphanumeric(cConf, Constants.Dataset.TABLE_PREFIX);
  }

  private static void assertAlphanumeric(CConfiguration cConf, String key) {
    String value = cConf.get(key);
    Preconditions.checkNotNull(value, "Entry of CConf with key: %s is null", key);
    Preconditions.checkArgument(value.matches("[a-zA-Z0-9]+"),
                                "CConf entry with key: %s must consist " +
                                  "of only alphanumeric characters; it is: %s", key, value);
  }
}
