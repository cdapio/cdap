/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Codec;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Has methods to set/get objects into Configuration object.
 * Also has methods for prefixing a set of configurations with a name and also for extracting the prefixed parameters.
 */
public final class ConfigurationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtil.class);

  public static <T> void set(Configuration conf, String key, Codec<T> codec, T obj) throws IOException {
    byte[] encoded = codec.encode(obj);
    LOG.trace("Serializing {} {}", key, Bytes.toStringBinary(encoded));
    conf.set(key, Base64.encodeBase64String(encoded));
  }

  public static <T> T get(Configuration conf, String key, Codec<T> codec) throws IOException {
    String value = conf.get(key);
    if (value == null) {
      LOG.trace("De-serializing {} with null value", key);
      return codec.decode(null);
    }

    byte[] encoded = Base64.decodeBase64(value);
    LOG.trace("De-serializing {} {}", key, Bytes.toStringBinary(encoded));
    return codec.decode(encoded);
  }

  public static <T> void set(Map<String, String> conf, String key, Codec<T> codec, T obj) throws IOException {
    byte[] encoded = codec.encode(obj);
    LOG.trace("Serializing {} {}", key, Bytes.toStringBinary(encoded));
    conf.put(key, Base64.encodeBase64String(encoded));
  }

  public static <T> T get(Map<String, String> conf, String key, Codec<T> codec) throws IOException {
    String value = conf.get(key);
    if (value == null) {
      LOG.trace("De-serializing {} with null value", key);
      return codec.decode(null);
    }

    byte[] encoded = Base64.decodeBase64(value);
    LOG.trace("De-serializing {} {}", key, Bytes.toStringBinary(encoded));
    return codec.decode(encoded);
  }

  /**
   * Creates a new map containing all key-value pairs in the given {@link Configuration}.
   */
  public static Map<String, String> toMap(Configuration conf) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  /**
   * Sets all key-value pairs from the given {@link Map} into the given {@link Configuration}.
   *
   * @return the {@link Configuration} instance provided
   */
  public static Configuration setAll(@Nullable Map<String, String> map, Configuration conf) {
    if (map == null) {
      return conf;
    }
    for (Map.Entry<String, String> entry : map.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }


  /**
   * Prefixes the specified configurations with the given prefix, and sets them onto the job's configuration.
   *
   * @param conf the Configuration object on which the configurations will be set on
   * @param confKeyPrefix the String to prefix the keys of the configuration
   * @param namedConf the configuration values to be set
   */
  public static void setNamedConfigurations(Configuration conf, String confKeyPrefix, Map<String, String> namedConf) {
    for (Map.Entry<String, String> entry : namedConf.entrySet()) {
      conf.set(confKeyPrefix + entry.getKey(), entry.getValue());
    }
  }

  /**
   * Retrieves all configurations that are prefixed with a particular prefix.
   *
   * @see #setNamedConfigurations(Configuration, String, Map)
   *
   * @param conf the Configuration from which to get the configurations
   * @param confKeyPrefix the prefix to search for in the keys
   * @return a map of key-value pairs, representing the requested configurations, after removing the prefix
   */
  public static Map<String, String> getNamedConfigurations(Configuration conf, String confKeyPrefix) {
    Map<String, String> namedConf = new HashMap<>();
    int prefixLength = confKeyPrefix.length();
    // since its a regex match, we want to look for the character '.', and not match any character
    confKeyPrefix = confKeyPrefix.replace(".", "\\.");
    Map<String, String> properties = conf.getValByRegex("^" + confKeyPrefix + ".*");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      namedConf.put(entry.getKey().substring(prefixLength), entry.getValue());
    }
    return namedConf;
  }

  private ConfigurationUtil() {
    // no-op
  }
}
