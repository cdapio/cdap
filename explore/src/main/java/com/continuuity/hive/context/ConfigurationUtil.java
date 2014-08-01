/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.hive.context;

import com.continuuity.common.io.Codec;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Has methods to set/get objects into Configuration object.
 */
public class ConfigurationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtil.class);

  public static <T> void set(Configuration conf, String key, Codec<T> codec, T obj) throws IOException {
    String value = new String(codec.encode(obj), Charsets.UTF_8);
    LOG.trace("Serializing {} {}", key, value);
    conf.set(key, value);
  }

  public static <T> T get(Configuration conf, String key, Codec<T> codec) throws IOException {
    String value = conf.get(key);
    LOG.trace("De-serializing {} {}", key, value);
    // Using Latin-1 encoding so that all bytes can be encoded as string. UTF-8 has some invalid bytes that will get
    // skipped.
    return codec.decode(value == null ? null : value.getBytes("ISO-8859-1"));
  }

  public static <T> void set(Map<String, String> conf, String key, Codec<T> codec, T obj) throws IOException {
    String value = new String(codec.encode(obj), Charsets.UTF_8);
    LOG.trace("Serializing {} {}", key, value);
    conf.put(key, value);
  }

  public static <T> T get(Map<String, String> conf, String key, Codec<T> codec) throws IOException {
    String value = conf.get(key);
    LOG.trace("De-serializing {} {}", key, value);
    // Using Latin-1 encoding so that all bytes can be encoded as string. UTF-8 has some invalid bytes that will get
    // skipped.
    return codec.decode(value == null ? null : value.getBytes("ISO-8859-1"));
  }

}
