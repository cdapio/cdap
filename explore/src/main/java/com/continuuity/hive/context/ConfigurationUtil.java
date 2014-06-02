package com.continuuity.hive.context;

import com.continuuity.common.io.Codec;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Has methods to set/get objects into Configuration object.
 */
public class ConfigurationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtil.class);

  public static <T> void set(Configuration conf, String key, Codec<T> codec, T obj) throws IOException {
    String value = new String(codec.encode(obj), Charsets.UTF_8);
    LOG.debug("Serializing {} {}", key, value);
    conf.set(key, value);
  }

  public static <T> T get(Configuration conf, String key, Codec<T> codec) throws IOException {
    String value = conf.get(key);
    LOG.debug("De-serializing {} {}", key, value);
    return codec.decode(value == null ? null : value.getBytes(Charsets.UTF_8));
  }
}
