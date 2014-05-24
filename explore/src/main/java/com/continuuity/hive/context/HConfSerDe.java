package com.continuuity.hive.context;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serializes and deserializes a HConfiguration object into a Configuration object.
 */
public class HConfSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(HConfSerDe.class);

  public static void serialize(Configuration srcConf, Configuration destConf) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    srcConf.writeXml(bos);
    bos.close();

    String confStr = new String(bos.toByteArray(), Charsets.UTF_8);
    LOG.debug("Serializing HConfiguration {}", confStr);
    destConf.set(Constants.HCONF_SERDE_KEY, confStr);
  }

  public static Configuration deserialize(Configuration fromConf) {
    String confStr = fromConf.get(Constants.HCONF_SERDE_KEY);

    if (confStr == null) {
      LOG.warn("HConfiguration is empty");
      return HBaseConfiguration.create();
    }

    LOG.debug("De-serializing HConfiguration {}", confStr);
    ByteArrayInputStream bin = new ByteArrayInputStream(confStr.getBytes(Charsets.UTF_8));
    Configuration hConfiguration = new Configuration();
    hConfiguration.addResource(bin);
    return hConfiguration;
  }
}
