package com.continuuity.hive.context;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serializes and deserializes a CConfiguration object into a Configuration object.
 */
public class CConfSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(CConfSerDe.class);

  public static void serialize(CConfiguration srcConf, Configuration destConf) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    srcConf.writeXml(bos);
    bos.close();

    String confStr = new String(bos.toByteArray(), Charsets.UTF_8);
    LOG.debug("Serializing CConfiguration {}", confStr);
    destConf.set(Constants.CCONF_SERDE_KEY, confStr);
  }

  public static CConfiguration deserialize(Configuration fromConf) {
    String confStr = fromConf.get(Constants.CCONF_SERDE_KEY);

    if (confStr == null) {
      LOG.warn("CConfiguration is empty");
      return CConfiguration.create();
    }

    LOG.debug("De-serializing CConfiguration {}", confStr);
    ByteArrayInputStream bin = new ByteArrayInputStream(confStr.getBytes(Charsets.UTF_8));
    CConfiguration cConfiguration = new CConfiguration();
    cConfiguration.addResource(bin);
    return cConfiguration;
  }
}
