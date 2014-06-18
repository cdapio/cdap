package com.continuuity.hive.context;

import com.continuuity.common.io.Codec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Codec to encode/decode HBase Configuration object.
 */
public class HConfCodec implements Codec<Configuration> {
  public static final HConfCodec INSTANCE = new HConfCodec();

  private HConfCodec() {
    // Use the static INSTANCE to get an instance.
  }

  @Override
  public byte[] encode(Configuration object) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    object.writeXml(bos);
    bos.close();
    return bos.toByteArray();
  }

  @Override
  public Configuration decode(byte[] data) throws IOException {
    if (data == null) {
      return HBaseConfiguration.create();
    }

    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    Configuration hConfiguration = new Configuration();
    hConfiguration.addResource(bin);
    return hConfiguration;
  }
}
