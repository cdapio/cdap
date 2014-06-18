package com.continuuity.hive.context;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.Codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Codec to encode/decode CConfiguration object.
 */
public class CConfCodec implements Codec<CConfiguration> {
  public static final CConfCodec INSTANCE = new CConfCodec();

  private CConfCodec() {
    // Use the static INSTANCE to get an instance.
  }

  @Override
  public byte[] encode(CConfiguration object) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    object.writeXml(bos);
    bos.close();
    return bos.toByteArray();
  }

  @Override
  public CConfiguration decode(byte[] data) throws IOException {
    if (data == null) {
      return CConfiguration.create();
    }

    ByteArrayInputStream bin = new ByteArrayInputStream(data);
    CConfiguration cConfiguration = new CConfiguration();
    cConfiguration.addResource(bin);
    return cConfiguration;
  }
}
