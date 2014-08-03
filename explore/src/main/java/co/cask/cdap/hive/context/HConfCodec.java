/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.hive.context;

import co.cask.cdap.common.io.Codec;
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
