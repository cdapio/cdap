/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Codec to encode/decode Hadoop Configuration object.
 */
public class HConfCodec extends ConfCodec<Configuration> {
  public static final HConfCodec INSTANCE = new HConfCodec();

  private HConfCodec() {
    // Use the static INSTANCE to get an instance.
  }

  @Override
  public void encode(Configuration object, StringWriter stringWriter) throws IOException {
    object.writeXml(stringWriter);
  }

  @Override
  public Configuration decode(StringReader stringReader) {
    Configuration hConfiguration = new Configuration();
    hConfiguration.addResource(new ReaderInputStream(stringReader));
    return hConfiguration;
  }
}
