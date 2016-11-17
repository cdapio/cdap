/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Codec;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Base codec class for encoding/deciding hConf and cConf.
 * We need to escape and unescape '{', so that getting the encoded conf from an hConf does not try to expand the
 * variables in the value of the Configuration, and avoid triggering of  an
 * IllegalStateException 'Variable substitution depth too large: 20'. See CDAP-7651 for more details.
 */
abstract class ConfCodec<T> implements Codec<T> {

  /**
   * Encodes the object to the given StringWriter.
   */
  public abstract void encode(T object, StringWriter stringWriter) throws IOException;

  /**
   * Returns an object decoded from the given StringReader
   */
  public abstract T decode(StringReader stringReader);

  @Override
  public byte[] encode(T object) throws IOException {
    StringWriter stringWriter = new StringWriter();
    encode(object, stringWriter);
    stringWriter.flush();
    String objectString = stringWriter.toString();
    String escapedString = StringUtils.escapeString(objectString, StringUtils.ESCAPE_CHAR, '{');
    return Bytes.toBytes(escapedString);
  }

  @Override
  public T decode(byte[] data) throws IOException {
    if (data == null) {
      // in all the places that ConfCodec is used, the serialized data should not be null.
      throw new IllegalStateException("data is null, while deserializing configuration.");
    }
    String objectString = Bytes.toString(data);
    String unescapedString = StringUtils.unEscapeString(objectString, StringUtils.ESCAPE_CHAR, '{');

    StringReader stringReader = new StringReader(unescapedString);
    return decode(stringReader);
  }
}
