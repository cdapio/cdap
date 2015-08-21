/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.auth;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.common.io.Decoder;
import co.cask.cdap.common.io.Encoder;
import co.cask.cdap.internal.io.DatumReader;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.DatumWriter;
import co.cask.cdap.internal.io.DatumWriterFactory;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Utility to encode and decode {@link AccessToken} and {@link AccessTokenIdentifier} instances to and from
 * byte array representations.
 */
public class AccessTokenCodec implements Codec<AccessToken> {
  private static final TypeToken<AccessToken> ACCESS_TOKEN_TYPE = new TypeToken<AccessToken>() { };

  private final DatumReaderFactory readerFactory;
  private final DatumWriterFactory writerFactory;

  @Inject
  public AccessTokenCodec(DatumReaderFactory readerFactory, DatumWriterFactory writerFactory) {
    this.readerFactory = readerFactory;
    this.writerFactory = writerFactory;
  }

  @Override
  public byte[] encode(AccessToken token) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);

    encoder.writeInt(AccessToken.Schemas.getVersion());
    DatumWriter<AccessToken> writer = writerFactory.create(ACCESS_TOKEN_TYPE, AccessToken.Schemas.getCurrentSchema());
    writer.encode(token, encoder);
    return bos.toByteArray();
  }

  @Override
  public AccessToken decode(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    Decoder decoder = new BinaryDecoder(bis);

    DatumReader<AccessToken> reader = readerFactory.create(ACCESS_TOKEN_TYPE, AccessToken.Schemas.getCurrentSchema());
    int readVersion = decoder.readInt();
    Schema readSchema = AccessToken.Schemas.getSchemaVersion(readVersion);
    if (readSchema == null) {
      throw new IOException("Unknown schema version for AccessToken: " + readVersion);
    }
    return reader.read(decoder, readSchema);
  }
}
