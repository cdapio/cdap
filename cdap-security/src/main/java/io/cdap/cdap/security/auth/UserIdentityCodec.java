/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.io.BinaryDecoder;
import io.cdap.cdap.common.io.BinaryEncoder;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.io.DatumReader;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.io.Decoder;
import io.cdap.cdap.common.io.Encoder;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.DatumWriterFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Utility to handle serialization and deserialization of {@link UserIdentity} objects.
 */
public class UserIdentityCodec implements Codec<UserIdentity> {
  private static final TypeToken<UserIdentity> ACCESS_TOKEN_IDENTIFIER_TYPE =
    new TypeToken<UserIdentity>() { };

  private final DatumReaderFactory readerFactory;
  private final DatumWriterFactory writerFactory;

  @Inject
  public UserIdentityCodec(DatumReaderFactory readerFactory, DatumWriterFactory writerFactory) {
    this.readerFactory = readerFactory;
    this.writerFactory = writerFactory;
  }

  @Override
  public byte[] encode(UserIdentity identifier) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);

    encoder.writeInt(UserIdentity.Schemas.getVersion());
    DatumWriter<UserIdentity> writer = writerFactory.create(ACCESS_TOKEN_IDENTIFIER_TYPE,
                                                            UserIdentity.Schemas.getCurrentSchema());
    writer.encode(identifier, encoder);
    return bos.toByteArray();
  }

  @Override
  public UserIdentity decode(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    Decoder decoder = new BinaryDecoder(bis);
    DatumReader<UserIdentity> reader = readerFactory.create(ACCESS_TOKEN_IDENTIFIER_TYPE,
                                                            UserIdentity.Schemas.getCurrentSchema());
    int readVersion = decoder.readInt();
    Schema readSchema = UserIdentity.Schemas.getSchemaVersion(readVersion);
    if (readSchema == null) {
      throw new IOException("Unknown schema version for AccessTokenIdentifier: " + readVersion);
    }
    return reader.read(decoder, readSchema);
  }

}
