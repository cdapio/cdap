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

package io.cdap.cdap.security.auth;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
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
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Utility to encode and decode keys that are shared between keyManagers.
 */
public class KeyIdentifierCodec implements Codec<KeyIdentifier> {

  private static final TypeToken<KeyIdentifier> KEY_IDENTIFIER_TYPE = new TypeToken<KeyIdentifier>() {
  };

  private final DatumReaderFactory readerFactory;
  private final DatumWriterFactory writerFactory;
  private final Gson gson;

  private static class ByteArrayBase64Deserializer implements JsonDeserializer<byte[]> {

    @Override
    public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      return Base64.getDecoder().decode(json.getAsString());
    }
  }

  @Inject
  public KeyIdentifierCodec(DatumReaderFactory readerFactory, DatumWriterFactory writerFactory) {
    this.readerFactory = readerFactory;
    this.writerFactory = writerFactory;
    this.gson = new GsonBuilder().registerTypeAdapter(byte[].class,
        new ByteArrayBase64Deserializer()).create();
  }

  @Override
  public byte[] encode(KeyIdentifier keyIdentifier) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);

    encoder.writeInt(KeyIdentifier.Schemas.getVersion());
    DatumWriter<KeyIdentifier> writer = writerFactory.create(KEY_IDENTIFIER_TYPE,
        KeyIdentifier.Schemas.getCurrentSchema());
    writer.encode(keyIdentifier, encoder);
    return bos.toByteArray();
  }

  @Override
  public KeyIdentifier decode(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    Decoder decoder = new BinaryDecoder(bis);

    DatumReader<KeyIdentifier> reader = readerFactory.create(KEY_IDENTIFIER_TYPE,
        KeyIdentifier.Schemas.getCurrentSchema());
    try {
      int readVersion = decoder.readInt();
      Schema readSchema = KeyIdentifier.Schemas.getSchemaVersion(readVersion);
      if (readSchema == null) {
        throw new IOException("Unknown schema version for KeyIdentifier: " + readVersion);
      }
      return reader.read(decoder, readSchema);
    } catch (IOException e) {
      // As a fallback, try deserializing from JSON format.
      try {
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), KeyIdentifier.class);
      } catch (JsonSyntaxException jse) {
        // For backwards compatibility, throw an IOException with the initial exception wrapped.
        throw new IOException("Failed to deserialize KeyIdentifier", e);
      }
    }
  }
}
