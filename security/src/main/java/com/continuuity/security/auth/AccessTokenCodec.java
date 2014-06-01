package com.continuuity.security.auth;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Codec;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.internal.io.DatumReader;
import com.continuuity.internal.io.DatumReaderFactory;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.DatumWriterFactory;
import com.continuuity.internal.io.Schema;
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
