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
 * Utility to handle serialization and deserialization of {@link AccessTokenIdentifier} objects.
 */
public class AccessTokenIdentifierCodec implements Codec<AccessTokenIdentifier> {
  private static final TypeToken<AccessTokenIdentifier> ACCESS_TOKEN_IDENTIFIER_TYPE =
    new TypeToken<AccessTokenIdentifier>() { };

  private final DatumReaderFactory readerFactory;
  private final DatumWriterFactory writerFactory;

  @Inject
  public AccessTokenIdentifierCodec(DatumReaderFactory readerFactory, DatumWriterFactory writerFactory) {
    this.readerFactory = readerFactory;
    this.writerFactory = writerFactory;
  }

  @Override
  public byte[] encode(AccessTokenIdentifier identifier) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);

    encoder.writeInt(AccessTokenIdentifier.Schemas.getVersion());
    DatumWriter<AccessTokenIdentifier> writer = writerFactory.create(ACCESS_TOKEN_IDENTIFIER_TYPE,
                                                                     AccessTokenIdentifier.Schemas.getCurrentSchema());
    writer.encode(identifier, encoder);
    return bos.toByteArray();
  }

  @Override
  public AccessTokenIdentifier decode(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    Decoder decoder = new BinaryDecoder(bis);
    DatumReader<AccessTokenIdentifier> reader = readerFactory.create(ACCESS_TOKEN_IDENTIFIER_TYPE,
                                                                     AccessTokenIdentifier.Schemas.getCurrentSchema());
    int readVersion = decoder.readInt();
    Schema readSchema = AccessTokenIdentifier.Schemas.getSchemaVersion(readVersion);
    if (readSchema == null) {
      throw new IOException("Unknown schema version for AccessTokenIdentifier: " + readVersion);
    }
    return reader.read(decoder, readSchema);
  }

}
