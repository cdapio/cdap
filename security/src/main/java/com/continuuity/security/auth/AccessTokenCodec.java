package com.continuuity.security.auth;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Utility to encode and decode {@link AccessToken} and {@link AccessTokenIdentifier} instances to and from
 * byte array representations.
 */
public class AccessTokenCodec {
  public byte[] encode(AccessToken token) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);

    encodeIdentifier(encoder, token.getIdentifier());
    encoder.writeInt(token.getKeyId());
    encoder.writeBytes(token.getDigestBytes());
    return bos.toByteArray();
  }

  public AccessToken decode(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    Decoder decoder = new BinaryDecoder(bis);

    AccessTokenIdentifier identifier = decodeIdentifier(decoder);
    int keyId = decoder.readInt();
    ByteBuffer digest = decoder.readBytes();
    byte[] digestBytes = new byte[digest.remaining()];
    digest.get(digestBytes);

    return new AccessToken(identifier, keyId, digestBytes);
  }

  public byte[] encodeIdentifier(AccessTokenIdentifier identifier) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);
    encodeIdentifier(encoder, identifier);
    return bos.toByteArray();
  }

  public void encodeIdentifier(Encoder encoder, AccessTokenIdentifier identifier) throws IOException {
    encoder.writeString(identifier.getUsername());
    List<String> groups = identifier.getGroups();
    encoder.writeInt(groups.size());
    for (String g : groups) {
      encoder.writeString(g);
    }
    encoder.writeLong(identifier.getIssueTimestamp());
    encoder.writeLong(identifier.getExpireTimestamp());
  }

  public AccessTokenIdentifier decodeIdentifier(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    Decoder decoder = new BinaryDecoder(bis);
    return decodeIdentifier(decoder);
  }

  public AccessTokenIdentifier decodeIdentifier(Decoder decoder) throws IOException {
    String username = decoder.readString();
    int groupSize = decoder.readInt();
    List<String> tmpGroups = Lists.newArrayListWithCapacity(groupSize);
    for (int i = 0; i < groupSize; i++) {
      tmpGroups.add(decoder.readString());
    }
    long issueTimestamp = decoder.readLong();
    long expireTimestamp = decoder.readLong();

    return new AccessTokenIdentifier(username, tmpGroups, issueTimestamp, expireTimestamp);
  }
}
