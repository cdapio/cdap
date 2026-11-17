/*
 * Copyright © 2026 Cask Data, Inc.
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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Codec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractKeyManager}.
 */
public class AbstractKeyManagerTest {

  private static final Codec<String> STRING_CODEC = new Codec<String>() {
    @Override
    public byte[] encode(String object) {
      return object.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decode(byte[] data) {
      return new String(data, StandardCharsets.UTF_8);
    }
  };

  @Test
  public void testGenerateAndValidateMac() throws Exception {
    InMemoryKeyManager keyManager = new InMemoryKeyManager(CConfiguration.create());
    keyManager.startAsync().awaitRunning();
    try {
      String payload = "hello-cdap";
      KeyManager.DigestId digestId = keyManager.generateMAC(Bytes.toBytes(payload));
      Signed<String> validSigned = createSigned(payload, digestId.getId(), digestId.getDigest());
      keyManager.validateMAC(STRING_CODEC, validSigned);

      byte[] tampered = Arrays.copyOf(digestId.getDigest(), digestId.getDigest().length);
      tampered[0] ^= 0x01;
      Signed<String> invalidSigned = createSigned(payload, digestId.getId(), tampered);
      try {
        keyManager.validateMAC(STRING_CODEC, invalidSigned);
        Assert.fail("Expected InvalidDigestException for tampered digest");
      } catch (InvalidDigestException expected) {
        // expected
      }

      Signed<String> unknownKeySigned = createSigned(payload, digestId.getId() + 1, digestId.getDigest());
      try {
        keyManager.validateMAC(STRING_CODEC, unknownKeySigned);
        Assert.fail("Expected InvalidKeyException for unknown keyId");
      } catch (InvalidKeyException expected) {
        // expected
      }
    } finally {
      keyManager.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testValidateMacPropagatesCodecIoException() throws Exception {
    InMemoryKeyManager keyManager = new InMemoryKeyManager(CConfiguration.create());
    keyManager.startAsync().awaitRunning();
    try {
      KeyManager.DigestId digestId = keyManager.generateMAC(Bytes.toBytes("payload"));
      Signed<String> signedMessage = createSigned("payload", digestId.getId(), digestId.getDigest());
      final IOException expectedIoException = new IOException("codec encode failure");
      Codec<String> failingCodec = new Codec<String>() {
        @Override
        public byte[] encode(String object) throws IOException {
          throw expectedIoException;
        }

        @Override
        public String decode(byte[] data) {
          return "";
        }
      };

      try {
        keyManager.validateMAC(failingCodec, signedMessage);
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException e) {
        Assert.assertSame(expectedIoException, e.getCause());
      }
    } finally {
      keyManager.stopAsync().awaitTerminated();
    }
  }

  private static Signed<String> createSigned(final String message, final int keyId, final byte[] digestBytes) {
    return new Signed<String>() {
      @Override
      public String getMessage() {
        return message;
      }

      @Override
      public int getKeyId() {
        return keyId;
      }

      @Override
      public byte[] getDigestBytes() {
        return digestBytes;
      }
    };
  }
}
