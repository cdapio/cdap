/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

import static org.junit.Assert.assertEquals;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.security.guice.FileBasedCoreSecurityModule;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Random;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test serialization and deserialization of KeyIdentifiers.
 */
public class TestKeyIdentifierCodec {
  private static final long HOUR_MILLIS = 60 * 1000;

  private static Codec<KeyIdentifier> keyIdentifierCodec;
  private static int keyLength;
  private static String keyAlgo;
  private static KeyGenerator keyGenerator;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = Guice.createInjector(new IOModule() , new ConfigModule(), new FileBasedCoreSecurityModule(),
                                             new InMemoryDiscoveryModule());
    CConfiguration conf = injector.getInstance(CConfiguration.class);
    keyIdentifierCodec = injector.getInstance(KeyIdentifierCodec.class);
    keyLength = conf.getInt(Constants.Security.TOKEN_DIGEST_KEY_LENGTH);
    keyAlgo = conf.get(Constants.Security.TOKEN_DIGEST_ALGO);

    keyGenerator = KeyGenerator.getInstance(keyAlgo);
    keyGenerator.init(keyLength);
  }

  @Test
  public void testKeyIdentifierSerialization() throws Exception {
    SecretKey nextKey = keyGenerator.generateKey();
    Random random = new Random();
    KeyIdentifier keyIdentifier = new KeyIdentifier(nextKey, random.nextInt(), Long.MAX_VALUE);

    byte[] encoded = keyIdentifierCodec.encode(keyIdentifier);
    KeyIdentifier decodedKeyIdentifier = keyIdentifierCodec.decode(encoded);

    assertEquals(keyIdentifier, decodedKeyIdentifier);
  }

  @Test
  public void testKeyIdentifierJSONDeserialization() throws Exception {
    String algorithm = "AES";
    int keyId = 15;
    long expiration = System.currentTimeMillis() + HOUR_MILLIS;
    byte[] secretKeyBytes = new byte[32];
    SecureRandom secureRandom = new SecureRandom();
    secureRandom.nextBytes(secretKeyBytes);
    SecretKeySpec keySpec = new SecretKeySpec(secretKeyBytes, algorithm);
    KeyIdentifier expectedKeyIdentifier = new KeyIdentifier(keySpec, keyId, expiration);

    String jsonKeyIdentifier = String.format("{\"encodedKey\": \"%s\", \"algorithm\": \"%s\", "
            + "\"keyId\": %d, \"expiration\": %d}",
        Base64.getEncoder().encodeToString(secretKeyBytes), algorithm, keyId,
        expiration);
    KeyIdentifier decodedKeyIdentifier = keyIdentifierCodec.decode(
        jsonKeyIdentifier.getBytes(StandardCharsets.UTF_8));
    assertEquals(expectedKeyIdentifier, decodedKeyIdentifier);
  }

}
