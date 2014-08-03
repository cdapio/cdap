/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.security.guice.FileBasedSecurityModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import static org.junit.Assert.assertEquals;

/**
 * Test serialization and deserialization of KeyIdentifiers.
 */
public class TestKeyIdentifierCodec {
  private static Codec<KeyIdentifier> keyIdentifierCodec;
  private static int keyLength;
  private static String keyAlgo;
  private static KeyGenerator keyGenerator;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = Guice.createInjector(new IOModule() , new ConfigModule(), new FileBasedSecurityModule(),
                                             new DiscoveryRuntimeModule().getInMemoryModules());
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

}
