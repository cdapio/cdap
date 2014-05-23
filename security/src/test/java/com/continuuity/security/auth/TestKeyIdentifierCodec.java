package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.io.Codec;
import com.continuuity.security.guice.FileBasedSecurityModule;
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
