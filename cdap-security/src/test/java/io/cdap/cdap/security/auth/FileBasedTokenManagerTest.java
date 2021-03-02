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

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.security.guice.FileBasedSecurityModule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for a key manager that saves keys to file.
 */
public class FileBasedTokenManagerTest extends TestTokenManager {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Override
  protected ImmutablePair<TokenManager, Codec<AccessToken>> getTokenManagerAndCodec() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(new IOModule(),
                                             new ConfigModule(cConf),
                                             new FileBasedSecurityModule(),
                                             new InMemoryDiscoveryModule());
    TokenManager tokenManager = injector.getInstance(TokenManager.class);
    tokenManager.startAndWait();
    Codec<AccessToken> tokenCodec = injector.getInstance(AccessTokenCodec.class);
    return new ImmutablePair<>(tokenManager, tokenCodec);
  }

  /**
   * Test that two token managers can share a key that is written to a file.
   */
  @Test
  public void testFileBasedKey() throws Exception {
    // Create two token managers that points to the same path
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    TokenManager tokenManager = Guice.createInjector(
      new IOModule(),
      new ConfigModule(cConf),
      new FileBasedSecurityModule(),
      new InMemoryDiscoveryModule()).getInstance(TokenManager.class);
    tokenManager.startAndWait();

    TokenManager tokenManager2 = Guice.createInjector(
      new IOModule(),
      new ConfigModule(cConf),
      new FileBasedSecurityModule(),
      new InMemoryDiscoveryModule()).getInstance(TokenManager.class);
    tokenManager2.startAndWait();

    Assert.assertNotSame("ERROR: Both token managers refer to the same object.", tokenManager, tokenManager2);

    String user = "testuser";
    long now = System.currentTimeMillis();
    List<String> groups = Lists.newArrayList("users", "admins");
    UserIdentity identifier = new UserIdentity(user, groups, now, now + TOKEN_DURATION);

    AccessToken token = tokenManager.signIdentifier(identifier);

    // Since both tokenManagers have the same key, they must both be able to validate the secret.
    tokenManager.validateSecret(token);
    tokenManager2.validateSecret(token);
  }

  @Test
  public void testKeyUpdate() throws Exception {
    File keyDir = TEMP_FOLDER.newFolder();
    File keyFile = new File(keyDir, "key");

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, keyFile.getAbsolutePath());

    Injector injector = Guice.createInjector(
      new IOModule(),
      new ConfigModule(cConf),
      new FileBasedSecurityModule(),
      new InMemoryDiscoveryModule()
    );

    Codec<KeyIdentifier> codec = injector.getInstance(Key.get(new TypeLiteral<Codec<KeyIdentifier>>() { }));
    FileBasedKeyManager keyManager = injector.getInstance(FileBasedKeyManager.class);

    KeyIdentifier keyIdentifier = generateAndSaveKey(keyFile.toPath(), keyManager, codec, 0);

    // Set the last modified time to 10 seconds ago to workaround the MacOS FS timestamp granularity (1 second)
    // so that test can run faster.
    // noinspection ResultOfMethodCallIgnored
    keyFile.setLastModified(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10));

    try {
      keyManager.startAndWait();
      // Upon the key manager starts, the current key should be the same as the one from the key file.
      Assert.assertEquals(keyIdentifier, keyManager.currentKey);

      // Now update the key by doing an atomic move
      Path tempFile = TEMP_FOLDER.newFile().toPath();
      keyIdentifier = generateAndSaveKey(tempFile, keyManager, codec, 1);
      Files.move(tempFile, keyFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

      // Wait for the key change in the key manager
      Tasks.waitFor(keyIdentifier, () -> keyManager.currentKey, 20, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    } finally {
      keyManager.stopAndWait();
    }
  }

  private KeyIdentifier generateAndSaveKey(Path keyFile, FileBasedKeyManager keyManager,
                                           Codec<KeyIdentifier> codec,
                                           int keyId) throws NoSuchAlgorithmException, IOException {
    KeyIdentifier keyIdentifier = keyManager.generateKey(keyManager.createKeyGenerator(), keyId);
    Files.write(keyFile, codec.encode(keyIdentifier));
    return keyIdentifier;
  }
}
