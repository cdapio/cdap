/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.security.store;

import io.cdap.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import io.cdap.cdap.security.store.file.FileSecureStoreCodec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link FileSecureStoreImporter}.
 */
public class FileSecureStoreImporterTest {

  @Test
  public void testBackwardsCompatibilityV1() throws Exception {
    char[] password = "test".toCharArray();
    Path keystoreFileVersion65Path = Paths.get(FileSecureStoreImporterTest.class.getClassLoader()
      .getResource("file-secure-store-65.jks").toURI());

    FileSecureStoreCodec currentCodec = FileSecureStoreService.CURRENT_CODEC.newInstance();

    FileSecureStoreImporter importer = new FileSecureStoreImporter(currentCodec);
    KeyStore keyStore = KeyStore.getInstance(currentCodec.getKeyStoreScheme());
    importer.importFromPath(keyStore, keystoreFileVersion65Path, password);

    // Included keystore has a secret under the namespace "abc" and key "abcd" with the value "test"
    SecureStoreService service = new FileSecureStoreService(new SimpleNamespaceQueryAdmin(), password,
                                                            keystoreFileVersion65Path, keyStore, currentCodec);
    byte[] outputData = service.get("abc", "abcd").get();
    Assert.assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), outputData);
  }
}
