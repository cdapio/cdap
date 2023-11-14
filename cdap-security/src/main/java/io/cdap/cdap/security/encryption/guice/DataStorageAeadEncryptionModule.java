/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.encryption.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.cdap.cdap.security.encryption.AeadCipher;

/**
 * Guice module for encryption bindings for data storage encryption.
 */
public class DataStorageAeadEncryptionModule extends PrivateModule {

  public static final String DATA_STORAGE_ENCRYPTION = "DataStorageEncryption";

  @Override
  protected void configure() {
    // Bind extension loader.
    bind(AeadCipherCryptorExtensionLoader.class).in(Scopes.SINGLETON);

    // Bind sensitive data storage encryption providers.
    bind(AeadCipher.class)
        .annotatedWith(Names.named(DATA_STORAGE_ENCRYPTION))
        .toProvider(DataStorageAeadCipherProvider.class).in(Scopes.SINGLETON);
    expose(AeadCipher.class)
        .annotatedWith(Names.named(DATA_STORAGE_ENCRYPTION));
  }
}
