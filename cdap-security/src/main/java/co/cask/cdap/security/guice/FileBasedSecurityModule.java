/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.security.auth.FileBasedKeyManager;
import co.cask.cdap.security.auth.KeyIdentifier;
import co.cask.cdap.security.auth.KeyManager;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;

/**
 * Guice bindings for FileBasedKeyManagers. This extends {@code SecurityModule} to provide
 * an instance of {@code FileBasedKeyManager}.
 */
public class FileBasedSecurityModule extends SecurityModule {

  @Override
  protected void bindKeyManager(Binder binder) {
    binder.bind(KeyManager.class).toProvider(FileBasedKeyManagerProvider.class).in(Scopes.SINGLETON);
  }

  private static final class FileBasedKeyManagerProvider implements  Provider<KeyManager> {
    private CConfiguration cConf;
    private Codec<KeyIdentifier> keyIdentifierCodec;

    @Inject
    FileBasedKeyManagerProvider(CConfiguration cConf, Codec<KeyIdentifier> keyIdentifierCodec) {
      this.cConf = cConf;
      this.keyIdentifierCodec = keyIdentifierCodec;
    }

    @Override
    public KeyManager get() {
      return new FileBasedKeyManager(cConf, keyIdentifierCodec);
    }
  };

}
