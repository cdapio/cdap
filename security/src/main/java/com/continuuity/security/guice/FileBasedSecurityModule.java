package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.Codec;
import com.continuuity.security.auth.FileBasedKeyManager;
import com.continuuity.security.auth.KeyIdentifier;
import com.continuuity.security.auth.KeyManager;
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
