package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.security.auth.KeyManager;
import com.continuuity.security.auth.TokenManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;

import java.security.NoSuchAlgorithmException;

/**
 * Guice bindings for security related classes.  This extends {@code PrivateModule} in order to limit which classes
 * are exposed.
 */
public class SecurityModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(KeyManager.class).toProvider(KeyManagerProvider.class).in(Scopes.SINGLETON);
    bind(TokenManager.class).in(Scopes.SINGLETON);
    expose(TokenManager.class);
  }

  static class KeyManagerProvider implements Provider<KeyManager> {
    private CConfiguration cConf = CConfiguration.create();

    @Inject(optional = true)
    public void setCConfiguration(CConfiguration conf) {
      this.cConf = conf;
    }

    @Override
    public KeyManager get() {
      KeyManager keyManager = new KeyManager(this.cConf);
      try {
        keyManager.init();
      } catch (NoSuchAlgorithmException nsae) {
        throw Throwables.propagate(nsae);
      }
      return keyManager;
    }
  }
}
