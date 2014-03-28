package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.security.auth.KeyManager;
import com.continuuity.security.auth.TokenManager;
import com.google.common.base.Throwables;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

import java.security.NoSuchAlgorithmException;

/**
 * Guice bindings for security related classes.  This extends {@code PrivateModule} in order to limit which classes
 * are exposed.
 */
public class SecurityModule extends PrivateModule {
  private final CConfiguration conf;

  public SecurityModule() {
    this(CConfiguration.create());
  }

  public SecurityModule(CConfiguration conf) {
    this.conf = conf;
  }

  @Override
  protected void configure() {
    KeyManager keyManager = new KeyManager(conf);
    try {
      keyManager.init();
    } catch (NoSuchAlgorithmException nsae) {
      throw Throwables.propagate(nsae);
    }
    bind(KeyManager.class).toInstance(keyManager);
    bind(TokenManager.class).in(Scopes.SINGLETON);
    expose(TokenManager.class);
  }
}
