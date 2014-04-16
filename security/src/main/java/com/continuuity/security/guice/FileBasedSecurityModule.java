package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.security.auth.FileBasedKeyManager;
import com.continuuity.security.auth.KeyManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 *
 */
public class FileBasedSecurityModule extends SecurityModule {

  @Override
  protected Provider<KeyManager> getKeyManagerProvider() {
    class FileBasedKeyManagerProvider implements Provider<KeyManager> {
      private CConfiguration cConf = CConfiguration.create();

      @Inject(optional = true)
      public void setCConfiguration(CConfiguration conf) {
        this.cConf = conf;
      }

      @Override
      public KeyManager get() {
        FileBasedKeyManager keyManager = new FileBasedKeyManager(cConf);
        try {
          keyManager.init();
        } catch (NoSuchAlgorithmException nsae) {
          throw Throwables.propagate(nsae);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
        return keyManager;
      }
    }
    return new FileBasedKeyManagerProvider();
  }
}
