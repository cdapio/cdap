package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.security.auth.KeyManager;
import com.continuuity.security.auth.SharedKeyManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Guice module for testing SharedKeyKeyManagers. Modifies functionality to write keys to a temporary folder.
 */
public class SharedKeySecurityTestModule extends SecurityModule {
  private TemporaryFolder temporaryFolder;

  public SharedKeySecurityTestModule(TemporaryFolder temporaryFolder) {
    this.temporaryFolder = temporaryFolder;
  }

  @Override
  protected Provider<KeyManager> getKeyManagerProvider() {
    class SharedKeyManagerProvider implements Provider<KeyManager> {
      private CConfiguration cConf = CConfiguration.create();

      @Inject(optional = true)
      public void setCConfiguration(CConfiguration conf) {
        this.cConf = conf;
      }

      @Override
      public KeyManager get() {
        // Set up the configuration to write the keyfile to a temporary folder.
        if (cConf == null) {
          cConf = SharedKeySecurityTestModule.super.cConf;
        }
        cConf.set(Constants.Security.CFG_SHARED_KEYFILE_DIR, temporaryFolder.getRoot().getPath());

        SharedKeyManager keyManager = new SharedKeyManager(cConf);
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
    return new SharedKeyManagerProvider();
  }
}
