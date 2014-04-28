package com.continuuity.data2.dataset2.lib.continuuity;

import com.continuuity.common.conf.CConfiguration;

/**
 * Implementation of this interface needs access to {@link CConfiguration}.
 */
// todo: datasets should not depend on continuuity configuration!
public interface CConfigurationAware {
  /**
   * Sets instances of {@link CConfiguration}
   * @param conf configuration to set
   */
  public void setCConfiguration(CConfiguration conf);
}
