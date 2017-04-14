/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.common.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;

/**
 * Guice {@link Provider} for {@link FileContext} created created with {@link UserGroupInformation} of
 * {@link Constants#CFG_HDFS_USER}
 */
public class FileContextProvider implements Provider<FileContext> {

  private static final Logger LOG = LoggerFactory.getLogger(FileContextProvider.class);

  private final CConfiguration cConf;
  private final Configuration hConf;

  @Inject
  public FileContextProvider(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Override
  public FileContext get() {
    return createUGI().doAs(new PrivilegedAction<FileContext>() {
      @Override
      public FileContext run() {
        try {
          return FileContext.getFileContext(hConf);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  private UserGroupInformation createUGI() {
    String hdfsUser = cConf.get(Constants.CFG_HDFS_USER);
    try {
      if (hdfsUser == null || UserGroupInformation.isSecurityEnabled()) {
        if (hdfsUser != null) {
          LOG.debug("Ignoring configuration {}={}, running on secure Hadoop", Constants.CFG_HDFS_USER, hdfsUser);
        }
        LOG.debug("Getting filesystem for current user");
        return UserGroupInformation.getCurrentUser();
      } else {
        LOG.debug("Getting filesystem for user {}", hdfsUser);
        return UserGroupInformation.createRemoteUser(hdfsUser);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
