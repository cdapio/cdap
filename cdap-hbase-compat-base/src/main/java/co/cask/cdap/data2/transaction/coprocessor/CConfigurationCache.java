/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.coprocessor;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.CConfigurationReader;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * {@link Thread} that refreshes {@link CConfiguration} periodically.
 */
public class CConfigurationCache extends AbstractIdleService {
  private static final Log LOG = LogFactory.getLog(CConfigurationCache.class);

  private static final String CCONF_UPDATE_PERIOD = "cdap.transaction.coprocessor.configuration.update.period.secs";
  private static final Long DEFAULT_CCONF_UPDATE_PERIOD = TimeUnit.MINUTES.toMillis(5);

  private final CConfigurationReader cConfReader;
  private final String maxLifetimeProperty;
  private final int defaultMaxLifetime;

  private volatile Thread refreshThread;
  private volatile CConfiguration cConf;
  private volatile boolean stopped;

  private volatile Configuration conf;
  private volatile Long txMaxLifetimeMillis;

  private long cConfUpdatePeriodInMillis = DEFAULT_CCONF_UPDATE_PERIOD;
  private long lastUpdated;

  public CConfigurationCache(Configuration hConf, String sysConfigTablePrefix, String maxLifetimeProperty,
                             int defaultMaxLifetime) {
    this.cConfReader = new CConfigurationReader(hConf, sysConfigTablePrefix);
    this.maxLifetimeProperty = maxLifetimeProperty;
    this.defaultMaxLifetime = defaultMaxLifetime;
  }

  @Nullable
  public CConfiguration getCConf() {
    return cConf;
  }

  @Nullable
  public Configuration getConf() {
    return conf;
  }

  @Nullable
  public Long getTxMaxLifetimeMillis() {
    return txMaxLifetimeMillis;
  }

  public boolean isAlive() {
    return refreshThread != null && refreshThread.isAlive();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting CConfiguration Refresh Thread.");
    startRefreshThread();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping CConfiguration Refresh Thread.");
    stopped = true;
    if (refreshThread != null) {
      refreshThread.interrupt();
      refreshThread.join(TimeUnit.SECONDS.toMillis(1));
    }
  }

  private void startRefreshThread() {
    refreshThread = new Thread("cdap-configuration-cache-refresh") {
      @Override
      public void run() {
        while ((!isInterrupted()) && !stopped) {
          long now = System.currentTimeMillis();
          if (now > (lastUpdated + cConfUpdatePeriodInMillis)) {
            try {
              CConfiguration newCConf = cConfReader.read();
              if (newCConf != null) {
                Configuration newConf = new Configuration();
                newConf.clear();
                for (Map.Entry<String, String> entry : newCConf) {
                  newConf.set(entry.getKey(), entry.getValue());
                }

                // Make the cConf properties available in both formats
                cConf = newCConf;
                conf = newConf;
                txMaxLifetimeMillis = TimeUnit.SECONDS.toMillis(conf.getInt(maxLifetimeProperty, defaultMaxLifetime));
                lastUpdated = now;
                cConfUpdatePeriodInMillis = cConf.getLong(CCONF_UPDATE_PERIOD, DEFAULT_CCONF_UPDATE_PERIOD);
              }
            } catch (TableNotFoundException ex) {
              LOG.warn("CConfiguration table not found.", ex);
              break;
            } catch (IOException ex) {
              LOG.warn("Error updating cConf", ex);
            }
          }

          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException ex) {
            interrupt();
            break;
          }
        }
        LOG.info("CConfiguration update thread terminated.");
      }
    };

    refreshThread.setDaemon(true);
    refreshThread.start();
  }
}
