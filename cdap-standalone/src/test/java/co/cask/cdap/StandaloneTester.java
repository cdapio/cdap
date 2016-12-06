/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * This class helps writing tests that needs {@link StandaloneMain} up and running.
 */
public class StandaloneTester extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneTester.class);

  private final TemporaryFolder tmpFolder = new TemporaryFolder();
  private final Object [] configs;
  private CConfiguration cConf;
  private StandaloneMain standaloneMain;

  public StandaloneTester(Object ... configs) {
    Preconditions.checkArgument(configs.length % 2 == 0,
                                "Arguments must be in pair form like (k1, v1, k2, v2): %s", Arrays.toString(configs));
    this.configs = configs;
  }

  @Override
  protected void before() throws Throwable {
    tmpFolder.create();

    CConfiguration cConf = CConfiguration.create();

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.Router.ADDRESS, getLocalHostname());
    cConf.setInt(Constants.Router.ROUTER_PORT, Networks.getRandomPort());
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    cConf.setBoolean(Constants.Explore.START_ON_DEMAND, false);
    cConf.setBoolean(StandaloneMain.DISABLE_UI, true);
    cConf.setBoolean(Constants.Audit.ENABLED, false);

    for (int i = 0; i < configs.length; i += 2) {
      cConf.set(configs[i].toString(), configs[i + 1].toString());
    }
    this.cConf = cConf;

    // Start standalone
    standaloneMain = StandaloneMain.create(cConf, new Configuration());
    standaloneMain.startUp();
    try {
      waitForStandalone();
    } catch (Throwable t) {
      standaloneMain.shutDown();
      throw t;
    }
  }

  @Override
  protected void after() {
    try {
      if (standaloneMain != null) {
        standaloneMain.shutDown();
      }
    } finally {
      tmpFolder.delete();
    }
  }

  /**
   * Returns the {@link CConfiguration} used by the standalone instance.
   */
  public CConfiguration getConfiguration() {
    Objects.requireNonNull(cConf, "StandaloneTester hasn't been initialized");
    return cConf;
  }

  /**
   * Returns the base URI for making REST calls to the standalone instance.
   */
  public URI getBaseURI() {
    return URI.create(String.format("http://%s:%d/",
                                    getConfiguration().get(Constants.Router.ADDRESS),
                                    getConfiguration().getInt(Constants.Router.ROUTER_PORT)));
  }

  /**
   * Adds a system artifact to CDAP instance that is used for testing.
   */
  public void addSystemArtifact(String name, ArtifactVersion version, File artifactFile,
                                @Nullable Set<ArtifactRange> parentArtifacts) throws Exception {
    ArtifactRepository artifactRepository = standaloneMain.getInjector().getInstance(ArtifactRepository.class);

    ArtifactId artifactId = NamespaceId.SYSTEM.artifact(name, version.getVersion());
    artifactRepository.addArtifact(artifactId, artifactFile, parentArtifacts);
  }

  private String getLocalHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Unable to resolve localhost", e);
      return "127.0.0.1";
    }
  }

  private void waitForStandalone() throws Exception {
    final URL url = getBaseURI().resolve("/ping").toURL();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        try {
          return urlConn.getResponseCode() == HttpURLConnection.HTTP_OK;
        } finally {
          urlConn.disconnect();
        }
      }
    }, 30, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }
}
