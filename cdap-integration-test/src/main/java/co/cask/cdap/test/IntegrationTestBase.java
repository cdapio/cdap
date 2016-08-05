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

package co.cask.cdap.test;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.client.MonitorClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.SecureStoreClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.ConfigEntry;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Abstract class for writing Integration tests for CDAP. Provides utility methods to use in Integration tests.
 * Users should extend this class in their test classes.
 */
public abstract class IntegrationTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class);
  private static final long SERVICE_CHECK_TIMEOUT_SECONDS = TimeUnit.MINUTES.toSeconds(10);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private AccessToken accessToken;

  @Before
  public void setUp() throws Exception {
    checkSystemServices();
    assertUnrecoverableResetEnabled();
    assertIsClear();
  }

  protected void checkSystemServices() throws TimeoutException, InterruptedException {
    Callable<Boolean> cdapAvailable = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        // first wait for all system services to be 'OK'
        if (!getMonitorClient().allSystemServicesOk()) {
          return false;
        }
        // Check that the dataset service is up, and also that the default namespace exists
        // Using list and checking if default namespace exists, as opposed to using get()
        // so we don't have to unnecessarily add a try-catch for NamespaceNotFoundException, since that exception is
        // not handled in checkServicesWithRetry.
        List<NamespaceMeta> namespaces = getNamespaceClient().list();

        if (namespaces.size() == 0) {
          return false;
        }
        if (namespaces.contains(NamespaceMeta.DEFAULT)) {
          return true;
        }
        throw new IllegalStateException("Default namespace not found. Instead found unexpected namespaces: "
                                          + namespaces);
      }
    };

    String errorMessage = String.format("CDAP Services are not available. Retried for %s seconds.",
                                        SERVICE_CHECK_TIMEOUT_SECONDS);
    try {
      checkServicesWithRetry(cdapAvailable, errorMessage);
    } catch (Throwable e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof UnauthenticatedException) {
        // security is enabled, we need to get access token before checking system services
        try {
          accessToken = fetchAccessToken();
        } catch (IOException ex) {
          throw Throwables.propagate(ex);
        }
        checkServicesWithRetry(cdapAvailable, errorMessage);
      } else {
        throw Throwables.propagate(rootCause);
      }
    }
    LOG.info("CDAP Services are up and running!");
  }

  /**
   * Uses BasicAuthenticationClient to fetch {@link AccessToken} - this implementation can be overridden if desired.
   *
   * @return {@link AccessToken}
   * @throws IOException
   * @throws TimeoutException if a timeout occurs while getting an access token
   */
  protected AccessToken fetchAccessToken() throws IOException, TimeoutException, InterruptedException {
    Properties properties = new Properties();
    properties.setProperty("security.auth.client.username", System.getProperty("cdap.username"));
    properties.setProperty("security.auth.client.password", System.getProperty("cdap.password"));
    final AuthenticationClient authClient = new BasicAuthenticationClient();
    authClient.configure(properties);
    ConnectionConfig connectionConfig = getClientConfig().getConnectionConfig();
    authClient.setConnectionInfo(connectionConfig.getHostname(), connectionConfig.getPort(), false);

    checkServicesWithRetry(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return authClient.getAccessToken() != null;
      }
    }, "Unable to connect to Authentication service to obtain access token, Connection info : " + connectionConfig);

    return authClient.getAccessToken();
  }


  private void checkServicesWithRetry(Callable<Boolean> callable,
                                      String exceptionMessage) throws TimeoutException, InterruptedException {
    Stopwatch sw = new Stopwatch().start();
    do {
      try {
        if (callable.call()) {
          return;
        }
      } catch (IOException e) {
        // We want to suppress and retry on IOException
      } catch (Throwable e) {
        // Also suppress and retry if the root cause is IOException
        Throwable rootCause = Throwables.getRootCause(e);
        if (!(rootCause instanceof IOException)) {
          // Throw if root cause is any other exception e.g. UnauthenticatedException
          throw Throwables.propagate(rootCause);
        }
      }
      TimeUnit.SECONDS.sleep(1);
    } while (sw.elapsedTime(TimeUnit.SECONDS) <= SERVICE_CHECK_TIMEOUT_SECONDS);

    // when we have passed the timeout and the check for services is not successful
    throw new TimeoutException(exceptionMessage);
  }

  private void assertUnrecoverableResetEnabled() throws IOException, UnauthenticatedException {
    ConfigEntry configEntry = getMetaClient().getCDAPConfig().get(Constants.Dangerous.UNRECOVERABLE_RESET);
    Preconditions.checkNotNull(configEntry,
                               "Missing key from CDAP Configuration: {}", Constants.Dangerous.UNRECOVERABLE_RESET);
    Preconditions.checkState(Boolean.parseBoolean(configEntry.getValue()), "UnrecoverableReset not enabled.");
  }

  @After
  public void tearDown() throws Exception {
    getTestManager().clear();
    assertIsClear();
  }

  protected TestManager getTestManager() {
    try {
      return new IntegrationTestManager(getClientConfig(), getRestClient(), TEMP_FOLDER.newFolder());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Reads the CDAP instance URI from the system property "instanceUri".
   * "instanceUri" should be specified in the format [host]:[port].
   * Defaults to "localhost:10000".
   */
  protected String getInstanceURI() {
    return System.getProperty("instanceUri", "localhost:10000");
  }

  /**
   * CDAP access token for making requests to secure CDAP instances.
   */
  public AccessToken getAccessToken() {
    return accessToken;
  }

  private void assertIsClear() throws Exception {
    Id.Namespace namespace = Id.Namespace.DEFAULT;

    // only namespace existing should be 'default'
    NamespaceClient namespaceClient = getNamespaceClient();
    List<NamespaceMeta> list = namespaceClient.list();
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(NamespaceMeta.DEFAULT, list.get(0));

    assertNoApps(namespace);
    assertNoUserDatasets(namespace);
    assertNoStreams(namespace);
    assertNoSecureKeys(namespace.toEntityId());
    // TODO: check metrics, etc.
  }

  protected ClientConfig getClientConfig() {
    ClientConfig.Builder builder = new ClientConfig.Builder();
    builder.setConnectionConfig(InstanceURIParser.DEFAULT.parse(
      URI.create(getInstanceURI()).toString()));

    if (accessToken != null) {
      builder.setAccessToken(accessToken);
    }

    String verifySSL = System.getProperty("verifySSL");
    if (verifySSL != null) {
      builder.setVerifySSLCert(Boolean.valueOf(verifySSL));
    }

    builder.setDefaultConnectTimeout(120000);
    builder.setDefaultReadTimeout(120000);
    builder.setUploadConnectTimeout(0);
    builder.setUploadReadTimeout(0);

    return builder.build();
  }

  protected RESTClient getRestClient() {
    return new RESTClient(getClientConfig());
  }

  protected MetaClient getMetaClient() {
    return new MetaClient(getClientConfig(), getRestClient());
  }

  protected NamespaceClient getNamespaceClient() {
    return new NamespaceClient(getClientConfig(), getRestClient());
  }

  @SuppressWarnings("unused")
  protected MetricsClient getMetricsClient() {
    return new MetricsClient(getClientConfig(), getRestClient());
  }

  protected MonitorClient getMonitorClient() {
    return new MonitorClient(getClientConfig(), getRestClient());
  }

  protected ApplicationClient getApplicationClient() {
    return new ApplicationClient(getClientConfig(), getRestClient());
  }

  @SuppressWarnings("unused")
  protected ProgramClient getProgramClient() {
    return new ProgramClient(getClientConfig(), getRestClient());
  }

  protected StreamClient getStreamClient() {
    return new StreamClient(getClientConfig(), getRestClient());
  }

  protected SecureStoreClient getSecureStoreClient() {
    return new SecureStoreClient(getClientConfig(), getRestClient());
  }

  protected DatasetClient getDatasetClient() {
    return new DatasetClient(getClientConfig(), getRestClient());
  }

  protected Id.Namespace createNamespace(String name) throws Exception {
    Id.Namespace namespace = new Id.Namespace(name);
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(namespace).build();
    getTestManager().createNamespace(namespaceMeta);
    return namespace;
  }

  protected ApplicationManager deployApplication(Id.Namespace namespace,
                                                 Class<? extends Application> applicationClz,
                                                 File...bundleEmbeddedJars) throws IOException {
    return getTestManager().deployApplication(namespace, applicationClz, bundleEmbeddedJars);
  }

  protected ApplicationManager deployApplication(Class<? extends Application> applicationClz) throws IOException {
    return deployApplication(Id.Namespace.DEFAULT, applicationClz);
  }

  protected ApplicationManager getApplicationManager(ApplicationId applicationId) throws Exception {
    return getTestManager().getApplicationManager(applicationId);
  }

  private boolean isUserDataset(DatasetSpecificationSummary specification) {
    final DefaultDatasetNamespace dsNamespace = new DefaultDatasetNamespace(CConfiguration.create());
    return !dsNamespace.contains(specification.getName(), Id.Namespace.SYSTEM.getId());
  }

  private void assertNoUserDatasets(Id.Namespace namespace) throws Exception {
    DatasetClient datasetClient = getDatasetClient();
    List<DatasetSpecificationSummary> datasets = datasetClient.list(namespace);

    Iterable<DatasetSpecificationSummary> userDatasets = Iterables.filter(
      datasets, new Predicate<DatasetSpecificationSummary>() {
        @Override
        public boolean apply(DatasetSpecificationSummary input) {
          return isUserDataset(input);
        }
    });

    Iterable<String> userDatasetNames = Iterables.transform(
      userDatasets, new Function<DatasetSpecificationSummary, String>() {
        @Override
        public String apply(DatasetSpecificationSummary input) {
          return input.getName();
        }
    });

    Assert.assertFalse("Must have no user datasets, but found the following user datasets: "
                         + Joiner.on(", ").join(userDatasetNames), userDatasets.iterator().hasNext());
  }

  private void assertNoApps(Id.Namespace namespace) throws Exception {
    ApplicationClient applicationClient = getApplicationClient();
    List<ApplicationRecord> applicationRecords = applicationClient.list(namespace);
    List<String> applicationIds = Lists.newArrayList();
    for (ApplicationRecord applicationRecord : applicationRecords) {
      applicationIds.add(applicationRecord.getName());
    }

    Assert.assertTrue("Must have no deployed apps, but found the following apps: "
                        + Joiner.on(", ").join(applicationIds), applicationRecords.isEmpty());
  }

  private void assertNoStreams(Id.Namespace namespace) throws Exception {
    List<StreamDetail> streams = getStreamClient().list(namespace);
    List<String> streamNames = Lists.newArrayList();
    for (StreamDetail stream : streams) {
      streamNames.add(stream.getName());
    }
    Assert.assertTrue("Must have no streams, but found the following streams: "
                        + Joiner.on(", ").join(streamNames), streamNames.isEmpty());
  }

  private void assertNoSecureKeys(NamespaceId namespaceId) throws Exception {
    List<SecureKeyListEntry> secureKeys = getSecureStoreClient().listKeys(namespaceId);
    List<String> keyNames = new ArrayList<>(secureKeys.size());
    for (SecureKeyListEntry entry : secureKeys) {
      keyNames.add(entry.getName());
    }
    Assert.assertTrue(String.format("Must have no secure keys, but found the following keys: %s",
                                    Joiner.on(", ").join(keyNames)), keyNames.isEmpty());
  }
}
