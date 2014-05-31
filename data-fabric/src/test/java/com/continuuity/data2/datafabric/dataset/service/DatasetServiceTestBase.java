package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.datafabric.dataset.service.executor.NoOpDatasetOpExecutor;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * Base class for unit-tests that require running of {@link DatasetService}
 */
public abstract class DatasetServiceTestBase {
  private static final Gson GSON = new Gson();

  private int port;
  private DatasetService service;
  private InMemoryTransactionManager txManager;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File datasetDir = new File(tmpFolder.newFolder(), "dataset");
    if (!datasetDir.mkdirs()) {
      throw
        new RuntimeException(String.format("Could not create DatasetFramework output dir %s", datasetDir.getPath()));
    }
    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    port = Networks.getRandomPort();
    cConf.setInt(Constants.Dataset.Manager.PORT, port);

    // Starting DatasetService service
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();

    // Tx Manager to support working with datasets
    txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    service = new DatasetService(cConf,
                                        new LocalLocationFactory(),
                                        discoveryService,
                                        discoveryService,
                                        new InMemoryDatasetFramework(),
                                        ImmutableSortedMap.<String, Class<? extends DatasetModule>>of(
                                          "memoryTable", InMemoryTableModule.class),
                                        txSystemClient,
                                        metricsCollectionService,
                                        new NoOpDatasetOpExecutor());
    service.startAndWait();
  }

  @After
  public void after() {
    try {
      service.stopAndWait();
    } finally {
      txManager.stopAndWait();
    }
  }

  protected String getUrl(String resource) {
    return "http://" + "localhost" + ":" + port + Constants.Gateway.GATEWAY_VERSION + resource;
  }

  // todo: use HttpUrlConnection
  protected int deployModule(String moduleName, Class moduleClass) throws IOException {
    String jarPath = JarFinder.getJar(moduleClass);

    HttpPost post = new HttpPost(getUrl("/data/modules/" + moduleName));
    post.setEntity(new FileEntity(new File(jarPath), "application/octet-stream"));
    post.addHeader("class-name", moduleClass.getName());

    DefaultHttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(post);

    return response.getStatusLine().getStatusCode();
  }

  // todo: use HttpUrlConnection
  protected int deleteModule(String moduleName) throws IOException {
    HttpDelete delete = new HttpDelete(getUrl("/data/modules/" + moduleName));
    HttpResponse response = new DefaultHttpClient().execute(delete);
    return response.getStatusLine().getStatusCode();
  }

  // todo: use HttpUrlConnection
  protected int deleteModules() throws IOException {
    HttpDelete delete = new HttpDelete(getUrl("/data/modules"));
    HttpResponse response = new DefaultHttpClient().execute(delete);
    return response.getStatusLine().getStatusCode();
  }

  @SuppressWarnings("unchecked")
  protected static <T> Response<T> parseResponse(HttpResponse response, Type typeOfT) throws IOException {
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    return new Response<T>(response.getStatusLine().getStatusCode(), (T) GSON.fromJson(reader, typeOfT));
  }

  static final class Response<T> {
    final int status;
    @Nullable
    final T value;

    private Response(int status, @Nullable T value) {
      this.status = status;
      this.value = value;
    }
  }
}
