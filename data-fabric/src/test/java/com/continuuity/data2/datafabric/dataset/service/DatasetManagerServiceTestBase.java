package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.manager.inmemory.InMemoryDatasetManager;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
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
import java.net.InetAddress;
import javax.annotation.Nullable;

/**
 * Base class for unit-tests that require running of {@link DatasetManagerService}
 */
public abstract class DatasetManagerServiceTestBase {
  private static final Gson GSON = new Gson();

  private DatasetManagerService service;
  private InMemoryTransactionManager txManager;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File datasetDir = new File(tmpFolder.newFolder(), "dataset");
    if (!datasetDir.mkdirs()) {
      throw new RuntimeException(String.format("Could not create DatasetManager output dir %s", datasetDir.getPath()));
    }
    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());

    // Starting DatasetManagerService service
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();

    // Tx Manager to support working with datasets
    txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    service = new DatasetManagerService(cConf,
                                        new LocalLocationFactory(),
                                        InetAddress.getByName("localhost"),
                                        discoveryService,
                                        new InMemoryDatasetManager(),
                                        ImmutableSortedMap.<String, Class<? extends DatasetModule>>of(
                                          "memoryTable", InMemoryTableModule.class),
                                        txSystemClient);
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

  protected static String getUrl(String resource) {
    return "http://" + "localhost" + ":" + Constants.Dataset.Manager.DEFAULT_PORT +
      "/" + Constants.Dataset.Manager.VERSION + resource;
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
