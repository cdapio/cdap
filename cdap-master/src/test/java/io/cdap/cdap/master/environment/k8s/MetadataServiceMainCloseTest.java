/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.cdap.cdap.internal.metadata.MetadataConsumerSubscriberService;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Fast unit test verifying {@link MetadataServiceMain} closes {@link MetadataStorage} quietly.
 */
public class MetadataServiceMainCloseTest {

  @Test
  public void testAddServicesClosesMetadataStorageQuietlyOnIOException() throws Exception {
    Injector injector = Mockito.mock(Injector.class);
    Mockito.when(injector.getInstance(MetadataService.class))
        .thenReturn(Mockito.mock(MetadataService.class));
    Mockito.when(injector.getInstance(MetadataSubscriberService.class))
        .thenReturn(Mockito.mock(MetadataSubscriberService.class));
    Mockito.when(injector.getInstance(MetadataConsumerSubscriberService.class))
        .thenReturn(Mockito.mock(MetadataConsumerSubscriberService.class));
    Mockito.when(injector.getExistingBinding(Key.get(ZKClientService.class))).thenReturn(null);

    MetadataStorage failingStorage = Mockito.mock(MetadataStorage.class);
    Mockito.doAnswer(inv -> sneakyThrow(new IOException("storage close failure")))
        .when(failingStorage).close();
    Mockito.when(injector.getInstance(MetadataStorage.class)).thenReturn(failingStorage);

    MetadataServiceMain serviceMain = new MetadataServiceMain();
    List<Service> services = new ArrayList<>();
    List<AutoCloseable> closeables = new ArrayList<>();

    serviceMain.addServices(
        injector, services, closeables, null, null, new EnvironmentOptions());

    Assert.assertEquals(4, services.size());
    Service storageCloserService = services.get(services.size() - 1);
    storageCloserService.startAndWait();
    Assert.assertEquals(Service.State.RUNNING, storageCloserService.state());

    storageCloserService.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, storageCloserService.state());
    Mockito.verify(failingStorage).close();
  }

  @SuppressWarnings("unchecked")
  private static <T, E extends Throwable> T sneakyThrow(Throwable t) throws E {
    throw (E) t;
  }
}
