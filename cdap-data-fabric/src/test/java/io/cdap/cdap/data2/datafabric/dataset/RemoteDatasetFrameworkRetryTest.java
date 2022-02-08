/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset;

import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import org.mockito.Mockito;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class RemoteDatasetFrameworkRetryTest extends RemoteDatasetFrameworkTest {

  @Override
  protected RemoteDatasetFramework createFramework(AuthenticationContext authenticationContext,
                                                   RemoteClientFactory remoteClientFactory) {
    cConf.set("system.dataset.remote.retry.policy.base.delay.ms", "0");
    cConf.set("system.dataset.remote.retry.policy.max.retries", "1");
    RemoteClientFactory mockedFactory = Mockito.spy(remoteClientFactory);
    Set<URL> failedURIs = new HashSet<>();
    Mockito.doAnswer(i -> {
      RemoteClient realClient = (RemoteClient) i.callRealMethod();
      RemoteClient mocked = Mockito.spy(realClient);
      Mockito.doAnswer(i2 -> {
        HttpRequest request = i2.getArgumentAt(0, HttpRequest.class);
        //Fail the first GET, allow the second
        if (request.getMethod() == HttpMethod.GET && failedURIs.add(request.getURL())) {
          throw new ServiceUnavailableException("service");
        }
        return i2.callRealMethod();
      }).when(mocked).execute(Mockito.any());
      return mocked;
    }).when(mockedFactory).createRemoteClient(Mockito.any(), Mockito.any(), Mockito.any());
    return super.createFramework(authenticationContext, mockedFactory);
  }
}
