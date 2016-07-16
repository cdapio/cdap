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

package co.cask.cdap.common.metadata;

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * /**
 * Implementation of metadataClient that uses {@link org.apache.twill.discovery.DiscoveryServiceClient} to dictate the resolution of metadata
 * resources.
 */
public class DiscoveryMetadataClient extends AbstractMetadataClient {
  private final Supplier<EndpointStrategy> endpointStrategySupplier;

  @Inject
  public DiscoveryMetadataClient(final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP));
      }
    });
  }


  @Override
  protected HttpResponse execute(HttpRequest request,  int... allowedErrorCodes) throws IOException {
    return HttpRequests.execute(request);
  }

  @Override
  protected URL resolve(Id.Namespace namespace, String resource) throws MalformedURLException {
    InetSocketAddress addr = getMetadataServiceAddress();
    String url = String.format("http://%s:%d%s/%s", addr.getHostName(), addr.getPort(),
                               Constants.Gateway.API_VERSION_3,resource); //Not sure
    return new URL(url);
  }

  private InetSocketAddress getMetadataServiceAddress() {
    Discoverable discoverable = endpointStrategySupplier.get().pick(3L, TimeUnit.SECONDS);
    if (discoverable != null) {
      return discoverable.getSocketAddress();
    }
    throw new ServiceUnavailableException(Constants.Service.METADATA_SERVICE);
  }

}
