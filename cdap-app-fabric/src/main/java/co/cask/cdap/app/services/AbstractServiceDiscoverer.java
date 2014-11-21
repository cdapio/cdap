/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.app.services;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.PayloadDiscoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link ServiceDiscoverer}.
 * It provides definition for {@link ServiceDiscoverer#getServiceURL}  and expects the sub-classes to give definition
 * for {@link AbstractServiceDiscoverer#getDiscoveryServiceClient}.
 */
public abstract class AbstractServiceDiscoverer implements ServiceDiscoverer {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceDiscoverer.class);
  private static final Gson GSON = new Gson();

  protected String accountId;
  protected String applicationId;

  protected AbstractServiceDiscoverer() {
  }

  public AbstractServiceDiscoverer(Program program) {
    this.accountId = program.getAccountId();
    this.applicationId = program.getApplicationId();
  }

  @Override
  public URL getServiceURL(final String applicationId, final String serviceId) {
    ServiceDiscovered serviceDiscovered = getDiscoveryServiceClient().discover(String.format("service.%s.%s.%s",
                                                                                             accountId,
                                                                                             applicationId,
                                                                                             serviceId));
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(serviceDiscovered);
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable != null) {
      return createURL(discoverable, applicationId, serviceId);
    }

    final SynchronousQueue<URL> discoverableQueue = new SynchronousQueue<URL>();
    Cancellable discoveryCancel = serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        try {
          URL url = createURL(serviceDiscovered.iterator().next(), applicationId, serviceId);
          discoverableQueue.offer(url);
        } catch (NoSuchElementException e) {
          LOG.debug("serviceDiscovered is empty");
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    try {
      URL url = discoverableQueue.poll(1, TimeUnit.SECONDS);
      if (url == null) {
        LOG.debug("Discoverable endpoint not found for appID: {}, serviceID: {}.", applicationId, serviceId);
      }
      return url;
    } catch (InterruptedException e) {
      LOG.error("Exception raised when discovering endpoint for {}/{}", applicationId, serviceId, e);
      return null;
    } finally {
      discoveryCancel.cancel();
    }
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return getServiceURL(applicationId, serviceId);
  }

  private URL createURL(@Nullable Discoverable discoverable, String applicationId, String serviceId) {
    if (discoverable == null || !isVisible(discoverable, applicationId)) {
      return null;
    }
    String hostName = discoverable.getSocketAddress().getHostName();
    int port = discoverable.getSocketAddress().getPort();
    String path = String.format("http://%s:%d%s/apps/%s/services/%s/methods/", hostName, port,
                                Constants.Gateway.GATEWAY_VERSION, applicationId, serviceId);
    try {
      return new URL(path);
    } catch (MalformedURLException e) {
      LOG.error("Got exception while creating serviceURL", e);
      return null;
    }
  }

  // Local services are discoverable from programs only within the same application.
  private boolean isVisible(Discoverable discoverable, String applicationId) {
    if (discoverable instanceof PayloadDiscoverable) {
      PayloadDiscoverable payloadDiscoverable = (PayloadDiscoverable) discoverable;
      String encodedACLS = Bytes.toString(payloadDiscoverable.getPayload());
      List<ACL> acls = GSON.fromJson(encodedACLS, new TypeToken<List<ACL>>() { }.getType());
      // If no ACLs are set, or its in the same application, then it is visible
      if (acls.isEmpty() || this.applicationId.equals(applicationId)) {
        return true;
      } else {
        // If they're in different applications, and the ACL is set to execute only within the same app, its not visible
        for (ACL acl : acls) {
          if (acl.getPrincipal() == null && acl.getPermissions().contains(PermissionType.EXECUTE)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * @return the {@link DiscoveryServiceClient} for Service Discovery
   */
  protected abstract DiscoveryServiceClient getDiscoveryServiceClient();
}
