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
package co.cask.cdap.metrics.query;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.ServerException;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import com.google.common.collect.ImmutableSet;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.net.URI;
import java.util.Set;

/**
 * Base metrics handler that can validate metrics path for existence of elements like streams, datasets, and programs.
 */
public abstract class BaseMetricsHandler extends AuthenticatedHttpHandler {

  private final Set<String> existingServices;

  protected BaseMetricsHandler(Authenticator authenticator) {
    super(authenticator);

    this.existingServices = ImmutableSet.of(Constants.Service.METRICS,
                                            Constants.Service.APP_FABRIC_HTTP,
                                            Constants.Service.DATASET_EXECUTOR,
                                            Constants.Service.DATASET_MANAGER,
                                            Constants.Service.STREAMS,
                                            Constants.Service.GATEWAY);
  }

  protected MetricsRequest parseAndValidate(HttpRequest request, URI requestURI)
    throws MetricsPathException, ServerException {
    ImmutablePair<MetricsRequest, MetricsRequestContext> pair = MetricsRequestParser.parseRequestAndContext(requestURI);
    validatePathElements(request, pair.getSecond());
    return pair.getFirst();
  }

  /**
   * Checks whether the elements (datasets, streams, and programs) in the context exist, throwing
   * a {@link MetricsPathException} with relevant error message if there is an element that does not exist.
   *
   * @param metricsRequestContext context containing elements whose existence we need to check.
   * @throws ServerException
   * @throws MetricsPathException
   */
  protected void validatePathElements(HttpRequest request, MetricsRequestContext metricsRequestContext)
    throws ServerException, MetricsPathException {

    // TODO: we want to check for existence of elements in the path, but be aware of overhead; for now we do only for
    //       services. REACTOR-12
    //       See git history for how it was implemented before: on every metrics request it went to mds and respective
    //       services to check for the existence. Which is not good.

    if (metricsRequestContext.getPathType() != null) {
      if (metricsRequestContext.getPathType().equals(MetricsRequestParser.PathType.SERVICES)) {
        if (!serviceExists(metricsRequestContext.getTypeId())) {
          throw new MetricsPathException(String.format("Service %s does not exist", metricsRequestContext.getTypeId()));
        }
        return;
      }
    }
  }

  private boolean serviceExists(String serviceName) {
    return existingServices.contains(serviceName);
  }

}
