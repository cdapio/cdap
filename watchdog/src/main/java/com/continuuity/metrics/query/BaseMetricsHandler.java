/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
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
