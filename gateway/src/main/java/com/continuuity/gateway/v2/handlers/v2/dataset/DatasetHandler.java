package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

/**
 * Handles dataset calls.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class DatasetHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetHandler.class);

  private final DataSetInstantiatorFromMetaData datasetInstantiator;
  private final DataSetAccessor dataSetAccessor;
  private final DiscoveryServiceClient discoveryClient;

  @Inject
  public DatasetHandler(GatewayAuthenticator authenticator,
                        DataSetInstantiatorFromMetaData datasetInstantiator,
                        DataSetAccessor dataSetAccessor, DiscoveryServiceClient discoveryClient) {
    super(authenticator);
    this.datasetInstantiator = datasetInstantiator;
    this.dataSetAccessor = dataSetAccessor;
    this.discoveryClient = discoveryClient;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting DatasetHandler");
    EndpointStrategy endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC)), 1L, TimeUnit.SECONDS);
    datasetInstantiator.init(endpointStrategy);
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping DatasetHandler");
  }

  @POST
  @Path("/datasets/{dataset-id}/truncate")
  public void truncate(HttpRequest request, final HttpResponder responder,
                             @PathParam("dataset-id") String tableName) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      try {
        truncateTable(tableName, new OperationContext(accountId));
        responder.sendStatus(OK);
      } catch (OperationException e) {
        LOG.error("could not truncate dataset {}", tableName, e);
        responder.sendStatus(CONFLICT);
      }
    } catch (DataSetInstantiationException e) {
      LOG.trace("Cannot instantiate table {}", tableName, e);
      responder.sendStatus(NOT_FOUND);
    } catch (SecurityException e) {
      responder.sendStatus(UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  private void truncateTable(String tableName, OperationContext opContext) throws OperationException {
    // NOTE: for now we just try to do the best we can: find all used DataSets of type Table and truncate them. This
    //       should be done better, when we refactor DataSet API (towards separating user API and management parts)
    DataSetSpecification config = datasetInstantiator.getDataSetSpecification(tableName, opContext);
    List<DataSetSpecification> allDataSets = getAllUsedDataSets(config);
    for (DataSetSpecification spec : allDataSets) {
      DataSet ds = datasetInstantiator.getDataSet(spec.getName(), opContext);
      if (ds instanceof Table) {
        try {
          DataSetManager dataSetManager =
            dataSetAccessor.getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.USER);
          dataSetManager.truncate(ds.getName());
        } catch (Exception e) {
          throw new OperationException(StatusCode.INTERNAL_ERROR, "failed to truncate table: " + ds.getName(), e);
        }
      }
    }
  }

  static List<DataSetSpecification> getAllUsedDataSets(DataSetSpecification config) {
    List<DataSetSpecification> all = new ArrayList<DataSetSpecification>();
    LinkedList<DataSetSpecification> stack = Lists.newLinkedList();
    stack.add(config);
    while (stack.size() > 0) {
      DataSetSpecification current = stack.removeLast();
      all.add(current);
      Iterable<DataSetSpecification> children = current.getSpecifications();
      if (children != null) {
        for (DataSetSpecification child : children) {
          stack.addLast(child);
        }
      }
    }

    return all;
  }

}
