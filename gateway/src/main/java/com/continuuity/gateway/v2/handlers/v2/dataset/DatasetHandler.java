package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.TruncateTable;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Handles dataset calls.
 */
@Path("/v2")
public class DatasetHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetHandler.class);

  private final DataSetInstantiatorFromMetaData datasetInstantiator;
  private final OperationExecutor opex;
  private final DataSetAccessor dataSetAccessor;

  @Inject
  public DatasetHandler(GatewayAuthenticator authenticator,
                        DataSetInstantiatorFromMetaData datasetInstantiator,
                        OperationExecutor opex,
                        DataSetAccessor dataSetAccessor) {
    super(authenticator);
    this.datasetInstantiator = datasetInstantiator;
    this.opex = opex;
    this.dataSetAccessor = dataSetAccessor;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting DatasetHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping DatasetHandler");
  }

  @DELETE
  @Path("/datasets/{name}/truncate")
  public void truncate(HttpRequest request, final HttpResponder responder,
                             @PathParam("name") String tableName) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      try {
        truncateTable(tableName, new OperationContext(accountId));
        responder.sendStatus(OK);
      } catch (OperationException e) {
        String errorMessage = "could not truncate dataset " + tableName + "\n" + StackTraceUtil.toStringStackTrace(e);
        LOG.error(errorMessage);
        responder.sendStatus(CONFLICT);
      }
    } catch (DataSetInstantiationException e) {
      LOG.trace("Cannot instantiate table {}", tableName, e);
      responder.sendStatus(NOT_FOUND);
    } catch (SecurityException e) {
      responder.sendStatus(FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(BAD_REQUEST);
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  private void truncateTable(String tableName, OperationContext opContext) throws OperationException {
    // NOTE: for now we just try to do the best we can: find all used DataSets of type Table and truncate them. This
    //       should be done better, when we refactor DataSet API (towards separating user API and management parts)
    DataSet dataSet = datasetInstantiator.getDataSet(tableName, opContext);
    DataSetSpecification config = dataSet.configure();
    List<DataSetSpecification> allDataSets = getAllUsedDataSets(config);
    for (DataSetSpecification spec : allDataSets) {
      DataSet ds = datasetInstantiator.getDataSet(spec.getName(), opContext);
      if (ds instanceof Table) {
        opex.execute(opContext, new TruncateTable(ds.getName()));
      }
      // also truncating using TxDs2
      try {
        DataSetManager dataSetManager =
          dataSetAccessor.getDataSetManager(OrderedColumnarTable.class);
        dataSetManager.truncate(ds.getName());
      } catch (Exception e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, "failed to truncate table: " + ds.getName(), e);
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
