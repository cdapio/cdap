package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.metadata.MetaDataStore;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.continuuity.metadata.types.Dataset;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import java.util.List;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Handles data fabric clear calls.
 */
@Path("/v2")
public class ClearFabricHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ClearFabricHandler.class);

  private final MetaDataTable metadataTable;
  private final MetaDataStore metaDataStore;
  private final QueueAdmin queueAdmin;
  private final DataSetInstantiatorFromMetaData datasetInstantiator;
  private final DataSetAccessor dataSetAccessor;

  @Inject
  public ClearFabricHandler(MetaDataTable metaDataTable, MetaDataStore metaDataStore,
                            QueueAdmin queueAdmin, DataSetInstantiatorFromMetaData datasetInstantiator,
                            DataSetAccessor dataSetAccessor, GatewayAuthenticator authenticator) {
    super(authenticator);
    this.metadataTable = metaDataTable;
    this.metaDataStore = metaDataStore;
    this.queueAdmin = queueAdmin;
    this.datasetInstantiator = datasetInstantiator;
    this.dataSetAccessor = dataSetAccessor;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting ClearFabricHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping ClearFabricHandler");
  }

  @DELETE
  @Path("/meta")
  public void clearMeta(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.META);
  }

  @DELETE
  @Path("/datasets")
  public void clearTables(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.TABLES);
  }

  @DELETE
  @Path("/queues")
  public void clearQueues(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.QUEUES);
  }

  @DELETE
  @Path("/streams")
  public void clearStreams(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.STREAMS);
  }

  @DELETE
  @Path("/all")
  public void clearAll(HttpRequest request, final HttpResponder responder) {
    clear(request, responder, ToClear.ALL);
  }

  private static enum ToClear {
    DATA, META, TABLES, QUEUES, STREAMS, ALL
  }

  private void clear(HttpRequest request, final HttpResponder responder, ToClear toClear) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      OperationContext context = new OperationContext(accountId);

      try {
        // remove from ds2 if needed (it uses mds, so doing it before mds cleanup)
        if (toClear == ToClear.ALL || toClear == ToClear.TABLES) {
          removeDs2Tables(context.getAccount(), context);
          // todo: remove all user tables using DataSetAccessor?
        }
        if (toClear == ToClear.ALL || toClear == ToClear.META) {
          metadataTable.clear(context, context.getAccount(), null);
        }
        if (toClear == ToClear.ALL || toClear == ToClear.QUEUES) {
          queueAdmin.dropAll();
        }
        if (toClear == ToClear.ALL || toClear == ToClear.STREAMS) {
          // NOTE: for now we store all streams data in same queue table, TODO: fix this
          queueAdmin.dropAll();
        }

        responder.sendStatus(OK);
      } catch (Exception e) {
        LOG.trace("Exception clearing data fabric: ", e);
        responder.sendStatus(INTERNAL_SERVER_ERROR);
      }

    } catch (SecurityException e) {
      responder.sendStatus(FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(BAD_REQUEST);
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  private void removeDs2Tables(String account, OperationContext context) throws Exception {
    List<Dataset> datasets = metaDataStore.getDatasets(account);
    for (Dataset ds : datasets) {
      removeDataset(ds.getName(), context);
    }
  }

  private void removeDataset(String datasetName, OperationContext opContext) throws OperationException {
    // NOTE: for now we just try to do the best we can: find all used DataSets of type Table and remove them. This
    //       should be done better, when we refactor DataSet API (towards separating user API and management parts)
    DataSet dataSet = datasetInstantiator.getDataSet(datasetName, opContext);
    DataSetSpecification config = dataSet.configure();
    List<DataSetSpecification> allDataSets = DatasetHandler.getAllUsedDataSets(config);
    for (DataSetSpecification spec : allDataSets) {
      DataSet ds = datasetInstantiator.getDataSet(spec.getName(), opContext);
      try {
        DataSetManager dataSetManager = dataSetAccessor.getDataSetManager(OrderedColumnarTable.class,
                                                                          DataSetAccessor.Namespace.USER);
        dataSetManager.drop(ds.getName());
      } catch (Exception e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, "failed to truncate table: " + ds.getName(), e);
      }
    }
  }

}

