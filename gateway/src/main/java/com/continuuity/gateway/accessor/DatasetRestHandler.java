package com.continuuity.gateway.accessor;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.TruncateTable;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.gateway.util.Util;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.thrift.TException;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 *
 */
public class DatasetRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
    .getLogger(DatasetRestHandler.class);

  /**
   * The allowed methods for this handler.
   */
  static final Set<HttpMethod> ALLOWED_METHODS = Sets.newHashSet(
    HttpMethod.GET,
    HttpMethod.DELETE,
    HttpMethod.PUT,
    HttpMethod.POST);

  // Will help validate URL paths, and also has the name of the connector and
  // the data fabric executor.
  private final DatasetRestAccessor accessor;

  // The metrics object of the rest accessor
  private final CMetrics metrics;

  // This is the prefix that all valid URLs must have.
  private final String pathPrefix;

  /**
   * Constructor requires the accessor that created this.
   *
   * @param accessor the accessor that created this
   */
  DatasetRestHandler(DatasetRestAccessor accessor) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
    this.pathPrefix =
      accessor.getHttpConfig().getPathPrefix() +
        accessor.getHttpConfig().getPathMiddle();
  }

  LinkedList<String> splitPath(String path) {
    LinkedList<String> components = Lists.newLinkedList();
    StringTokenizer tok = new StringTokenizer(path, "/");
    while (tok.hasMoreElements()) {
      components.add(tok.nextToken());
    }
    return components;
  }

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {

    // first decode the request
    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();
    QueryStringDecoder decoder = new QueryStringDecoder(requestUri);
    String path = decoder.getPath();
    Map<String, List<String>> parameters = decoder.getParameters();

    // log and meter the request
    if (LOG.isTraceEnabled()) {
      LOG.trace("Request received: " + method + " " + requestUri);
    }
    MetricsHelper helper = new MetricsHelper(
      this.getClass(), this.metrics, this.accessor.getMetricsQualifier());

    // validate method
    try {
      // check whether the request's HTTP method is supported
      if (!ALLOWED_METHODS.contains(method)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Received a " + method + " request, which is not supported (path is + '" + path + "')");
        }
        respondNotAllowed(message.getChannel(), ALLOWED_METHODS);
        helper.finish(BadRequest);
        return;
      }

      // is this a ping? (http://gw:port/ping) if so respond OK and done
      if ("/ping".equals(requestUri) && HttpMethod.GET.equals(method)) {
        helper.setMethod("ping");
        respondToPing(message.getChannel(), request);
        helper.finish(Success);
        return;
      }

      // check that path begins with prefix
      if (!path.startsWith(this.pathPrefix)) {
        helper.finish(NotFound);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Received a request with unkown path prefix " +
                      "(must be '" + this.pathPrefix + "' but received '" + path + "'.");
        }
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }

      // if authentication is enabled, verify an authentication token has been
      // passed and then verify the token is valid
      if (!accessor.getAuthenticator().authenticateRequest(request)) {
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        helper.finish(BadRequest);
        return;
      }
      String accountId = accessor.getAuthenticator().getAccountId(request);
      if (accountId == null || accountId.isEmpty()) {
        // this should not happen after successful auth... but better to check
        LOG.info("No valid account information found");
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        helper.finish(BadRequest);
        return;
      }
      OperationContext opContext = new OperationContext(accountId);

      LinkedList<String> pathComponents = splitPath(path.substring(this.pathPrefix.length()));
      if (pathComponents.size() == 0) {
        // must be an operation global to data-fabric
        handleGlobalOperation(message, request, helper, parameters, opContext);
        return;
      }

      String datasetType = pathComponents.removeFirst();
      if ("Table".equals(datasetType)) {
        handleTableOperation(message, request, helper, pathComponents, parameters, opContext);
      } else if ("KeyValueTable".equals(datasetType)) {
        // handleKeyValueTableOperation(message, pathComponents, parameters);
      } else if ("DataSet".equals(datasetType)) {
        // This looks ugly: it is not a DataSet type. At the same time it is safe: noone will create DataSet of type
        // DataSet. We put it here because we don't want to break the API. And we are going to refactor it before next
        // release (otherwise this will be hidden from user)
        handleDataSetManagementOperation(message, request, helper, pathComponents, parameters, opContext);
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Received request for unsupported Dataset type '" + datasetType +
                      "', URI is '" + request.getUri() + "'");
        }
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      }

    } catch (Exception e) {
      LOG.error("Exception caught for connector '" + this.accessor.getName() + "'. ", e);
      helper.finish(Error);
      if (message.getChannel().isOpen()) {
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        message.getChannel().close();
      }
    }
  }

  private void handleDataSetManagementOperation(MessageEvent message, HttpRequest request,
                                                MetricsHelper helper, LinkedList<String> pathComponents,
                                                Map<String, List<String>> parameters, OperationContext opContext) {

    // all operations must have dataset name
    if (pathComponents.isEmpty()) {
      respondBadRequest(message, request, helper, "table name missing");
      return;
    }

    if (!HttpMethod.PUT.equals(request.getMethod()) && !HttpMethod.DELETE.equals(request.getMethod())) {
      respondBadRequest(message, request, helper, "Only PUT or DELETE methods supported");
      return;
    }

    String tableName = pathComponents.removeFirst();

    HttpMethod method = request.getMethod();
    if (HttpMethod.PUT.equals(method)) {

      // todo: create dataset
      respondBadRequest(message, request, helper, "creating dataset is not supported", HttpResponseStatus.CONFLICT);

    } else if (HttpMethod.DELETE.equals(method)) {

      // if there's param op=truncate then we do truncate, otherwise we drop dataset
      List<String> opParam = parameters.get("op");
      boolean isTruncate = opParam != null && opParam.size() > 0 && "truncate".equals(opParam.get(0));
      if (isTruncate) {
        try {
          truncateTable(tableName, opContext);
        } catch (OperationException e) {
          String errorMessage = "could not truncate dataset " + tableName + "\n" + StackTraceUtil.toStringStackTrace(e);
          LOG.error(errorMessage);
          respondBadRequest(message, request, helper, errorMessage, HttpResponseStatus.CONFLICT);
          return;
        }
        respondSuccess(message.getChannel(), request);
        helper.finish(Success);
      } else {
        // todo: drop dataset
        respondBadRequest(message, request, helper, "drop of dataset is not supported", HttpResponseStatus.CONFLICT);
        helper.finish(BadRequest);
      }
    }
  }

  private void truncateTable(String tableName, OperationContext opContext) throws OperationException {
    // NOTE: for now we just try to do the best we can: find all used DataSets of type Table and truncate them. This
    //       should be done better, when we refactor DataSet API (towards separating user API and management parts)
    DataSet dataSet = this.accessor.getInstantiator().getDataSet(tableName, opContext);
    DataSetSpecification config = dataSet.configure();
    List<DataSetSpecification> allDataSets = getAllUsedDataSets(config);
    for (DataSetSpecification spec : allDataSets) {
      DataSet ds = this.accessor.getInstantiator().getDataSet(spec.getName(), opContext);
      if (ds instanceof Table) {
        this.accessor.getExecutor().execute(opContext, new TruncateTable(ds.getName()));
      }
      // also truncating using TxDs2
      try {
        DataSetManager dataSetManager =
          this.accessor.getDataSetAccessor().getDataSetManager(OrderedColumnarTable.class);
        dataSetManager.truncate(ds.getName());
      } catch (Exception e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, "failed to truncate table: " + ds.getName(), e);
      }
    }
  }

  private List<DataSetSpecification> getAllUsedDataSets(DataSetSpecification config) {
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

  private enum TableOp {List, Increment, Read, Write, Create, Delete}

  private void handleTableOperation(MessageEvent message, HttpRequest request,
                                    MetricsHelper helper, LinkedList<String> pathComponents,
                                    Map<String, List<String>> parameters, OperationContext opContext) {
    // all operations must have table name
    if (pathComponents.isEmpty()) {
      respondBadRequest(message, request, helper, "table name missing");
      return;
    }
    String tableName = pathComponents.removeFirst();

    // if there is another path component, it must be a row key
    String row = pathComponents.isEmpty() ? null : pathComponents.removeFirst();

    // no more components allowed on path
    if (!pathComponents.isEmpty()) {
      respondBadRequest(message, request, helper, "extra components in path");
      return;
    }

    // is there an operation parameter? Can have only one value, and only list and increment are allowed
    TableOp operation = null;
    List<String> operations = parameters.get("op");
    if (operations != null) {
      if (operations.size() > 1) {
        respondBadRequest(message, request, helper, "more than one 'op' parameter");
        return;
      } else if (operations.size() == 1) {
        String op = operations.get(0);
        if ("list".equals(op)) {
          operation = TableOp.List;
        } else if ("increment".equals(op)) {
          operation = TableOp.Increment;
        } else {
          respondBadRequest(message, request, helper, "unsupported 'op' parameter");
          return;
        }
      }
    }

    // for read and delete operations, optional parameter is columns
    List<String> columns = null;
    List<String> columnParams = parameters.get("columns");
    if (columnParams != null && columnParams.size() > 0) {
      columns = Lists.newLinkedList();
      for (String param : columnParams) {
        Collections.addAll(columns, param.split(","));
      }
    }

    // optional parameters start
    List<String> startParams = parameters.get("start");
    if (startParams != null && startParams.size() > 1) {
      respondBadRequest(message, request, helper, "more than one 'start' parameter");
      return;
    }
    String start = (startParams == null || startParams.isEmpty()) ? null : startParams.get(0);

    // optional parameters stop
    List<String> stopParams = parameters.get("stop");
    if (stopParams != null && stopParams.size() > 1) {
      respondBadRequest(message, request, helper, "more than one 'stop' parameter");
      return;
    }
    String stop = (stopParams == null || stopParams.isEmpty()) ? null : stopParams.get(0);

    // optional parameters limit
    List<String> limitParams = parameters.get("limit");
    if (limitParams != null && limitParams.size() > 1) {
      respondBadRequest(message, request, helper, "more than one 'limit' parameter");
      return;
    }
    Integer limit;
    try {
      limit = (limitParams == null || limitParams.isEmpty()) ? null : Integer.parseInt(limitParams.get(0));
    } catch (NumberFormatException e) {
      respondBadRequest(message, request, helper, "'limit' parameter is not an integer");
      return;
    }

    // optional parameter encoding
    String encoding = null;
    List<String> encodingParams = parameters.get("encoding");
    if (encodingParams != null) {
      // make sure there is at most one
      if (encodingParams.size() > 1) {
        respondBadRequest(message, request, helper, "more than one 'encoding' parameter");
        return;
      }
      // make sure that if there is one, it is supported
      if (!encodingParams.isEmpty()) {
        encoding = encodingParams.get(0);
        if (!Util.supportedEncoding(encoding)) {
          respondBadRequest(message, request, helper, "unsupported 'encoding' parameter");
          return;
        }
      }
    }

    // optional parameter counter - if true, column values are interpreted (and returned) as long numbers
    boolean counter = false;
    List<String> counterParams = parameters.get("counter");
    if (counterParams != null) {
      // make sure there is at most one
      if (counterParams.size() > 1) {
        respondBadRequest(message, request, helper, "more than one 'counter' parameter");
        return;
      }
      // make sure that if there is one, it is supported
      if (!counterParams.isEmpty()) {
        String param = counterParams.get(0);
        counter = "1".equals(param) || "true".equals(param);
      }
    }

    // make sure the operations and parameters are valid
    HttpMethod method = request.getMethod();
    if (HttpMethod.GET.equals(method)) {
      // can be either a list or a read, list would have had ?op=list
      if (operation == TableOp.List) {
        if (row != null) {
          respondBadRequest(message, request, helper, "list operation cannot have row key");
          return;
        } else {
          respondBadRequest(message, request, helper,
                            "list operation not implemented", HttpResponseStatus.NOT_IMPLEMENTED);
          return;
        }
      }
      // make sure no other operation was given with ?op=
      if (operation != null) {
        respondBadRequest(message, request, helper, "invalid operation for method GET");
        return;
      }
      // must be a read, requires a row
      if (row == null) {
        respondBadRequest(message, request, helper, "read must have a row key");
        return;
      }
      if (columns != null && !columns.isEmpty() && (start != null || stop != null)) {
        respondBadRequest(message, request, helper, "read can only specify columns or range");
        return;
      }
      operation = TableOp.Read;

    } else if (HttpMethod.DELETE.equals(method)) {
      // make sure no operation was given with ?op=
      if (operation != null) {
        respondBadRequest(message, request, helper, "invalid operation for method GET");
        return;
      }
      // must be a delete, requires a row
      if (row == null) {
        respondBadRequest(message, request, helper, "delete must have a row key");
        return;
      }
      if (columns == null || columns.isEmpty()) {
        respondBadRequest(message, request, helper, "delete must have columns");
        return;
      }
      operation = TableOp.Delete;

    } else if (HttpMethod.PUT.equals(method)) {
      // make sure no operation was given with ?op=
      if (operation != null) {
        respondBadRequest(message, request, helper, "invalid operation for method PUT");
        return;
      }
      // must be a write, requires a row
      if (row == null) {
        operation = TableOp.Create;
      } else {
        operation = TableOp.Write;
      }

    } else if (HttpMethod.POST.equals(method)) {
      // make sure no operation was given with ?op=
      if (operation == null) {
        respondBadRequest(message, request, helper, "missing operation for method POST");
        return;
      }
      if (operation != TableOp.Increment) {
        respondBadRequest(message, request, helper, "invalid operation for method POST");
        return;
      }
      // must be increment, requires a row
      if (row == null) {
        respondBadRequest(message, request, helper, "increment must have a row key");
        return;
      }
      operation = TableOp.Increment;
    }

    Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
    // Type longMapType = new TypeToken<Map<String, Long>>() {}.getType();

    // for operations write and increment, there must be a JSON string in the body
    Map<String, String> valueMap = null;
    // Map<String, Long> longMap = null;
    try {
      if (operation == TableOp.Increment || operation == TableOp.Write) {
        InputStreamReader reader = new InputStreamReader(
          new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
        if (operation == TableOp.Write) {
          valueMap = new Gson().fromJson(reader, stringMapType);
        } else {
          // does not seem to work, Gson returns Map<String,String>
          // longMap = new Gson().fromJson(reader, longMapType);
          valueMap = new Gson().fromJson(reader, stringMapType);
        }
      }
    } catch (Exception e) {
      // failed to parse json, that is a bad request
      respondBadRequest(message, request, helper, "failed to read body as json: " + e.getMessage());
      return;
    }

    if (operation.equals(TableOp.Create)) {
      DataSetSpecification spec = new Table(tableName).configure();
      Dataset ds = new Dataset(spec.getName());
      ds.setName(spec.getName());
      ds.setType(spec.getType());
      ds.setSpecification(new Gson().toJson(spec));
      try {
        this.accessor.getMetadataService().assertDataset(new Account(opContext.getAccount()), ds);
      } catch (MetadataServiceException e) {
        respondBadRequest(message, request, helper, "table already exists", HttpResponseStatus.CONFLICT);
        return;
      } catch (TException e) {
        helper.finish(Error);
        LOG.error("Thrift error creating table: " + e.getMessage(), e);
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }
      respondSuccess(message.getChannel(), request);
      helper.finish(Success);
      return;
    }

    // make sure the table exists and instantiate dataset
    Table table;
    try {
      table = this.accessor.getInstantiator().getDataSet(tableName, opContext);
    } catch (Exception e) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Cannot instantiate requested table '" + tableName + "' (" +
                    e.getMessage() + ") for URI '" + request.getUri() + "'");
      }
      helper.finish(BadRequest);
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND, "table does not exist");
      return;
    }

    // try to convert the row ket to bytes, using the given encoding
    byte[] rowKey;
    try {
      rowKey = row == null ? null : Util.decodeBinary(row, encoding);
    } catch (Exception e) {
      respondBadRequest(message, request, helper, "error decoding row key", e);
      return;
    }

    if (operation.equals(TableOp.List)) {
      // TODO not implemented

    } else if (operation.equals(TableOp.Read)) {
      Read read;
      try {
        if (columns == null || columns.isEmpty()) {
          // column range
          byte[] startCol = start == null ? null : Util.decodeBinary(start, encoding);
          byte[] stopCol = stop == null ? null : Util.decodeBinary(stop, encoding);
          read = new Read(rowKey, startCol, stopCol, limit == null ? -1 : limit);
        } else {
          byte[][] cols = new byte[columns.size()][];
          int i = 0;
          for (String column : columns) {
            cols[i++] = Util.decodeBinary(column, encoding);
          }
          read = new Read(rowKey, cols);
        }
      } catch (Exception e) {
        respondBadRequest(message, request, helper, "error decoding column key(s)", e);
        return;
      }
      OperationResult<Map<byte[], byte[]>> result;
      try {
        Transaction tx = startTx();
        result = table.read(read);
        commitTx(tx);
      } catch (OperationException e) {
        helper.finish(Error);
        LOG.error("Error during Read: " + e.getMessage(), e);
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }
      // read successful, now respond with result
      if (result.isEmpty() || result.getValue().isEmpty()) {
        helper.finish(NoData);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      } else {
        // result is not empty, now construct a json response
        // first convert the bytes to strings
        Map<String, String> map = Maps.newTreeMap();
        for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
          map.put(Util.encodeBinary(entry.getKey(), encoding), Util.encodeBinary(entry.getValue(), encoding, counter));
        }
        // now write a json string representing the map
        byte[] response = new Gson().toJson(map).getBytes(Charsets.UTF_8);
        respondSuccess(message.getChannel(), request, response);
        helper.finish(Success);
      }

    } else if (operation.equals(TableOp.Delete)) {
      Delete delete;
      try {
        byte[][] cols = new byte[columns.size()][];
        int i = 0;
        for (String column : columns) {
          cols[i++] = Util.decodeBinary(column, encoding);
        }
        delete = new Delete(rowKey, cols);
      } catch (Exception e) {
        respondBadRequest(message, request, helper, "error decoding column key(s)", e);
        return;
      }
      // now execute the delete operation
      try {
        Transaction tx = startTx();
        table.write(delete);
        commitTx(tx);
      } catch (OperationException e) {
        helper.finish(Error);
        LOG.error("Error during Delete: " + e.getMessage(), e);
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }
      helper.finish(Success);
      respondSuccess(message.getChannel(), request);

    } else if (operation.equals(TableOp.Write)) {
      Write write;
      // decode the columns and values into byte arrays
      if (valueMap == null || valueMap.isEmpty()) {
        // this happens when we have no content
        respondBadRequest(message, request, helper, "request body has no columns to write");
        return;
      }
      try {
        byte[][] cols = new byte[valueMap.size()][];
        byte[][] vals = new byte[valueMap.size()][];
        int i = 0;
        for (Map.Entry<String, String> entry : valueMap.entrySet()) {
          cols[i] = Util.decodeBinary(entry.getKey(), encoding);
          vals[i] = Util.decodeBinary(entry.getValue(), encoding, counter);
          i++;
        }
        write = new Write(rowKey, cols, vals);
      } catch (Exception e) {
        respondBadRequest(message, request, helper, "error decoding column key(s) and value(s)", e);
        return;
      }
      // now execute the write
      try {
        Transaction tx = startTx();
        table.write(write);
        commitTx(tx);
      } catch (OperationException e) {
        helper.finish(Error);
        LOG.error("Error during Writte: " + e.getMessage(), e);
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }
      helper.finish(Success);
      respondSuccess(message.getChannel(), request);
    } else if (operation.equals(TableOp.Increment)) {
      Increment increment;
      if (valueMap == null || valueMap.isEmpty()) {
        // this happens when we have no content
        respondBadRequest(message, request, helper, "request body has no columns to increment");
        return;
      }
      // decode the columns and values into byte arrays
      try {
        byte[][] cols = new byte[valueMap.size()][];
        long[] vals = new long[valueMap.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : valueMap.entrySet()) {
          cols[i] = Util.decodeBinary(entry.getKey(), encoding);
          vals[i] = Long.parseLong(entry.getValue());
          i++;
        }
        increment = new Increment(rowKey, cols, vals);
      } catch (Exception e) {
        respondBadRequest(message, request, helper, "error decoding column key(s) and value(s)", e);
        return;
      }
      // now execute the write
      Map<byte[], Long> results;
      try {
        Transaction tx = startTx();
        results = table.incrementAndGet(increment);
        commitTx(tx);
      } catch (OperationException e) {
        // if this was an illegal increment, then it was a bad request
        if (StatusCode.ILLEGAL_INCREMENT == e.getStatus()) {
          respondBadRequest(message, request, helper, "attempt to increment a value that is not a long");
        } else {
          // otherwise it is an internal error
          helper.finish(Error);
          LOG.error("Error during Write: " + e.getMessage(), e);
          respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
        return;
      }
      // first convert the bytes to strings
      Map<String, Long> map = Maps.newTreeMap();
      for (Map.Entry<byte[], Long> entry : results.entrySet()) {
        map.put(Util.encodeBinary(entry.getKey(), encoding), entry.getValue());
      }
      // now write a json string representing the map
      byte[] response = new Gson().toJson(map).getBytes(Charsets.UTF_8);
      respondSuccess(message.getChannel(), request, response);
      helper.finish(Success);
    }
  }

  private Transaction startTx() {
    Transaction tx = accessor.getTxSystemClient().startShort();
    for (TransactionAware txAware : accessor.getInstantiator().getInstantiator().getTransactionAware()) {
      txAware.startTx(tx);
    }
    return tx;
  }

  private void commitTx(Transaction tx) {
    for (TransactionAware txAware : accessor.getInstantiator().getInstantiator().getTransactionAware()) {
      try {
        txAware.commitTx();
      } catch (Exception e) {
        // todo
        throw Throwables.propagate(e);
      }
    }
    accessor.getTxSystemClient().commit(tx);
  }

  private void handleGlobalOperation(MessageEvent message, HttpRequest request, MetricsHelper helper,
                                     Map<String, List<String>> parameters, OperationContext context) {
    // must be a POST
    if (request.getMethod() != HttpMethod.POST) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Received unknown request with URI '" + request.getUri() + "'");
      }
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      return;
    }
    // only global operation supported is clear
    List<ClearFabric.ToClear> toClear = Lists.newArrayList();
    List<String> clearParams = parameters.get("clear");
    if (clearParams == null || clearParams.isEmpty()) {
      respondBadRequest(message, request, helper, "no operation at global level");
      return;
    }
    for (String param : parameters.get("clear")) {
      for (String what : param.split(",")) {
        if ("all".equals(what)) {
          toClear.add(ClearFabric.ToClear.ALL);
        } else if ("data".equals(what)) {
          toClear.add(ClearFabric.ToClear.DATA);
        } else if ("meta".equals(what)) {
          toClear.add(ClearFabric.ToClear.META);
        } else if ("tables".equals(what)) {
          toClear.add(ClearFabric.ToClear.TABLES);
        } else if ("queues".equals(what)) {
          toClear.add(ClearFabric.ToClear.QUEUES);
        } else if ("streams".equals(what)) {
          toClear.add(ClearFabric.ToClear.STREAMS);
        } else {
          respondBadRequest(message, request, helper, "invalid argument for clear");
          return;
        }
      }
    }
    if (toClear.isEmpty()) {
      respondBadRequest(message, request, helper, "clear without arguments");
      return;
    }
    ClearFabric clearFabric = new ClearFabric(toClear);
    try {
      // remove from ds2 if needed (it uses mds, so doing it before mds cleanup)
      if (toClear.contains(ClearFabric.ToClear.ALL) || toClear.contains(ClearFabric.ToClear.TABLES)) {
        removeDs2Tables(context.getAccount(), context);
      }

      this.accessor.getExecutor().execute(context, clearFabric);

    } catch (Exception e) {
      LOG.error("Exception clearing data fabric: ", e);
      helper.finish(Error);
      respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    respondSuccess(message.getChannel(), request);
    helper.finish(Success);
  }

  private void removeDs2Tables(String account, OperationContext context) throws Exception {
    List<Dataset> datasets = this.accessor.getMetadataService().getDatasets(new Account(account));
    for (Dataset ds : datasets) {
      removeDataset(ds.getName(), context);
    }
  }

  private void removeDataset(String datasetName, OperationContext opContext) throws OperationException {
    // NOTE: for now we just try to do the best we can: find all used DataSets of type Table and remove them. This
    //       should be done better, when we refactor DataSet API (towards separating user API and management parts)
    DataSet dataSet = this.accessor.getInstantiator().getDataSet(datasetName, opContext);
    DataSetSpecification config = dataSet.configure();
    List<DataSetSpecification> allDataSets = getAllUsedDataSets(config);
    for (DataSetSpecification spec : allDataSets) {
      DataSet ds = this.accessor.getInstantiator().getDataSet(spec.getName(), opContext);
      try {
        DataSetManager dataSetManager =
          this.accessor.getDataSetAccessor().getDataSetManager(OrderedColumnarTable.class);
        dataSetManager.drop(ds.getName());
      } catch (Exception e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, "failed to truncate table: " + ds.getName(), e);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
    throws Exception {
    MetricsHelper.meterError(metrics, this.accessor.getMetricsQualifier());
    LOG.error("Exception caught for connector '" +
                this.accessor.getName() + "'. ", e.getCause());
    if (e.getChannel().isOpen()) {
      respondError(ctx.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }

}
