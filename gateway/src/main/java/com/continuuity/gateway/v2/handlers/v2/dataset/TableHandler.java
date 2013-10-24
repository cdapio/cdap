package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.ProgramId;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.util.ThriftHelper;
import com.continuuity.gateway.util.Util;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.thrift.protocol.TProtocol;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

/**
 * Handles Table REST calls.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class TableHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TableHandler.class);
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() {}.getType();
  private static final Type LONG_MAP_TYPE = new TypeToken<Map<String, Long>>() {}.getType();

  private final DataSetInstantiatorFromMetaData datasetInstantiator;
  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryClient;
  private EndpointStrategy endpointStrategy;


  private final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

  @Inject
  public TableHandler(DataSetInstantiatorFromMetaData datasetInstantiator, DiscoveryServiceClient discoveryClient,
                      TransactionSystemClient txSystemClient, GatewayAuthenticator authenticator) {
    super(authenticator);
    this.datasetInstantiator = datasetInstantiator;
    this.discoveryClient = discoveryClient;
    this.txSystemClient = txSystemClient;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting TableHandler");
    endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC)), 1L, TimeUnit.SECONDS);
    datasetInstantiator.init(endpointStrategy);
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping TableHandler");
  }

  @PUT
  @Path("/tables/{table-id}")
  public void createTable(HttpRequest request, final HttpResponder responder,
                          @PathParam("table-id") String tableName) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      String spec = gson.get().toJson(new Table(tableName).configure());
      try {
        client.createDataSet(new ProgramId(accountId, "", ""), spec);
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(OK);

    } catch (SecurityException e) {
      responder.sendStatus(UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  @PUT
  @Path("/tables/{table-id}/rows/{row-id}")
  public void writeTableRow(HttpRequest request, final HttpResponder responder,
                            @PathParam("table-id") String tableName, @PathParam("row-id") String key) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte [] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      boolean counter = getCounter(queryParams);

      // Read values from request body
      Map<String, String> valueMap = getValuesMap(request);
      // decode the columns and values into byte arrays
      if (valueMap == null || valueMap.isEmpty()) {
        // this happens when we have no content
        throw new IllegalArgumentException("request body has no columns to write");
      }

      byte[][] cols = new byte[valueMap.size()][];
      byte[][] vals = new byte[valueMap.size()][];
      int i = 0;
      for (Map.Entry<String, String> entry : valueMap.entrySet()) {
        cols[i] = Util.decodeBinary(entry.getKey(), encoding);
        vals[i] = Util.decodeBinary(entry.getValue(), encoding, counter);
        i++;
      }

      // now execute the write
      TransactionContext txContext = new TransactionContext(
        txSystemClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txContext.start();
      table.put(rowKey, cols, vals);
      txContext.finish();
      responder.sendStatus(OK);

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

  @GET
  @Path("/tables/{table-id}/rows/{row-id}")
  public void readTableRow(HttpRequest request, final HttpResponder responder,
                            @PathParam("table-id") String tableName, @PathParam("row-id") String key) {


    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte [] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      boolean counter = getCounter(queryParams);
      List<String> columns = getColumns(queryParams);
      String start = getOptionalSingletonParam(queryParams, "start");
      String stop = getOptionalSingletonParam(queryParams, "stop");
      int limit = getLimit(queryParams);

      if (columns != null && !columns.isEmpty() && (start != null || stop != null)) {
        throw new IllegalArgumentException("Read can only specify columns or range");
      }

      TransactionContext txContext = new TransactionContext(
        txSystemClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txContext.start();

      Row result;
      if (columns == null || columns.isEmpty()) {
        // column range
        byte[] startCol = start == null ? null : Util.decodeBinary(start, encoding);
        byte[] stopCol = stop == null ? null : Util.decodeBinary(stop, encoding);
        result = table.get(rowKey, startCol, stopCol, limit);
      } else {
        byte[][] cols = new byte[columns.size()][];
        int i = 0;
        for (String column : columns) {
          cols[i++] = Util.decodeBinary(column, encoding);
        }
        result = table.get(rowKey, cols);
      }

      txContext.finish();

      // read successful, now respond with result
      if (result.isEmpty() || result.isEmpty()) {
        responder.sendStatus(NO_CONTENT);
      } else {
        // result is not empty, now construct a json response
        // first convert the bytes to strings
        Map<String, String> map = Maps.newTreeMap();
        for (Map.Entry<byte[], byte[]> entry : result.getColumns().entrySet()) {
          map.put(Util.encodeBinary(entry.getKey(), encoding),
                  Util.encodeBinary(entry.getValue(), encoding, counter));
        }
        responder.sendJson(OK, map, STRING_MAP_TYPE);
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

  @POST
  @Path("/tables/{table-id}/rows/{row-id}/increment")
  public void incrementTableRow(HttpRequest request, final HttpResponder responder,
                                @PathParam("table-id") String tableName, @PathParam("row-id") String key) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte [] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      // Read values from request body
      Map<String, String> valueMap = getValuesMap(request);
      // decode the columns and values into byte arrays
      if (valueMap == null || valueMap.isEmpty()) {
        // this happens when we have no content
        throw new IllegalArgumentException("request body has no columns to write");
      }

      // decode the columns and values into byte arrays
      byte[][] cols = new byte[valueMap.size()][];
      long[] vals = new long[valueMap.size()];
      int i = 0;
      for (Map.Entry<String, String> entry : valueMap.entrySet()) {
        cols[i] = Util.decodeBinary(entry.getKey(), encoding);
        vals[i] = Long.parseLong(entry.getValue());
        i++;
      }
      // now execute the increment
      TransactionContext txContext = new TransactionContext(
        txSystemClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txContext.start();
      Row result = table.increment(rowKey, cols, vals);
      txContext.finish();

      // first convert the bytes to strings
      Map<String, Long> map = Maps.newTreeMap();
      for (Map.Entry<byte[], byte[]> entry : result.getColumns().entrySet()) {
        map.put(Util.encodeBinary(entry.getKey(), encoding), Bytes.toLong(entry.getValue()));
      }
      // now write a json string representing the map
      responder.sendJson(OK, map, LONG_MAP_TYPE);

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


  @DELETE
  @Path("/tables/{table-id}/rows/{row-id}")
  public void deleteTableRow(HttpRequest request, final HttpResponder responder,
                             @PathParam("table-id") String tableName, @PathParam("row-id") String key) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte [] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      List<String> columns = getColumns(queryParams);
      byte[][] cols = null;
      if (columns != null && !columns.isEmpty()) {
        cols = new byte[columns.size()][];
        int i = 0;
        for (String column : columns) {
          cols[i++] = Util.decodeBinary(column, encoding);
        }
      }

      // now execute the delete operation
      TransactionContext txContext = new TransactionContext(
        txSystemClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txContext.start();
      table.delete(rowKey, cols);
      txContext.finish();
      responder.sendStatus(OK);

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

  private Map<String, String> getValuesMap(HttpRequest request) {
    // parse JSON string in the body
    try {
        InputStreamReader reader = new InputStreamReader(
          new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
          return gson.get().fromJson(reader, STRING_MAP_TYPE);
    } catch (Exception e) {
      // failed to parse json, that is a bad request
      throw new IllegalArgumentException("Failed to parse body as json");
    }
  }

  private String getEncoding(Map<String, List<String>> queryParams) {
    String encoding = null;
    List<String> encodingParams = queryParams.get("encoding");

    if (encodingParams != null) {
      // make sure there is at most one
      if (encodingParams.size() > 1) {
        throw new IllegalArgumentException("More than one 'encoding' parameter");
      }

      // make sure that if there is one, it is supported
      if (!encodingParams.isEmpty()) {
        encoding = encodingParams.get(0);
        if (!Util.supportedEncoding(encoding)) {
          throw  new IllegalArgumentException("Unsupported 'encoding' parameter");
        }
      }
    }

    return encoding;
  }

  private boolean getCounter(Map<String, List<String>> queryParams) {
    // optional parameter counter - if true, column values are interpreted (and returned) as long numbers
    boolean counter = false;
    List<String> counterParams = queryParams.get("counter");
    if (counterParams != null) {
      // make sure there is at most one
      if (counterParams.size() > 1) {
        throw new IllegalArgumentException("More than one 'counter' parameter");
      }
      // make sure that if there is one, it is supported
      if (!counterParams.isEmpty()) {
        String param = counterParams.get(0);
        counter = "1".equals(param) || "true".equals(param);
      }
    }

    return counter;
  }

  private List<String> getColumns(Map<String, List<String>> queryParams) {
    // for read and delete operations, optional parameter is columns
    List<String> columns = null;
    List<String> columnParams = queryParams.get("columns");
    if (columnParams != null && columnParams.size() > 0) {
      columns = Lists.newLinkedList();
      for (String param : columnParams) {
        Collections.addAll(columns, param.split(","));
      }
    }

    return columns;
  }

  private String getOptionalSingletonParam(Map<String, List<String>> queryParams, String name) {
    List<String> params = queryParams.get(name);
    if (params != null && params.size() > 1) {
      throw new IllegalArgumentException(String.format("More than one '%s' parameter", name));
    }
    return  (params == null || params.isEmpty()) ? null : params.get(0);
  }

  private int getLimit(Map<String, List<String>> queryParams) {
    List<String> limitParams = queryParams.get("limit");
    if (limitParams != null && limitParams.size() > 1) {
      throw new IllegalArgumentException("More than one 'limit' parameter");
    }

    try {
      return (limitParams == null || limitParams.isEmpty()) ? -1 : Integer.parseInt(limitParams.get(0));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("'limit' parameter is not an integer");
    }
  }
}
