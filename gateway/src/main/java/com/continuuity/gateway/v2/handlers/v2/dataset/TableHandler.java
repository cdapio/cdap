package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.data.dataset.DataSetInstantiationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.util.Util;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.continuuity.gateway.v2.txmanager.TxManager;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.thrift.TException;
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

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Handles Table REST calls.
 */
@Path("/v2")
public class TableHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TableHandler.class);
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() {}.getType();
  private static final Type LONG_MAP_TYPE = new TypeToken<Map<String, Long>>() {}.getType();

  private final MetadataService metadataService;
  private final DataSetInstantiatorFromMetaData datasetInstantiator;
  private final TransactionSystemClient txSystemClient;
  private final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

  @Inject
  public TableHandler(MetadataService metadataService, DataSetInstantiatorFromMetaData datasetInstantiator,
                      TransactionSystemClient txSystemClient, GatewayAuthenticator authenticator) {
    super(authenticator);
    this.metadataService = metadataService;
    this.datasetInstantiator = datasetInstantiator;
    this.txSystemClient = txSystemClient;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting TableHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping TableHandler");
  }

  @PUT
  @Path("/tables/{name}")
  public void createTable(HttpRequest request, final HttpResponder responder, @PathParam("name") String tableName) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      DataSetSpecification spec = new Table(tableName).configure();
      Dataset ds = new Dataset(spec.getName());
      ds.setName(spec.getName());
      ds.setType(spec.getType());
      ds.setSpecification(new Gson().toJson(spec));

      metadataService.assertDataset(new Account(accountId), ds);
      responder.sendStatus(OK);

    } catch (MetadataServiceException e) {
      responder.sendStatus(CONFLICT);
    } catch (TException e) {
      LOG.error("Thrift error creating table {}", tableName, e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    } catch (SecurityException e) {
      responder.sendStatus(FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(BAD_REQUEST);
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  @PUT
  @Path("/tables/{name}/row/{key}")
  public void writeTableRow(HttpRequest request, final HttpResponder responder,
                            @PathParam("name") String tableName, @PathParam("key") String key) {

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

      Write write = new Write(rowKey, cols, vals);

      // now execute the write
      TxManager txManager = new TxManager(txSystemClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txManager.start();
      try {
        table.write(write);
        txManager.commit();
        responder.sendStatus(OK);
      } catch (OperationException e) {
        LOG.trace("Error during Write: ", e);
        txManager.abort();
        responder.sendStatus(INTERNAL_SERVER_ERROR);
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

  @GET
  @Path("/tables/{name}/row/{key}")
  public void readTableRow(HttpRequest request, final HttpResponder responder,
                            @PathParam("name") String tableName, @PathParam("key") String key) {

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

      Read read;
      if (columns == null || columns.isEmpty()) {
        // column range
        byte[] startCol = start == null ? null : Util.decodeBinary(start, encoding);
        byte[] stopCol = stop == null ? null : Util.decodeBinary(stop, encoding);
        read = new Read(rowKey, startCol, stopCol, limit);
      } else {
        byte[][] cols = new byte[columns.size()][];
        int i = 0;
        for (String column : columns) {
          cols[i++] = Util.decodeBinary(column, encoding);
        }
        read = new Read(rowKey, cols);
      }

      TxManager txManager = new TxManager(txSystemClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txManager.start();
      try {
        OperationResult<Map<byte[], byte[]>> result = table.read(read);
        txManager.commit();

        // read successful, now respond with result
        if (result.isEmpty() || result.getValue().isEmpty()) {
          responder.sendStatus(NO_CONTENT);
        } else {
          // result is not empty, now construct a json response
          // first convert the bytes to strings
          Map<String, String> map = Maps.newTreeMap();
          for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
            map.put(Util.encodeBinary(entry.getKey(), encoding),
                    Util.encodeBinary(entry.getValue(), encoding, counter));
          }
          responder.sendJson(OK, map, STRING_MAP_TYPE);
        }
      } catch (OperationException e) {
        LOG.trace("Error during Read: ", e);
        txManager.abort();
        responder.sendStatus(INTERNAL_SERVER_ERROR);
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

  @POST
  @Path("/tables/{name}/row/{key}")
  public void incrementTableRow(HttpRequest request, final HttpResponder responder,
                                @PathParam("name") String tableName, @PathParam("key") String key) {
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
      Increment increment = new Increment(rowKey, cols, vals);

      // now execute the increment
      TxManager txManager = new TxManager(txSystemClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txManager.start();
      try {
        Map<byte[], Long> results = table.incrementAndGet(increment);
        txManager.commit();

        // first convert the bytes to strings
        Map<String, Long> map = Maps.newTreeMap();
        for (Map.Entry<byte[], Long> entry : results.entrySet()) {
          map.put(Util.encodeBinary(entry.getKey(), encoding), entry.getValue());
        }
        // now write a json string representing the map
        responder.sendJson(OK, map, LONG_MAP_TYPE);

      } catch (OperationException e) {
        // if this was an illegal increment, then it was a bad request
        if (StatusCode.ILLEGAL_INCREMENT == e.getStatus()) {
          responder.sendString(BAD_REQUEST, "attempt to increment a value that is not a long");
        } else {
          // otherwise it is an internal error
          LOG.trace("Error during Increment: ", e);
          responder.sendStatus(INTERNAL_SERVER_ERROR);
        }
        txManager.abort();
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


  @DELETE
  @Path("/tables/{name}/row/{key}")
  public void deleteTableRow(HttpRequest request, final HttpResponder responder,
                             @PathParam("name") String tableName, @PathParam("key") String key) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      // Fetch table
      Table table = datasetInstantiator.getDataSet(tableName, new OperationContext(accountId));

      // decode row key using the given encoding
      String encoding = getEncoding(queryParams);
      byte [] rowKey = key == null ? null : Util.decodeBinary(key, encoding);

      List<String> columns = getColumns(queryParams);
      if (columns == null || columns.isEmpty()) {
        responder.sendString(BAD_REQUEST, "delete must have columns");
        return;
      }

      byte[][] cols = new byte[columns.size()][];
      int i = 0;
      for (String column : columns) {
        cols[i++] = Util.decodeBinary(column, encoding);
      }
      Delete delete = new Delete(rowKey, cols);

      // now execute the delete operation
      TxManager txManager = new TxManager(txSystemClient, datasetInstantiator.getInstantiator().getTransactionAware());
      txManager.start();
      try {
        table.write(delete);
        txManager.commit();
        responder.sendStatus(OK);
      } catch (OperationException e) {
        LOG.trace("Error during Delete: ", e);
        txManager.abort();
        responder.sendStatus(INTERNAL_SERVER_ERROR);
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
