/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Closeables;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.txprune.RegionPruneInfo;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handler to for managing transaction states.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TransactionHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionHttpHandler.class);
  private static final Type STRING_LONG_MAP_TYPE = new TypeToken<Map<String, Long>>() { }.getType();
  private static final Type STRING_LONG_SET_MAP_TYPE = new TypeToken<Map<String, Set<Long>>>() { }.getType();

  private final TransactionSystemClient txClient;
  private Class<?> debugClazz;
  private Object debugObject;

  @Inject
  public TransactionHttpHandler(CConfiguration cConf, TransactionSystemClient txClient) {
    this.txClient = new TransactionSystemClientAdapter(txClient);
    try {
      this.debugClazz = getClass().getClassLoader()
        .loadClass("org.apache.tephra.hbase.txprune.InvalidListPruningDebug");
      this.debugObject = debugClazz.newInstance();
      Configuration hConf = new Configuration();
      for (Map.Entry<String, String> entry : cConf) {
        hConf.set(entry.getKey(), entry.getValue());
      }
      Method initMethod = debugClazz.getMethod("initialize", Configuration.class);
      initMethod.setAccessible(true);
      initMethod.invoke(debugObject, hConf);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
      LOG.warn("InvalidListPruningDebug class not found. Pruning Debug endpoints will not work.", ex);
      this.debugClazz = null;
    } catch (NoSuchMethodException | InvocationTargetException ex) {
      LOG.warn("Initialize method was not found in InvalidListPruningDebug. Debug endpoints will not work.", ex);
      this.debugClazz = null;
    }
  }

  /**
   * Retrieve the state of the transaction manager.
   */
  @Path("/transactions/state")
  @GET
  public void getTxManagerSnapshot(HttpRequest request, HttpResponder responder)
    throws TransactionCouldNotTakeSnapshotException, IOException {
    LOG.trace("Taking transaction manager snapshot at time {}", System.currentTimeMillis());
    LOG.trace("Took and retrieved transaction manager snapshot successfully.");
    try (InputStream in = txClient.getSnapshotInputStream()) {
      ChunkResponder chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK,
                                                               ImmutableMultimap.<String, String>of());
      while (true) {
        // netty doesn't copy the readBytes buffer, so we have to reallocate a new buffer
        byte[] readBytes = new byte[4096];
        int res = in.read(readBytes, 0, 4096);
        if (res == -1) {
          break;
        }
        // If failed to send chunk, IOException will be raised.
        // It'll just propagated to the netty-http library to handle it
        chunkResponder.sendChunk(ChannelBuffers.wrappedBuffer(readBytes, 0, res));
      }
      Closeables.closeQuietly(chunkResponder);
    }
  }

  /**
   * Invalidate a transaction.
   * @param txId transaction ID.
   */
  @Path("/transactions/{tx-id}/invalidate")
  @POST
  public void invalidateTx(HttpRequest request, HttpResponder responder,
                           @PathParam("tx-id") String txId) {
    try {
      long txIdLong = Long.parseLong(txId);
      boolean success = txClient.invalidate(txIdLong);
      if (success) {
        LOG.info("Transaction {} successfully invalidated", txId);
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        LOG.info("Transaction {} could not be invalidated: not in progress.", txId);
        responder.sendStatus(HttpResponseStatus.CONFLICT);
      }
    } catch (NumberFormatException e) {
      LOG.info("Could not invalidate transaction: {} is not a valid tx id", txId);
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
    }
  }

  @Path("/transactions/invalid/remove/until")
  @POST
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void truncateInvalidTxBefore(HttpRequest request,
                                      HttpResponder responder) throws InvalidTruncateTimeException {
    Map<String, Long> body;
    try {
      body = parseBody(request, STRING_LONG_MAP_TYPE);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid time value in request");
      return;
    }

    if (body == null || !body.containsKey("time")) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Time not specified");
      return;
    }

    long time = body.get("time");
    txClient.truncateInvalidTxBefore(time);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/transactions/invalid/remove/ids")
  @POST
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void truncateInvalidTx(HttpRequest request, HttpResponder responder) {
    Map<String, Set<Long>> body;
    try {
      body = parseBody(request, STRING_LONG_SET_MAP_TYPE);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid ids specified in request");
      return;
    }

    if (body == null || !body.containsKey("ids")) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Transaction ids not specified");
      return;
    }

    Set<Long> txIds = body.get("ids");
    txClient.truncateInvalidTx(txIds);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/transactions/invalid/size")
  @GET
  public void invalidTxSize(HttpRequest request, HttpResponder responder) {
    int invalidSize = txClient.getInvalidSize();
    responder.sendJson(HttpResponseStatus.OK, ImmutableMap.of("size", invalidSize));
  }

  @Path("/transactions/invalid")
  @GET
  public void invalidList(HttpRequest request, HttpResponder responder,
                          @QueryParam("limit") @DefaultValue("-1") int limit) {
    Transaction tx = txClient.startShort();
    txClient.abort(tx);
    long[] invalids = tx.getInvalids();
    if (limit == -1) {
      responder.sendJson(HttpResponseStatus.OK, invalids);
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, Arrays.copyOf(invalids, Math.min(limit, invalids.length)));
  }

  /**
   * Reset the state of the transaction manager.
   */
  @Path("/transactions/state")
  @POST
  public void resetTxManagerState(HttpRequest request, HttpResponder responder) {
    txClient.resetState();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/transactions/prune/regions/{region-name}")
  @GET
  public void getPruneInfo(HttpRequest request, HttpResponder responder, @PathParam("region-name") String regionName) {
    if (debugClazz == null || debugObject == null) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Invalid List Pruning debug class not found.");
      return;
    }

    try {
      Method method = debugClazz.getMethod("getRegionPruneInfo", String.class);
      method.setAccessible(true);
      Object response = method.invoke(debugObject, regionName);
      if (response == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             "No prune upper bound has been registered for this region yet.");
        return;
      }
      RegionPruneInfo pruneInfo = (RegionPruneInfo) response;
      responder.sendJson(HttpResponseStatus.OK, pruneInfo);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the RegionPruneInfo.", e);
    }
  }

  @Path("/transactions/prune/regions")
  @GET
  public void getTimeRegions(HttpRequest request, HttpResponder responder,
                             @QueryParam("time") @DefaultValue("9223372036854775807") long time) {
    if (debugClazz == null || debugObject == null) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Invalid List Pruning debug class not found.");
      return;
    }

    try {
      Method method = debugClazz.getMethod("getRegionsOnOrBeforeTime", Long.class);
      method.setAccessible(true);
      Object response = method.invoke(debugObject, time);
      if (response == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             String.format("No regions have been registered on or before time %d", time));
        return;
      }
      Map<Long, SortedSet<String>> timeRegionInfo = (Map<Long, SortedSet<String>>) response;
      responder.sendJson(HttpResponseStatus.OK, timeRegionInfo);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the time region.", e);
    }
  }

  @Path("/transactions/prune/regions/idle")
  @GET
  public void getIdleRegions(HttpRequest request, HttpResponder responder,
                             @QueryParam("limit") @DefaultValue("-1") int numRegions) {
    if (debugClazz == null || debugObject == null) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Invalid List Pruning debug class not found.");
      return;
    }

    try {
      Method method = debugClazz.getMethod("getIdleRegions", Integer.class);
      method.setAccessible(true);
      Object response = method.invoke(debugObject, numRegions);
      Queue<RegionPruneInfo> pruneInfos = (Queue<RegionPruneInfo>) response;
      responder.sendJson(HttpResponseStatus.OK, pruneInfos);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the idle regions.", e);
    }
  }
}
