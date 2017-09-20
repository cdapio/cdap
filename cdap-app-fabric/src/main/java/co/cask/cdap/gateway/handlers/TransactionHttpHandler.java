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
import co.cask.http.HandlerContext;
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
import org.apache.tephra.TxConstants;
import org.apache.tephra.txprune.RegionPruneInfo;
import org.apache.tephra.txprune.hbase.InvalidListPruningDebug;
import org.apache.tephra.txprune.hbase.RegionsAtTime;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
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
  private static final String PRUNING_TOOL_CLASS_NAME = "org.apache.tephra.hbase.txprune.InvalidListPruningDebugTool";

  private final Configuration hConf;
  private final CConfiguration cConf;
  private final TransactionSystemClient txClient;
  private final boolean pruneEnable;
  private volatile InvalidListPruningDebug pruningDebug;

  @Inject
  public TransactionHttpHandler(Configuration hConf, CConfiguration cConf, TransactionSystemClient txClient) {
    this.hConf = hConf;
    this.cConf = cConf;
    this.txClient = new TransactionSystemClientAdapter(txClient);
    this.pruneEnable = cConf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE,
                                        TxConstants.TransactionPruning.DEFAULT_PRUNE_ENABLE);
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

  /**
   * Trigger transaction pruning.
   */
  @Path("/transactions/prune/now")
  @POST
  public void pruneNow(HttpRequest request, HttpResponder responder) {
    txClient.pruneNow();
    responder.sendStatus(HttpResponseStatus.OK);
  }


  @Path("/transactions/prune/regions/{region-name}")
  @GET
  public void getPruneInfo(HttpRequest request, HttpResponder responder, @PathParam("region-name") String regionName) {
    try {
      if (!initializePruningDebug(responder)) {
        return;
      }

      RegionPruneInfo pruneInfo = pruningDebug.getRegionPruneInfo(regionName);
      if (pruneInfo == null) {
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             "No prune upper bound has been registered for this region yet.");
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, pruneInfo);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the RegionPruneInfo.", e);
    }
  }

  @Path("/transactions/prune/regions")
  @GET
  public void getTimeRegions(HttpRequest request, HttpResponder responder,
                             @QueryParam("time") @DefaultValue("now") String time) {
    try {
      if (!initializePruningDebug(responder)) {
        return;
      }

      RegionsAtTime timeRegionInfo = pruningDebug.getRegionsOnOrBeforeTime(time);
      responder.sendJson(HttpResponseStatus.OK, timeRegionInfo);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the time region.", e);
    }
  }

  @Path("/transactions/prune/regions/idle")
  @GET
  public void getIdleRegions(HttpRequest request, HttpResponder responder,
                             @QueryParam("limit") @DefaultValue("-1") int numRegions,
                             @QueryParam("time") @DefaultValue("now") String time) {
    try {
      if (!initializePruningDebug(responder)) {
        return;
      }

      SortedSet<? extends RegionPruneInfo> pruneInfos = pruningDebug.getIdleRegions(numRegions, time);
      responder.sendJson(HttpResponseStatus.OK, pruneInfos);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to fetch the idle regions.", e);
    }
  }

  @Path("/transactions/prune/regions/block")
  @GET
  public void getRegionsToBeCompacted(HttpRequest request, HttpResponder responder,
                                      @QueryParam("limit") @DefaultValue("-1") int numRegions,
                                      @QueryParam("time") @DefaultValue("now") String time) {
    try {
      if (!initializePruningDebug(responder)) {
        return;
      }

      Set<String> regionNames = pruningDebug.getRegionsToBeCompacted(numRegions, time);
      responder.sendJson(HttpResponseStatus.OK, regionNames);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      LOG.debug("Exception while trying to get the regions that needs to be compacted.", e);
    }
  }

  private boolean initializePruningDebug(HttpResponder responder) {
    if (!pruneEnable) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid List Pruning is not enabled.");
      return false;
    }

    synchronized (this) {
      if (pruningDebug != null) {
        return true;
      }

      // Copy over both cConf and hConf into the pruning configuration
      Configuration configuration = new Configuration();
      configuration.clear();
      // First copy hConf and then cConf so that we retain the values from cConf for any parameters defined in both
      copyConf(configuration, hConf);
      copyConf(configuration, cConf);

      try {
        @SuppressWarnings("unchecked")
        Class<? extends InvalidListPruningDebug> clazz =
          (Class<? extends InvalidListPruningDebug>) getClass().getClassLoader().loadClass(PRUNING_TOOL_CLASS_NAME);
        this.pruningDebug = clazz.newInstance();
        pruningDebug.initialize(configuration);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
        ClassCastException | IOException e) {
        LOG.debug("Not able to instantiate pruning debug class", e);
        pruningDebug = null;
      }
      return true;
    }
  }

  @Override
  public void destroy(HandlerContext context) {
    super.destroy(context);
    synchronized (this) {
      if (pruningDebug != null) {
        try {
          pruningDebug.destroy();
        } catch (IOException e) {
          LOG.error("Error destroying pruning debug instance", e);
        }
      }
    }
  }

  private void copyConf(Configuration to, Iterable<Map.Entry<String, String>> from) {
    for (Map.Entry<String, String> entry : from) {
      to.set(entry.getKey(), entry.getValue());
    }
  }
}
