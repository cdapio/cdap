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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler to for managing transaction states.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class TransactionHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionHttpHandler.class);

  private final TransactionSystemClient txClient;

  @Inject
  public TransactionHttpHandler(Authenticator authenticator, TransactionSystemClient txClient) {
    super(authenticator);
    this.txClient = txClient;
  }

  /**
   * Retrieve the state of the transaction manager.
   */
  @Path("/transactions/state")
  @GET
  public void getTxManagerSnapshot(HttpRequest request, HttpResponder responder) {
    try {
      LOG.trace("Taking transaction manager snapshot at time {}", System.currentTimeMillis());
      InputStream in = txClient.getSnapshotInputStream();
      LOG.trace("Took and retrieved transaction manager snapshot successfully.");
      try {
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
      } finally {
        in.close();
      }
    } catch (Exception e) {
      LOG.error("Could not take transaction manager snapshot", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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

  /**
   * Reset the state of the transaction manager.
   */
  @Path("/transactions/state")
  @POST
  public void resetTxManagerState(HttpRequest request, HttpResponder responder) {
    txClient.resetState();
    responder.sendStatus(HttpResponseStatus.OK);
  }

}
