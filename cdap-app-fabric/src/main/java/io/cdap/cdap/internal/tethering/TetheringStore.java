/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;

/**
 * Store for tethering data.
 */
public class TetheringStore {

  private static final Gson GSON = new GsonBuilder().create();
  // Prefix of per-peer topic that contains last message id received
  static final String PEER_TOPIC_PREFIX = "tethering.peer.message.state.";
  // Prefix of per-peer topic that contains last message id sent
  static final String MSG_TO_PEER_TOPIC_PREFIX = "tethering.peer.message.sent.state.";
  private final TransactionRunner transactionRunner;

  @Inject
  public TetheringStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Adds a peer if it doesn't already exist.
   *
   * @param peerInfo peer information
   * @throws IOException if inserting into the table fails
   * @throws PeerAlreadyExistsException if peer information already exists
   */
  public void addPeer(PeerInfo peerInfo) throws IOException, PeerAlreadyExistsException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
      Collection<Field<?>> key = ImmutableList.of(
          Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerInfo.getName()));
      Optional<StructuredRow> row = tetheringTable.read(key);
      if (row.isPresent()) {
        PeerInfo peer = getPeerInfo(row.get());
        throw new PeerAlreadyExistsException(peer.getName(), peer.getTetheringStatus());
      }
      writePeerInternal(peerInfo, context);
    }, IOException.class, PeerAlreadyExistsException.class);
  }

  /**
   * Adds peer information if it doesn't already exist, updates peer metadata otherwise.
   *
   * @param peerInfo peer information
   * @return true if the peer doesn't already exist, false otherwise
   * @throws IOException if writing to the table fails
   */
  public boolean writePeer(PeerInfo peerInfo) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
      Optional<StructuredRow> row = getPeerInternal(tetheringTable, peerInfo.getName());
      if (!row.isPresent()) {
        writePeerInternal(peerInfo, context);
        return true;
      } else {
        PeerInfo existingPeerInfo = getPeerInfo(row.get());
        PeerInfo peerInfoToWrite = new PeerInfo(peerInfo.getName(), peerInfo.getEndpoint(),
            existingPeerInfo.getTetheringStatus(), peerInfo.getMetadata(),
            peerInfo.getRequestTime(), existingPeerInfo.getLastConnectionTime());
        writePeerInternal(peerInfoToWrite, context);
        return false;
      }
    }, IOException.class);
  }

  private void writePeerInternal(PeerInfo peerInfo, StructuredTableContext context)
      throws IOException {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerInfo.getName()));
    fields.add(
        Fields.stringField(StoreDefinition.TetheringStore.PEER_URI_FIELD, peerInfo.getEndpoint()));
    fields.add(Fields.stringField(StoreDefinition.TetheringStore.TETHERING_STATE_FIELD,
        peerInfo.getTetheringStatus().toString()));
    fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_METADATA_FIELD,
        GSON.toJson(peerInfo.getMetadata())));
    fields.add(Fields.longField(StoreDefinition.TetheringStore.REQUEST_TIME_FIELD,
        peerInfo.getRequestTime()));
    fields.add(Fields.longField(StoreDefinition.TetheringStore.LAST_CONNECTION_TIME_FIELD,
        peerInfo.getLastConnectionTime()));

    StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
    tetheringTable.upsert(fields);
  }

  /**
   * Updates tethering status and last connection time for a peer.
   *
   * @param peerName name of the peer
   * @param tetheringStatus status of tetherinf with the peer
   * @throws IOException if updating the table fails
   */
  public void updatePeerStatusAndTimestamp(String peerName, TetheringStatus tetheringStatus)
      throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName));
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.TETHERING_STATE_FIELD,
          tetheringStatus.toString()));
      fields.add(Fields.longField(StoreDefinition.TetheringStore.LAST_CONNECTION_TIME_FIELD,
          System.currentTimeMillis()));
      StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
      tetheringTable.update(fields);
    }, IOException.class);
  }

  /**
   * Updates tethering status for a peer.
   *
   * @param peerName name of the peer
   * @param tetheringStatus status of tethering with the peer
   * @throws IOException if updating the table fails
   */
  public void updatePeerStatus(String peerName, TetheringStatus tetheringStatus)
      throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName));
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.TETHERING_STATE_FIELD,
          tetheringStatus.toString()));
      StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
      tetheringTable.update(fields);
    }, IOException.class);
  }

  /**
   * Updates the last connection timestamp for the peer.
   *
   * @param peerName name of the peer
   * @throws IOException if updating the table fails
   */
  public void updatePeerTimestamp(String peerName) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName));
      fields.add(Fields.longField(StoreDefinition.TetheringStore.LAST_CONNECTION_TIME_FIELD,
          System.currentTimeMillis()));
      StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
      tetheringTable.update(fields);
    }, IOException.class);
  }

  /**
   * Deletes a peer
   *
   * @param peerName name of the peer
   * @throws IOException if deleting the table fails
   * @throws PeerNotFoundException if the peer is not found
   */
  public void deletePeer(String peerName) throws IOException, PeerNotFoundException {
    deletePeer(peerName, false);
  }

  /**
   * Deletes a peer
   *
   * @param peerName name of the peer
   * @param deleteSubscriberState whether to delete subscriber state
   * @throws IOException if deleting the table fails
   * @throws PeerNotFoundException if the peer is not found
   */
  public void deletePeer(String peerName, boolean deleteSubscriberState)
      throws IOException, PeerNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
      Optional<StructuredRow> row = getPeerInternal(tetheringTable, peerName);
      if (!row.isPresent()) {
        throw new PeerNotFoundException(peerName);
      }
      tetheringTable
          .delete(Collections.singleton(
              Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName)));
      if (deleteSubscriberState) {
        AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
        appMetadataStore.deleteSubscriberState(PEER_TOPIC_PREFIX + peerName,
            TetheringProgramEventPublisher.SUBSCRIBER);
        appMetadataStore.deleteSubscriberState(MSG_TO_PEER_TOPIC_PREFIX + peerName,
            TetheringProgramEventPublisher.SUBSCRIBER);
      }
    }, IOException.class, PeerNotFoundException.class);
  }

  /**
   * Get information about all tethered peers
   *
   * @return information about status of tethered peers
   * @throws IOException if reading from the database fails
   */
  public List<PeerInfo> getPeers() throws IOException {
    List<PeerInfo> peers = new ArrayList<>();
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetheringTable = context
          .getTable(StoreDefinition.TetheringStore.TETHERING);
      try (CloseableIterator<StructuredRow> iterator = tetheringTable.scan(Range.all(),
          Integer.MAX_VALUE)) {
        iterator.forEachRemaining(row -> {
          peers.add(getPeerInfo(row));
        });
        return peers;
      }
    }, IOException.class);
  }

  /**
   * Get information about a peer
   *
   * @return information about status of a peer
   * @throws IOException if reading from the database fails
   * @throws PeerNotFoundException if the peer is not found
   */
  public PeerInfo getPeer(String peerName) throws IOException, PeerNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetheringTable = context
          .getTable(StoreDefinition.TetheringStore.TETHERING);
      Optional<StructuredRow> row = getPeerInternal(tetheringTable, peerName);
      if (!row.isPresent()) {
        throw new PeerNotFoundException(peerName);
      }
      return getPeerInfo(row.get());
    }, PeerNotFoundException.class, IOException.class);
  }

  /**
   * Initialize last message id sent and received from each peer in {@code peers}
   *
   * @param peers list of peer names
   * @return subscriber state
   */
  public SubscriberState initializeSubscriberState(List<String> peers) {
    SubscriberState subscriberState = new SubscriberState();
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      for (String peer : peers) {
        String messageId = appMetadataStore.retrieveSubscriberState(PEER_TOPIC_PREFIX + peer,
            TetheringProgramEventPublisher.SUBSCRIBER);
        subscriberState.setLastMessageIdReceived(peer, messageId);

        messageId = appMetadataStore.retrieveSubscriberState(MSG_TO_PEER_TOPIC_PREFIX + peer,
            TetheringProgramEventPublisher.SUBSCRIBER);
        subscriberState.setLastMessageIdSent(peer, messageId);
      }
    });
    return subscriberState;
  }

  /**
   * Updates subscriber state table entries for tethered peers
   *
   * @param subscriberState subscriber state
   */
  public void updateSubscriberState(SubscriberState subscriberState) {
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
      for (Map.Entry<String, String> entry : subscriberState.getLastMessageIdsReceived()
          .entrySet()) {
        if (!getPeerInternal(tetheringTable, entry.getKey()).isPresent()) {
          // Don't update subscriber state for deleted peer
          continue;
        }
        appMetadataStore.persistSubscriberState(PEER_TOPIC_PREFIX + entry.getKey(),
            TetheringProgramEventPublisher.SUBSCRIBER,
            entry.getValue());
      }
      for (Map.Entry<String, String> entry : subscriberState.getLastMessageIdsSent().entrySet()) {
        if (!getPeerInternal(tetheringTable, entry.getKey()).isPresent()) {
          // Don't update subscriber state for deleted peer
          continue;
        }
        appMetadataStore.persistSubscriberState(MSG_TO_PEER_TOPIC_PREFIX + entry.getKey(),
            TetheringProgramEventPublisher.SUBSCRIBER,
            entry.getValue());
      }
    });
  }

  private Optional<StructuredRow> getPeerInternal(StructuredTable tetheringTable, String peerName)
      throws IOException {
    Collection<Field<?>> key = ImmutableList.of(
        Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName));
    return tetheringTable.read(key);
  }

  private PeerInfo getPeerInfo(StructuredRow row) {
    String peerName = row.getString(StoreDefinition.TetheringStore.PEER_NAME_FIELD);
    String endpoint = row.getString(StoreDefinition.TetheringStore.PEER_URI_FIELD);
    TetheringStatus tetheringStatus = TetheringStatus.valueOf(
        row.getString(StoreDefinition.TetheringStore.TETHERING_STATE_FIELD));
    PeerMetadata peerMetadata = GSON.fromJson(
        row.getString(StoreDefinition.TetheringStore.PEER_METADATA_FIELD),
        PeerMetadata.class);
    long lastConnectionTime = row.getLong(
        StoreDefinition.TetheringStore.LAST_CONNECTION_TIME_FIELD);
    long requestTime = row.getLong(StoreDefinition.TetheringStore.REQUEST_TIME_FIELD);
    return new PeerInfo(peerName, endpoint, tetheringStatus, peerMetadata, requestTime,
        lastConnectionTime);
  }
}
