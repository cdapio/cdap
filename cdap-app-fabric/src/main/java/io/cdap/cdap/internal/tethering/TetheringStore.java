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
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
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
import java.util.Optional;
import javax.inject.Inject;

/**
 * Store for tethering data.
 */
public class TetheringStore {
  private static final Gson GSON = new GsonBuilder().create();

  private final TransactionRunner transactionRunner;

  @Inject
  public TetheringStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Adds a peer.
   *
   * @param peerInfo peer information
   * @throws IOException if inserting into the table fails
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
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerInfo.getName()));
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_URI_FIELD, peerInfo.getEndpoint()));
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.TETHERING_STATE_FIELD,
                                    peerInfo.getTetheringStatus().toString()));
      fields.add(Fields.longField(StoreDefinition.TetheringStore.LAST_CONNECTION_TIME_FIELD, 0L));
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_METADATA_FIELD,
                                    GSON.toJson(peerInfo.getMetadata())));
      tetheringTable.upsert(fields);
    }, IOException.class, PeerAlreadyExistsException.class);
  }

  /**
   * Updates tethering status and last connection time for a peer.
   *
   * @param peerName name of the peer
   * @param tetheringStatus status of tetherinf with the peer
   * @throws IOException if updating the table fails
   */
    public void updatePeerStatusAndTimestamp(String peerName, TetheringStatus tetheringStatus) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName));
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.TETHERING_STATE_FIELD, tetheringStatus.toString()));
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
  public void updatePeerStatus(String peerName, TetheringStatus tetheringStatus) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName));
      fields.add(Fields.stringField(StoreDefinition.TetheringStore.TETHERING_STATE_FIELD, tetheringStatus.toString()));
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
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tetheringTable = context.getTable(StoreDefinition.TetheringStore.TETHERING);
      // throw PeerNotFoundException if peer doesn't exist
      getPeer(tetheringTable, peerName);
      tetheringTable
        .delete(Collections.singleton(Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName)));
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
      try (CloseableIterator<StructuredRow> iterator = tetheringTable.scan(Range.all(), Integer.MAX_VALUE)) {
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
      Optional<StructuredRow> row = getPeer(tetheringTable, peerName);
      return getPeerInfo(row.get());
    }, PeerNotFoundException.class, IOException.class);
  }

  private Optional<StructuredRow> getPeer(StructuredTable tetheringTable, String peerName)
    throws IOException, PeerNotFoundException {
    Collection<Field<?>> key = ImmutableList.of(
      Fields.stringField(StoreDefinition.TetheringStore.PEER_NAME_FIELD, peerName));
    Optional<StructuredRow> row = tetheringTable.read(key);
    if (!row.isPresent()) {
      throw new PeerNotFoundException(peerName);
    }
    return tetheringTable.read(key);
  }

  private PeerInfo getPeerInfo(StructuredRow row) {
    String peerName = row.getString(StoreDefinition.TetheringStore.PEER_NAME_FIELD);
    String endpoint = row.getString(StoreDefinition.TetheringStore.PEER_URI_FIELD);
    TetheringStatus tetheringStatus = TetheringStatus.valueOf(
      row.getString(StoreDefinition.TetheringStore.TETHERING_STATE_FIELD));
    PeerMetadata peerMetadata = GSON.fromJson(row.getString(StoreDefinition.TetheringStore.PEER_METADATA_FIELD),
                                              PeerMetadata.class);
    long lastConnectionTime = row.getLong(StoreDefinition.TetheringStore.LAST_CONNECTION_TIME_FIELD);
    return new PeerInfo(peerName, endpoint, tetheringStatus, peerMetadata, lastConnectionTime);
  }
}
