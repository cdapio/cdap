package com.continuuity.data2.transaction.persist;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Handles serialization/deserialization of a {@link TransactionSnapshot} and its elements to {@code byte[]}.
 */
public class SnapshotCodec {
  private static final int STATE_PERSIST_VERSION = 1;

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotCodec.class);

  //--------- helpers to encode or decode the transaction state --------------

  /**
   * Encodes a given {@code TransactionSnapshot} instance into a byte array.  Can be reversed by calling
   * {@link #decodeState(byte[])}.
   * @param snapshot snapshot state to be serialized.
   * @return a byte array representing the serialized {@code TransactionSnapshot} state.
   */
  public byte[] encodeState(TransactionSnapshot snapshot) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = new BinaryEncoder(bos);

    try {
      encoder.writeInt(STATE_PERSIST_VERSION);
      encoder.writeLong(snapshot.getTimestamp());
      encoder.writeLong(snapshot.getReadPointer());
      encoder.writeLong(snapshot.getWritePointer());
      encoder.writeLong(snapshot.getWatermark());
      encodeInvalid(encoder, snapshot.getInvalid());
      encodeInProgress(encoder, snapshot.getInProgress());
      encodeChangeSets(encoder, snapshot.getCommittingChangeSets());
      encodeChangeSets(encoder, snapshot.getCommittedChangeSets());

    } catch (IOException e) {
      LOG.error("Unable to serialize transaction state: ", e);
      throw Throwables.propagate(e);
    }
    return bos.toByteArray();
  }

  /**
   * Deserializes an encoded {@code TransactionSnapshot} back into its object representation.  Reverses serialization
   * performed by {@link #encodeState(TransactionSnapshot)}.
   * @param bytes the serialized {@code TransactionSnapshot} representation.
   * @return a {@code TransactionSnapshot} instance populated with the serialized values.
   */
  public TransactionSnapshot decodeState(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    Decoder decoder = new BinaryDecoder(bis);

    try {
      int persistedVersion = decoder.readInt();
      if (persistedVersion != STATE_PERSIST_VERSION) {
        throw new RuntimeException("Can't decode state persisted with version " + persistedVersion + ". Current " +
                                     "version is " + STATE_PERSIST_VERSION);
      }
      long timestamp = decoder.readLong();
      long readPointer = decoder.readLong();
      long writePointer = decoder.readLong();
      long waterMark = decoder.readLong();
      Collection<Long> invalid = decodeInvalid(decoder);
      NavigableMap<Long, Long> inProgress = decodeInProgress(decoder);
      NavigableMap<Long, Set<ChangeId>> committing = decodeChangeSets(decoder);
      NavigableMap<Long, Set<ChangeId>> committed = decodeChangeSets(decoder);

      return new TransactionSnapshot(timestamp, readPointer, writePointer, waterMark, invalid, inProgress,
                                     committing, committed);
    } catch (IOException e) {
      LOG.error("Unable to deserialize transaction state: ", e);
      throw Throwables.propagate(e);
    }
  }

  private void encodeInvalid(Encoder encoder, Collection<Long> invalid) throws IOException {
    if (!invalid.isEmpty()) {
      encoder.writeInt(invalid.size());
      for (long invalidTx : invalid) {
        encoder.writeLong(invalidTx);
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private Collection<Long> decodeInvalid(Decoder decoder) throws IOException {
    int size = decoder.readInt();
    Collection<Long> invalid = Lists.newArrayListWithCapacity(size);
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        invalid.add(decoder.readLong());
      }
      size = decoder.readInt();
    }
    return invalid;
  }

  private void encodeInProgress(Encoder encoder, Map<Long, Long> inProgress) throws IOException {
    if (!inProgress.isEmpty()) {
      encoder.writeInt(inProgress.size());
      for (Map.Entry<Long, Long> entry : inProgress.entrySet()) {
        encoder.writeLong(entry.getKey()); // tx id
        encoder.writeLong(entry.getValue()); // time stamp;
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private NavigableMap<Long, Long> decodeInProgress(Decoder decoder) throws IOException {
    int size = decoder.readInt();
    NavigableMap<Long, Long> inProgress = new TreeMap<Long, Long>();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        inProgress.put(decoder.readLong(), decoder.readLong());
      }
      size = decoder.readInt();
    }
    return inProgress;
  }

  private void encodeChangeSets(Encoder encoder, Map<Long, Set<ChangeId>> changes) throws IOException {
    if (!changes.isEmpty()) {
      encoder.writeInt(changes.size());
      for (Map.Entry<Long, Set<ChangeId>> entry : changes.entrySet()) {
        encoder.writeLong(entry.getKey());
        encodeChanges(encoder, entry.getValue());
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private NavigableMap<Long, Set<ChangeId>> decodeChangeSets(Decoder decoder) throws IOException {
    int size = decoder.readInt();
    NavigableMap<Long, Set<ChangeId>> changeSets = new TreeMap<Long, Set<ChangeId>>();
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        changeSets.put(decoder.readLong(), decodeChanges(decoder));
      }
      size = decoder.readInt();
    }
    return changeSets;
  }

  private void encodeChanges(Encoder encoder, Set<ChangeId> changes) throws IOException {
    if (!changes.isEmpty()) {
      encoder.writeInt(changes.size());
      for (ChangeId change : changes) {
        encoder.writeBytes(change.getKey());
      }
    }
    encoder.writeInt(0); // zero denotes end of list as per AVRO spec
  }

  private Set<ChangeId> decodeChanges(Decoder decoder) throws IOException {
    int size = decoder.readInt();
    HashSet<ChangeId> changes = Sets.newHashSetWithExpectedSize(size);
    while (size != 0) { // zero denotes end of list as per AVRO spec
      for (int remaining = size; remaining > 0; --remaining) {
        changes.add(new ChangeId(Bytes.toBytes(decoder.readBytes())));
      }
      size = decoder.readInt();
    }
    // todo is there an immutable hash set?
    return changes;
  }
}
