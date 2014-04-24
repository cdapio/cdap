package com.continuuity.data2.transaction.persist;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.InputStream;
import java.io.OutputStream;
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
 * Handles serialization/deserialization of a {@link com.continuuity.data2.transaction.persist.TransactionSnapshot} and
 * its elements to {@code byte[]}.
 */
public abstract class AbstractSnapshotCodec {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSnapshotCodec.class);

  protected abstract int getVersion();
  protected abstract NavigableMap<Long, InMemoryTransactionManager.InProgressTx> decodeInProgress(Decoder decoder)
    throws IOException;
  protected abstract void encodeInProgress(Encoder encoder,
                                           Map<Long, InMemoryTransactionManager.InProgressTx> inProgress)
    throws IOException;

  //--------- helpers to encode or decode the transaction state --------------

  /**
   * Encodes a given {@code TransactionSnapshot} instance into a byte array.  Can be reversed by calling
   * {@link #decodeState(byte[])}.
   * @param snapshot snapshot state to be serialized.
   * @return a byte array representing the serialized {@code TransactionSnapshot} state.
   */
  public byte[] encodeState(TransactionSnapshot snapshot) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      encodeState(bos, snapshot);
      return bos.toByteArray();
    } finally {
      try {
        bos.close();
      } catch (IOException e) {
        LOG.error("Unable to close output stream properly: ", e);
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Encodes a given {@code TransactionSnapshot} instance into a byte array output stream.
   * Can be reversed by calling {@link #decodeState(java.io.InputStream)}.
   * @param snapshot snapshot state to be serialized.
   * @return a byte array output stream containing the serialized {@code TransactionSnapshot} state.
   */
  public void encodeState(OutputStream out, TransactionSnapshot snapshot) {
    Encoder encoder = new BinaryEncoder(out);
    try {
      encoder.writeInt(getVersion());
      encoder.writeLong(snapshot.getTimestamp());
      encoder.writeLong(snapshot.getReadPointer());
      encoder.writeLong(snapshot.getWritePointer());
      // supporting old versions of codecs
      writeAbsoleteAttributes(encoder);
      encodeInvalid(encoder, snapshot.getInvalid());
      encodeInProgress(encoder, snapshot.getInProgress());
      encodeChangeSets(encoder, snapshot.getCommittingChangeSets());
      encodeChangeSets(encoder, snapshot.getCommittedChangeSets());

    } catch (IOException e) {
      LOG.error("Unable to serialize transaction state: ", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Deserializes an encoded {@code TransactionSnapshot} back into its object representation.  Reverses serialization
   * performed by {@link #encodeState(com.continuuity.data2.transaction.persist.TransactionSnapshot)}.
   * @param bytes the serialized {@code TransactionSnapshot} representation as a byte array.
   * @return a {@code TransactionSnapshot} instance populated with the serialized values.
   */
  public TransactionSnapshot decodeState(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    try {
      return decodeState(bis);
    } finally {
      try {
        bis.close();
      } catch (IOException e) {
        LOG.error("Unable to close input stream properly: ", e);
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Deserializes an encoded {@code TransactionSnapshot} back into its object representation.  Reverses serialization
   * performed by {@link #encodeState(OutputStream, com.continuuity.data2.transaction.persist.TransactionSnapshot)}.
   * @param is the serialized {@code TransactionSnapshot} representation as an input stream.
   * @return a {@code TransactionSnapshot} instance populated with the serialized values.
   */
  public TransactionSnapshot decodeState(InputStream is) {
    Decoder decoder = new BinaryDecoder(is);

    try {
      int persistedVersion = decoder.readInt();
      if (persistedVersion != getVersion()) {
        throw new RuntimeException("Can't decode state persisted with version " + persistedVersion + ". Expected " +
                                   "version is " + getVersion());
      }
    } catch (IOException e) {
      LOG.error("Unable to deserialize transaction state: ", e);
      throw Throwables.propagate(e);
    }
    return decodePartialState(is);
  }

  /**
   * Deserializes an encoded {@code TransactionSnapshot} from which the version is missing, back into its object
   * representation. Reverses serialization performed by
   * {@link #encodeState(OutputStream, com.continuuity.data2.transaction.persist.TransactionSnapshot)}.
   * @param is the serialized {@code TransactionSnapshot} representation as an input stream.
   * @return a {@code TransactionSnapshot} instance populated with the serialized values.
   */
  public TransactionSnapshot decodePartialState(InputStream is) {
    Decoder decoder = new BinaryDecoder(is);

    try {
      long timestamp = decoder.readLong();
      long readPointer = decoder.readLong();
      long writePointer = decoder.readLong();
      // some attributes where removed during format change, luckily those stored at the end, so we just give a chance
      // to skip them
      readAbsoleteAttributes(decoder);
      Collection<Long> invalid = decodeInvalid(decoder);
      NavigableMap<Long, InMemoryTransactionManager.InProgressTx> inProgress = decodeInProgress(decoder);
      NavigableMap<Long, Set<ChangeId>> committing = decodeChangeSets(decoder);
      NavigableMap<Long, Set<ChangeId>> committed = decodeChangeSets(decoder);

      return new TransactionSnapshot(timestamp, readPointer, writePointer, invalid, inProgress,
                                     committing, committed);
    } catch (IOException e) {
      LOG.error("Unable to deserialize transaction state: ", e);
      throw Throwables.propagate(e);
    }
  }

  // todo: remove in next version that breaks compatibility of tx log
  @Deprecated
  protected void readAbsoleteAttributes(Decoder decoder) throws IOException {
    // NOTHING by default
  }

  // todo: remove in next version that breaks compatibility of tx log
  @Deprecated
  protected void writeAbsoleteAttributes(Encoder encoder) throws IOException {
    // NOTHING by default
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
