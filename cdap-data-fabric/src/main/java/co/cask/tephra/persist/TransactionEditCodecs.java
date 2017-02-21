/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.cask.tephra.persist;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.tephra.ChangeId;
import org.apache.tephra.TransactionType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Utilities to handle encoding and decoding of {@link TransactionEdit} entries, while maintaining compatibility
 * with older versions of the serialized data. This class was included for backward compatibility reasons.
 * It will be removed in future releases.
 */
@Deprecated
public class TransactionEditCodecs {

  private static final TransactionEditCodec[] ALL_CODECS = {
      new TransactionEditCodecV1(),
      new TransactionEditCodecV2(),
      new TransactionEditCodecV3(),
      new TransactionEditCodecV4()
  };

  private static final SortedMap<Byte, TransactionEditCodec> CODECS = new TreeMap<>();
  static {
    for (TransactionEditCodec codec : ALL_CODECS) {
      CODECS.put(codec.getVersion(), codec);
    }
  }

  /**
   * Deserializes the encoded data from the given input stream, setting the values as fields
   * on the given {@code TransactionEdit} instances.  This method expects first value in the
   * {code DataInput} to be a byte representing the codec version used to serialize the instance.
   *
   * @param dest the transaction edit to populate with the deserialized values
   * @param in the input stream containing the encoded data
   * @throws IOException if an error occurs while deserializing from the input stream
   */
  public static void decode(TransactionEdit dest, DataInput in) throws IOException {
    byte version = in.readByte();
    TransactionEditCodec codec = CODECS.get(version);
    if (codec == null) {
      throw new IOException("TransactionEdit was serialized with an unknown codec version " + version +
          ". Was it written with a newer version of Tephra?");
    }
    codec.decode(dest, in);
  }

  /**
   * Serializes the given {@code TransactionEdit} instance with the latest available codec.
   * This will first write out the version of the codec used to serialize the instance so that
   * the correct codec can be used when calling {@link #decode(TransactionEdit, DataInput)}.
   *
   * @param src the transaction edit to serialize
   * @param out the output stream to contain the data
   * @throws IOException if an error occurs while serializing to the output stream
   */
  public static void encode(TransactionEdit src, DataOutput out) throws IOException {
    TransactionEditCodec latestCodec = CODECS.get(CODECS.firstKey());
    out.writeByte(latestCodec.getVersion());
    latestCodec.encode(src, out);
  }

  /**
   * Encodes the given transaction edit using a specific codec.  Note that this is only exposed
   * for use by tests.
   */
  @VisibleForTesting
  static void encode(TransactionEdit src, DataOutput out, TransactionEditCodec codec) throws IOException {
    out.writeByte(codec.getVersion());
    codec.encode(src, out);
  }

  /**
   * Defines the interface used for encoding and decoding {@link TransactionEdit} instances to and from
   * binary representations.
   */
  interface TransactionEditCodec {
    /**
     * Reads the encoded values from the data input stream and sets the fields in the given {@code TransactionEdit}
     * instance.
     *
     * @param dest the instance on which to set all the deserialized values
     * @param in the input stream containing the serialized data
     * @throws IOException if an error occurs while deserializing the data
     */
    void decode(TransactionEdit dest, DataInput in) throws IOException;

    /**
     * Writes all the field values from the {@code TransactionEdit} instance in serialized form to the data
     * output stream.
     *
     * @param src the instance to serialize to the stream
     * @param out the output stream to contain the data
     * @throws IOException if an error occurs while serializing the data
     */
    void encode(TransactionEdit src, DataOutput out) throws IOException;

    /**
     * Returns the version number for this codec.  Each codec should use a unique version number, with the newest
     * codec having the lowest number.
     */
    byte getVersion();
  }


  // package-private for unit-test access
  static class TransactionEditCodecV1 implements TransactionEditCodec {
    @Override
    public void decode(TransactionEdit dest, DataInput in) throws IOException {
      dest.setWritePointer(in.readLong());
      int stateIdx = in.readInt();
      try {
        dest.setState(TransactionEdit.State.values()[stateIdx]);
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IOException("State enum ordinal value is out of range: " + stateIdx);
      }
      dest.setExpiration(in.readLong());
      dest.setCommitPointer(in.readLong());
      dest.setCanCommit(in.readBoolean());
      int changeSize = in.readInt();
      Set<ChangeId> changes = Sets.newHashSet();
      for (int i = 0; i < changeSize; i++) {
        int currentLength = in.readInt();
        byte[] currentBytes = new byte[currentLength];
        in.readFully(currentBytes);
        changes.add(new ChangeId(currentBytes));
      }
      dest.setChanges(changes);
      // 1st version did not store this info. It is safe to set firstInProgress to 0, it may decrease performance until
      // this tx is finished, but correctness will be preserved.
      dest.setVisibilityUpperBound(0);
    }

    /** @deprecated use {@link TransactionEditCodecs.TransactionEditCodecV4} instead, it is still here for
     *  unit-tests only */
    @Override
    @Deprecated
    public void encode(TransactionEdit src, DataOutput out) throws IOException {
      out.writeLong(src.getWritePointer());
      // use ordinal for predictable size, though this does not support evolution
      out.writeInt(src.getState().ordinal());
      out.writeLong(src.getExpiration());
      out.writeLong(src.getCommitPointer());
      out.writeBoolean(src.getCanCommit());
      Set<ChangeId> changes = src.getChanges();
      if (changes == null) {
        out.writeInt(0);
      } else {
        out.writeInt(changes.size());
        for (ChangeId c : changes) {
          byte[] cKey = c.getKey();
          out.writeInt(cKey.length);
          out.write(cKey);
        }
      }
      // NOTE: we didn't have visibilityUpperBound in V1, it was added in V2
      // we didn't have transaction type, truncateInvalidTx and truncateInvalidTxTime in V1 and V2,
      // it was added in V3
    }

    @Override
    public byte getVersion() {
      return -1;
    }
  }

  // package-private for unit-test access
  static class TransactionEditCodecV2 extends TransactionEditCodecV1 implements TransactionEditCodec {
    @Override
    public void decode(TransactionEdit dest, DataInput in) throws IOException {
      super.decode(dest, in);
      dest.setVisibilityUpperBound(in.readLong());
    }

    /** @deprecated use {@link TransactionEditCodecs.TransactionEditCodecV4} instead, it is still here for
     *  unit-tests only */
    @Override
    public void encode(TransactionEdit src, DataOutput out) throws IOException {
      super.encode(src, out);
      out.writeLong(src.getVisibilityUpperBound());
      // NOTE: we didn't have transaction type, truncateInvalidTx and truncateInvalidTxTime in V1 and V2,
      // it was added in V3
    }

    @Override
    public byte getVersion() {
      return -2;
    }
  }

  // TODO: refactor to avoid duplicate code among different version of codecs
  // package-private for unit-test access
  static class TransactionEditCodecV3 extends TransactionEditCodecV2 implements TransactionEditCodec {
    @Override
    public void decode(TransactionEdit dest, DataInput in) throws IOException {
      super.decode(dest, in);
      int typeIdx = in.readInt();
      // null transaction type is represented as -1
      if (typeIdx < 0) {
        dest.setType(null);
      } else {
        try {
          dest.setType(TransactionType.values()[typeIdx]);
        } catch (ArrayIndexOutOfBoundsException e) {
          throw new IOException("Type enum ordinal value is out of range: " + typeIdx);
        }
      }

      int truncateInvalidTxSize = in.readInt();
      Set<Long> truncateInvalidTx = emptySet(dest.getTruncateInvalidTx());
      for (int i = 0; i < truncateInvalidTxSize; i++) {
        truncateInvalidTx.add(in.readLong());
      }
      dest.setTruncateInvalidTx(truncateInvalidTx);
      dest.setTruncateInvalidTxTime(in.readLong());
    }

    private <T> Set<T> emptySet(Set<T> set) {
      if (set == null) {
        return Sets.newHashSet();
      }
      set.clear();
      return set;
    }

    @Override
    public void encode(TransactionEdit src, DataOutput out) throws IOException {
      super.encode(src, out);
      // null transaction type is represented as -1
      if (src.getType() == null) {
        out.writeInt(-1);
      } else {
        out.writeInt(src.getType().ordinal());
      }

      Set<Long> truncateInvalidTx = src.getTruncateInvalidTx();
      if (truncateInvalidTx == null) {
        out.writeInt(0);
      } else {
        out.writeInt(truncateInvalidTx.size());
        for (long id : truncateInvalidTx) {
          out.writeLong(id);
        }
      }
      out.writeLong(src.getTruncateInvalidTxTime());
    }

    @Override
    public byte getVersion() {
      return -3;
    }
  }

  static class TransactionEditCodecV4 extends TransactionEditCodecV3 {
    @Override
    public void decode(TransactionEdit dest, DataInput in) throws IOException {
      super.decode(dest, in);
      dest.setParentWritePointer(in.readLong());
      int checkpointPointersLen = in.readInt();
      if (checkpointPointersLen >= 0) {
        long[] checkpointPointers = new long[checkpointPointersLen];
        for (int i = 0; i < checkpointPointersLen; i++) {
          checkpointPointers[i] = in.readLong();
        }
        dest.setCheckpointPointers(checkpointPointers);
      }
    }

    @Override
    public void encode(TransactionEdit src, DataOutput out) throws IOException {
      super.encode(src, out);
      out.writeLong(src.getParentWritePointer());
      long[] checkpointPointers = src.getCheckpointPointers();
      if (checkpointPointers == null) {
        out.writeInt(-1);
      } else {
        out.writeInt(checkpointPointers.length);
        for (int i = 0; i < checkpointPointers.length; i++) {
          out.writeLong(checkpointPointers[i]);
        }
      }
    }

    @Override
    public byte getVersion() {
      return -4;
    }
  }
}
