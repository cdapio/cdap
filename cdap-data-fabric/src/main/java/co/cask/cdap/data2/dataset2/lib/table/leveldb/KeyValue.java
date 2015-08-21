/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data2.dataset2.lib.table.leveldb;

import co.cask.cdap.api.common.Bytes;

import java.io.DataOutput;
import java.io.IOException;

/**
 * This is a copy of parts of HBase's KeyValue, to be used for the leveldb table implementation.
 */
public class KeyValue {

  /**
   * Timestamp to use when we want to refer to the latest cell.
   * This is the timestamp sent by clients when no timestamp is specified on
   * commit.
   */
  public static final long LATEST_TIMESTAMP = Long.MAX_VALUE;

  /**
   * Timestamp to use when we want to refer to the oldest cell.
   */
  public static final long OLDEST_TIMESTAMP = Long.MIN_VALUE;

  /**
   * Comparator for plain key; i.e. non-catalog table key.  Works on Key portion
   * of KeyValue only.
   */
  public static final KeyComparator KEY_COMPARATOR = new KeyComparator();

  /** Size of the key type field in bytes. */
  public static final int TYPE_SIZE = Bytes.SIZEOF_BYTE;

  /** Size of the row length field in bytes. */
  public static final int ROW_LENGTH_SIZE = Bytes.SIZEOF_SHORT;

  /** Size of the family length field in bytes. */
  public static final int FAMILY_LENGTH_SIZE = Bytes.SIZEOF_BYTE;

  /** Size of the timestamp field in bytes. */
  public static final int TIMESTAMP_SIZE = Bytes.SIZEOF_LONG;

  // Size of the timestamp and type byte on end of a key -- a long + a byte.
  public static final int TIMESTAMP_TYPE_SIZE = TIMESTAMP_SIZE + TYPE_SIZE;

  // Size of the length shorts and bytes in key.
  public static final int KEY_INFRASTRUCTURE_SIZE = ROW_LENGTH_SIZE
      + FAMILY_LENGTH_SIZE + TIMESTAMP_TYPE_SIZE;

  // How far into the key the row starts at. First thing to read is the short
  // that says how long the row is.
  public static final int ROW_OFFSET =
    Bytes.SIZEOF_INT /*keylength*/ +
    Bytes.SIZEOF_INT /*valuelength*/;

  // Size of the length ints in a KeyValue datastructure.
  public static final int KEYVALUE_INFRASTRUCTURE_SIZE = ROW_OFFSET;

  /**
   * Key type.
   * Has space for other key types to be added later.  Cannot rely on
   * enum ordinals . They change if item is removed or moved.  Do our own codes.
   */
  public static enum Type {
    Minimum((byte) 0),
    Put((byte) 4),

    Delete((byte) 8),
    DeleteColumn((byte) 12),
    UndeleteColumn((byte) 13),
    DeleteFamily((byte) 14),

    // Maximum is used when searching; you look from maximum on down.
    Maximum((byte) 255);

    private final byte code;

    Type(final byte c) {
      this.code = c;
    }

    public byte getCode() {
      return this.code;
    }

    /**
     * Cannot rely on enum ordinals . They change if item is removed or moved.
     * Do our own codes.
     * @return Type associated with passed code.
     */
    public static Type codeToType(final byte b) {
      for (Type t : Type.values()) {
        if (t.getCode() == b) {
          return t;
        }
      }
      throw new RuntimeException("Unknown code " + b);
    }
  }

  private byte [] bytes = null;
  private int offset = 0;
  private int length = 0;

  // the row cached
  private volatile byte [] rowCache = null;

  /**
   * Creates a KeyValue from the start of the specified byte array.
   * Presumes <code>bytes</code> content is formatted as a KeyValue blob.
   * @param bytes byte array
   */
  public KeyValue(final byte [] bytes) {
    this(bytes, 0);
  }

  /**
   * Creates a KeyValue from the specified byte array and offset.
   * Presumes <code>bytes</code> content starting at <code>offset</code> is
   * formatted as a KeyValue blob.
   * @param bytes byte array
   * @param offset offset to start of KeyValue
   */
  public KeyValue(final byte [] bytes, final int offset) {
    this(bytes, offset, getLength(bytes, offset));
  }

  /**
   * Creates a KeyValue from the specified byte array, starting at offset, and
   * for length <code>length</code>.
   * @param bytes byte array
   * @param offset offset to start of the KeyValue
   * @param length length of the KeyValue
   */
  public KeyValue(final byte [] bytes, final int offset, final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /** Constructors that build a new backing byte array from fields */

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type key type
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
      final byte[] qualifier, final long timestamp, Type type) {
    this(row, family, qualifier, timestamp, type, null);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value column value
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
      final byte[] qualifier, final long timestamp, final byte[] value) {
    this(row, family, qualifier, timestamp, Type.Put, value);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte[] row, final byte[] family,
      final byte[] qualifier, final long timestamp, Type type,
      final byte[] value) {
    this(row, family, qualifier, 0, qualifier == null ? 0 : qualifier.length,
        timestamp, type, value, 0, value == null ? 0 : value.length);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row row key
   * @param family family name
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @throws IllegalArgumentException
   */
  public KeyValue(byte [] row, byte [] family,
      byte [] qualifier, int qoffset, int qlength, long timestamp, Type type,
      byte [] value, int voffset, int vlength) {
    this(row, 0, row == null ? 0 : row.length,
        family, 0, family == null ? 0 : family.length,
        qualifier, qoffset, qlength, timestamp, type,
        value, voffset, vlength);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @throws IllegalArgumentException
   */
  public KeyValue(final byte [] row, final int roffset, final int rlength,
      final byte [] family, final int foffset, final int flength,
      final byte [] qualifier, final int qoffset, final int qlength,
      final long timestamp, final Type type,
      final byte [] value, final int voffset, final int vlength) {
    this.bytes = createByteArray(row, roffset, rlength,
        family, foffset, flength, qualifier, qoffset, qlength,
        timestamp, type, value, voffset, vlength);
    this.length = bytes.length;
    this.offset = 0;
  }

  /**
   * Write KeyValue format into a byte array.
   *
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param type key type
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @return The newly created byte array.
   */
  static byte [] createByteArray(final byte [] row, final int roffset,
      final int rlength, final byte [] family, final int foffset, int flength,
      final byte [] qualifier, final int qoffset, int qlength,
      final long timestamp, final Type type,
      final byte [] value, final int voffset, int vlength) {
    if (rlength > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Row > " + Short.MAX_VALUE);
    }
    if (row == null) {
      throw new IllegalArgumentException("Row is null");
    }
    // Family length
    flength = family == null ? 0 : flength;
    if (flength > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Family > " + Byte.MAX_VALUE);
    }
    // Qualifier length
    qlength = qualifier == null ? 0 : qlength;
    if (qlength > Integer.MAX_VALUE - rlength - flength) {
      throw new IllegalArgumentException("Qualifier > " + Integer.MAX_VALUE);
    }
    // Key length
    long longkeylength = KEY_INFRASTRUCTURE_SIZE + rlength + flength + qlength;
    if (longkeylength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longkeylength + " > " +
        Integer.MAX_VALUE);
    }
    int keylength = (int) longkeylength;
    // Value length
    vlength = value == null ? 0 : vlength;

    // Allocate right-sized byte array.
    byte [] bytes = new byte[KEYVALUE_INFRASTRUCTURE_SIZE + keylength + vlength];
    // Write key, value and key row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keylength);
    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short) (rlength & 0x0000ffff));
    pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    pos = Bytes.putByte(bytes, pos, (byte) (flength & 0x0000ff));
    if (flength != 0) {
      pos = Bytes.putBytes(bytes, pos, family, foffset, flength);
    }
    if (qlength != 0) {
      pos = Bytes.putBytes(bytes, pos, qualifier, qoffset, qlength);
    }
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    if (value != null && value.length > 0) {
      Bytes.putBytes(bytes, pos, value, voffset, vlength);
    }
    return bytes;
  }

  // Needed doing 'contains' on List.  Only compares the key portion, not the
  // value.
  public boolean equals(Object other) {
    if (!(other instanceof KeyValue)) {
      return false;
    }
    KeyValue kv = (KeyValue) other;
    // Comparing bytes should be fine doing equals test.  Shouldn't have to
    // worry about special .META. comparators doing straight equals.
    return Bytes.equals(getBuffer(), getKeyOffset(), getKeyLength(),
      kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength());
  }

  public int hashCode() {
    byte[] b = getBuffer();
    int start = getOffset(), end = getOffset() + getLength();
    int h = b[start++];
    for (int i = start; i < end; i++) {
      h = (h * 13) ^ b[i];
    }
    return h;
  }

  //---------------------------------------------------------------------------
  //
  //  String representation
  //
  //---------------------------------------------------------------------------

  public String toString() {
    if (this.bytes == null || this.bytes.length == 0) {
      return "empty";
    }
    return keyToString(this.bytes, this.offset + ROW_OFFSET, getKeyLength()) +
      "/vlen=" + getValueLength();
  }

  public static String keyToString(final byte [] b, final int o, final int l) {
    if (b == null) {
      return "";
    }
    int rowlength = Bytes.toShort(b, o);
    String row = Bytes.toStringBinary(b, o + Bytes.SIZEOF_SHORT, rowlength);
    int columnoffset = o + Bytes.SIZEOF_SHORT + 1 + rowlength;
    int familylength = b[columnoffset - 1];
    int columnlength = l - ((columnoffset - o) + TIMESTAMP_TYPE_SIZE);
    String family = familylength == 0 ? "" :
      Bytes.toStringBinary(b, columnoffset, familylength);
    String qualifier = columnlength == 0 ? "" :
      Bytes.toStringBinary(b, columnoffset + familylength,
      columnlength - familylength);
    long timestamp = Bytes.toLong(b, o + (l - TIMESTAMP_TYPE_SIZE));
    String timestampStr = humanReadableTimestamp(timestamp);
    byte type = b[o + l - 1];
    return row + "/" + family +
      (family != null && family.length() > 0 ? ":" : "") +
      qualifier + "/" + timestampStr + "/" + Type.codeToType(type);
  }

  public static String humanReadableTimestamp(final long timestamp) {
    if (timestamp == LATEST_TIMESTAMP) {
      return "LATEST_TIMESTAMP";
    }
    if (timestamp == OLDEST_TIMESTAMP) {
      return "OLDEST_TIMESTAMP";
    }
    return String.valueOf(timestamp);
  }

  //---------------------------------------------------------------------------
  //
  //  Public Member Accessors
  //
  //---------------------------------------------------------------------------

  /**
   * @return The byte array backing this KeyValue.
   */
  public byte [] getBuffer() {
    return this.bytes;
  }

  /**
   * @return Offset into {@link #getBuffer()} at which this KeyValue starts.
   */
  public int getOffset() {
    return this.offset;
  }

  /**
   * @return Length of bytes this KeyValue occupies in {@link #getBuffer()}.
   */
  public int getLength() {
    return length;
  }

  //---------------------------------------------------------------------------
  //
  //  Length and Offset Calculators
  //
  //---------------------------------------------------------------------------

  /**
   * Determines the total length of the KeyValue stored in the specified
   * byte array and offset.  Includes all headers.
   * @param bytes byte array
   * @param offset offset to start of the KeyValue
   * @return length of entire KeyValue, in bytes
   */
  private static int getLength(byte [] bytes, int offset) {
    return ROW_OFFSET +
        Bytes.toInt(bytes, offset) +
        Bytes.toInt(bytes, offset + Bytes.SIZEOF_INT);
  }

  /**
   * @return Key offset in backing buffer..
   */
  public int getKeyOffset() {
    return this.offset + ROW_OFFSET;
  }

  /**
   * Length of key portion.
   */
  private int keyLength = 0;

  public int getKeyLength() {
    if (keyLength == 0) {
      keyLength = Bytes.toInt(this.bytes, this.offset);
    }
    return keyLength;
  }

  /**
   * @return Value offset
   */
  public int getValueOffset() {
    return getKeyOffset() + getKeyLength();
  }

  /**
   * @return Value length
   */
  public int getValueLength() {
    return Bytes.toInt(this.bytes, this.offset + Bytes.SIZEOF_INT);
  }

  /**
   * @return Row offset
   */
  public int getRowOffset() {
    return getKeyOffset() + Bytes.SIZEOF_SHORT;
  }

  /**
   * @return Row length
   */
  public short getRowLength() {
    return Bytes.toShort(this.bytes, getKeyOffset());
  }

  /**
   * @return Family offset
   */
  public int getFamilyOffset() {
    return getFamilyOffset(getRowLength());
  }

  /**
   * @return Family offset
   */
  public int getFamilyOffset(int rlength) {
    return this.offset + ROW_OFFSET + Bytes.SIZEOF_SHORT + rlength + Bytes.SIZEOF_BYTE;
  }

  /**
   * @return Family length
   */
  public byte getFamilyLength() {
    return getFamilyLength(getFamilyOffset());
  }

  /**
   * @return Family length
   */
  public byte getFamilyLength(int foffset) {
    return this.bytes[foffset - 1];
  }

  /**
   * @return Qualifier offset
   */
  public int getQualifierOffset() {
    return getQualifierOffset(getFamilyOffset());
  }

  /**
   * @return Qualifier offset
   */
  public int getQualifierOffset(int foffset) {
    return foffset + getFamilyLength(foffset);
  }

  /**
   * @return Qualifier length
   */
  public int getQualifierLength() {
    return getQualifierLength(getRowLength(), getFamilyLength());
  }

  /**
   * @return Qualifier length
   */
  public int getQualifierLength(int rlength, int flength) {
    return getKeyLength() -
      (KEY_INFRASTRUCTURE_SIZE + rlength + flength);
  }

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Timestamp offset
   */
  public int getTimestampOffset(final int keylength) {
    return getKeyOffset() + keylength - TIMESTAMP_TYPE_SIZE;
  }

  //---------------------------------------------------------------------------
  //
  //  Methods that return copies of fields
  //
  //---------------------------------------------------------------------------

  /**
   * Do not use unless you have to.  Used internally for compacting and testing.
   *
   * Use {@link #getRow()}, {@link #getFamily()}, {@link #getQualifier()}, and
   * {@link #getValue()} if accessing a KeyValue client-side.
   * @return Copy of the key portion only.
   */
  public byte [] getKey() {
    int keylength = getKeyLength();
    byte [] key = new byte[keylength];
    System.arraycopy(getBuffer(), getKeyOffset(), key, 0, keylength);
    return key;
  }

  /**
   * Returns value in a new byte array.
   * Primarily for use client-side. If server-side, use
   * {@link #getBuffer()} with appropriate offsets and lengths instead to
   * save on allocations.
   * @return Value in a new byte array.
   */
  public byte [] getValue() {
    int o = getValueOffset();
    int l = getValueLength();
    byte [] result = new byte[l];
    System.arraycopy(getBuffer(), o, result, 0, l);
    return result;
  }

  /**
   * Primarily for use client-side.  Returns the row of this KeyValue in a new
   * byte array.<p>
   *
   * If server-side, use {@link #getBuffer()} with appropriate offsets and
   * lengths instead.
   * @return Row in a new byte array.
   */
  public byte [] getRow() {
    if (rowCache == null) {
      int o = getRowOffset();
      short l = getRowLength();
      // initialize and copy the data into a local variable
      // in case multiple threads race here.
      byte local[] = new byte[l];
      System.arraycopy(getBuffer(), o, local, 0, l);
      rowCache = local; // volatile assign
    }
    return rowCache;
  }

  private long timestampCache = -1;
  /**
   * @return Timestamp
   */
  public long getTimestamp() {
    if (timestampCache == -1) {
      timestampCache = getTimestamp(getKeyLength());
    }
    return timestampCache;
  }

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Timestamp
   */
  long getTimestamp(final int keylength) {
    int tsOffset = getTimestampOffset(keylength);
    return Bytes.toLong(this.bytes, tsOffset);
  }

  /**
   * @return Type of this KeyValue.
   */
  public byte getType() {
    return getType(getKeyLength());
  }

  /**
   * @param keylength Pass if you have it to save on a int creation.
   * @return Type of this KeyValue.
   */
  byte getType(final int keylength) {
    return this.bytes[this.offset + keylength - 1 + ROW_OFFSET];
  }

  /**
   * Primarily for use client-side.  Returns the family of this KeyValue in a
   * new byte array.<p>
   *
   * If server-side, use {@link #getBuffer()} with appropriate offsets and
   * lengths instead.
   * @return Returns family. Makes a copy.
   */
  public byte [] getFamily() {
    int o = getFamilyOffset();
    int l = getFamilyLength(o);
    byte [] result = new byte[l];
    System.arraycopy(this.bytes, o, result, 0, l);
    return result;
  }

  /**
   * Primarily for use client-side.  Returns the column qualifier of this
   * KeyValue in a new byte array.<p>
   *
   * If server-side, use {@link #getBuffer()} with appropriate offsets and
   * lengths instead.
   * Use {@link #getBuffer()} with appropriate offsets and lengths instead.
   * @return Returns qualifier. Makes a copy.
   */
  public byte [] getQualifier() {
    int o = getQualifierOffset();
    int l = getQualifierLength();
    byte [] result = new byte[l];
    System.arraycopy(this.bytes, o, result, 0, l);
    return result;
  }

  //---------------------------------------------------------------------------
  //
  //  KeyValue splitter
  //
  //---------------------------------------------------------------------------

  /**
   * Utility class that splits a KeyValue buffer into separate byte arrays.
   * <p>
   * Should get rid of this if we can, but is very useful for debugging.
   */
  public static class SplitKeyValue {
    private byte [][] split;
    SplitKeyValue() {
      this.split = new byte[6][];
    }
    public void setRow(byte [] value) {
      this.split[0] = value;
    }
    public void setFamily(byte [] value) {
      this.split[1] = value;
    }
    public void setQualifier(byte [] value) {
      this.split[2] = value;
    }
    public void setTimestamp(byte [] value) {
      this.split[3] = value;
    }
    public void setType(byte [] value) {
      this.split[4] = value;
    }
    public void setValue(byte [] value) {
      this.split[5] = value;
    }
    public byte [] getRow() {
      return this.split[0];
    }
    public byte [] getTimestamp() {
      return this.split[3];
    }
    public byte [] getType() {
      return this.split[4];
    }
    public byte [] getValue() {
      return this.split[5];
    }
  }

  public SplitKeyValue split() {
    SplitKeyValue split = new SplitKeyValue();
    int splitOffset = this.offset;
    int keyLen = Bytes.toInt(bytes, splitOffset);
    splitOffset += Bytes.SIZEOF_INT;
    int valLen = Bytes.toInt(bytes, splitOffset);
    splitOffset += Bytes.SIZEOF_INT;
    short rowLen = Bytes.toShort(bytes, splitOffset);
    splitOffset += Bytes.SIZEOF_SHORT;
    byte [] row = new byte[rowLen];
    System.arraycopy(bytes, splitOffset, row, 0, rowLen);
    splitOffset += rowLen;
    split.setRow(row);
    byte famLen = bytes[splitOffset];
    splitOffset += Bytes.SIZEOF_BYTE;
    byte [] family = new byte[famLen];
    System.arraycopy(bytes, splitOffset, family, 0, famLen);
    splitOffset += famLen;
    split.setFamily(family);
    int colLen = keyLen -
      (rowLen + famLen + Bytes.SIZEOF_SHORT + Bytes.SIZEOF_BYTE +
      Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE);
    byte [] qualifier = new byte[colLen];
    System.arraycopy(bytes, splitOffset, qualifier, 0, colLen);
    splitOffset += colLen;
    split.setQualifier(qualifier);
    byte [] timestamp = new byte[Bytes.SIZEOF_LONG];
    System.arraycopy(bytes, splitOffset, timestamp, 0, Bytes.SIZEOF_LONG);
    splitOffset += Bytes.SIZEOF_LONG;
    split.setTimestamp(timestamp);
    byte [] type = new byte[1];
    type[0] = bytes[splitOffset];
    splitOffset += Bytes.SIZEOF_BYTE;
    split.setType(type);
    byte [] value = new byte[valLen];
    System.arraycopy(bytes, splitOffset, value, 0, valLen);
    split.setValue(value);
    return split;
  }

  //---------------------------------------------------------------------------
  //
  //  Compare specified fields against those contained in this KeyValue
  //
  //---------------------------------------------------------------------------

  /**
   * Compare key portion of a {@link KeyValue}.
   */
  public static class KeyComparator {
    volatile boolean ignoreTimestamp = false;
    volatile boolean ignoreType = false;

    public int compare(byte[] left, int loffset, int llength, byte[] right,
        int roffset, int rlength) {
      // Compare row
      short lrowlength = Bytes.toShort(left, loffset);
      short rrowlength = Bytes.toShort(right, roffset);
      int compare = compareRows(left, loffset + Bytes.SIZEOF_SHORT,
          lrowlength, right, roffset + Bytes.SIZEOF_SHORT, rrowlength);
      if (compare != 0) {
        return compare;
      }

      // Compare the rest of the two KVs without making any assumptions about
      // the common prefix. This function will not compare rows anyway, so we
      // don't need to tell it that the common prefix includes the row.
      return compareWithoutRow(0, left, loffset, llength, right, roffset,
          rlength, rrowlength);
    }

    /**
     * Compare columnFamily, qualifier, timestamp, and key type (everything
     * except the row). This method is used both in the normal comparator and
     * the "same-prefix" comparator. Note that we are assuming that row portions
     * of both KVs have already been parsed and found identical, and we don't
     * validate that assumption here.
     * @param commonPrefix
     *          the length of the common prefix of the two key-values being
     *          compared, including row length and row
     */
    private int compareWithoutRow(int commonPrefix, byte[] left, int loffset,
        int llength, byte[] right, int roffset, int rlength, short rowlength) {
      /***
       * KeyValue Format and commonLength:
       * |_keyLen_|_valLen_|_rowLen_|_rowKey_|_famiLen_|_fami_|_Quali_|....
       * ------------------|-------commonLength--------|--------------
       */
      int commonLength = ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + rowlength;

      // commonLength + TIMESTAMP_TYPE_SIZE
      int commonLengthWithTSAndType = TIMESTAMP_TYPE_SIZE + commonLength;
      // ColumnFamily + Qualifier length.
      int lcolumnlength = llength - commonLengthWithTSAndType;
      int rcolumnlength = rlength - commonLengthWithTSAndType;

      byte ltype = left[loffset + (llength - 1)];
      byte rtype = right[roffset + (rlength - 1)];

      // If the column is not specified, the "minimum" key type appears the
      // latest in the sorted order, regardless of the timestamp. This is used
      // for specifying the last key/value in a given row, because there is no
      // "lexicographically last column" (it would be infinitely long). The
      // "maximum" key type does not need this behavior.
      if (lcolumnlength == 0 && ltype == Type.Minimum.getCode()) {
        // left is "bigger", i.e. it appears later in the sorted order
        return 1;
      }
      if (rcolumnlength == 0 && rtype == Type.Minimum.getCode()) {
        return -1;
      }

      int lfamilyoffset = commonLength + loffset;
      int rfamilyoffset = commonLength + roffset;

      // Column family length.
      int lfamilylength = left[lfamilyoffset - 1];
      int rfamilylength = right[rfamilyoffset - 1];
      // If left family size is not equal to right family size, we need not
      // compare the qualifiers. 
      boolean sameFamilySize = (lfamilylength == rfamilylength);
      int common = 0;
      if (commonPrefix > 0) {
        common = Math.max(0, commonPrefix - commonLength);
        if (!sameFamilySize) {
          // Common should not be larger than Math.min(lfamilylength,
          // rfamilylength).
          common = Math.min(common, Math.min(lfamilylength, rfamilylength));
        } else {
          common = Math.min(common, Math.min(lcolumnlength, rcolumnlength));
        }
      }
      if (!sameFamilySize) {
        // comparing column family is enough.
        return Bytes.compareTo(left, lfamilyoffset + common, lfamilylength
            - common, right, rfamilyoffset + common, rfamilylength - common);
      }
      // Compare family & qualifier together.
      final int comparison = Bytes.compareTo(left, lfamilyoffset + common,
          lcolumnlength - common, right, rfamilyoffset + common,
          rcolumnlength - common);
      if (comparison != 0) {
        return comparison;
      }
      return compareTimestampAndType(left, loffset, llength, right, roffset,
          rlength, ltype, rtype);
    }

    private int compareTimestampAndType(byte[] left, int loffset, int llength,
        byte[] right, int roffset, int rlength, byte ltype, byte rtype) {
      int compare;
      if (!this.ignoreTimestamp) {
        // Get timestamps.
        long ltimestamp = Bytes.toLong(left,
            loffset + (llength - TIMESTAMP_TYPE_SIZE));
        long rtimestamp = Bytes.toLong(right,
            roffset + (rlength - TIMESTAMP_TYPE_SIZE));
        compare = compareTimestamps(ltimestamp, rtimestamp);
        if (compare != 0) {
          return compare;
        }
      }

      if (!this.ignoreType) {
        // Compare types. Let the delete types sort ahead of puts; i.e. types
        // of higher numbers sort before those of lesser numbers. Maximum (255)
        // appears ahead of everything, and minimum (0) appears after
        // everything.
        return (0xff & rtype) - (0xff & ltype);
      }
      return 0;
    }

    public int compare(byte[] left, byte[] right) {
      return compare(left, 0, left.length, right, 0, right.length);
    }

    public int compareRows(byte [] left, int loffset, int llength,
        byte [] right, int roffset, int rlength) {
      return Bytes.compareTo(left, loffset, llength, right, roffset, rlength);
    }

    int compareTimestamps(final long ltimestamp, final long rtimestamp) {
      // The below older timestamps sorting ahead of newer timestamps looks
      // wrong but it is intentional. This way, newer timestamps are first
      // found when we iterate over a memstore and newer versions are the
      // first we trip over when reading from a store file.
      if (ltimestamp < rtimestamp) {
        return 1;
      } else if (ltimestamp > rtimestamp) {
        return -1;
      }
      return 0;
    }
  }

  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.length);
    out.write(this.bytes, this.offset, this.length);
  }

  public static KeyValue fromKey(byte[] key) {
    int len = key.length + (2 * Bytes.SIZEOF_INT);
    byte[] kvBytes = new byte[len];
    int pos = 0;
    pos = Bytes.putInt(kvBytes, pos, key.length);
    pos = Bytes.putInt(kvBytes, pos, 0);
    Bytes.putBytes(kvBytes, pos, key, 0, key.length);
    return new KeyValue(kvBytes);
  }
}
