/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.io;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 *
 */
public class CodecTest {

  @Test
  public void testCodec() throws IOException {
    PipedOutputStream output = new PipedOutputStream();
    PipedInputStream input = new PipedInputStream(output);

    Encoder encoder = new BinaryEncoder(output);
    Decoder decoder = new BinaryDecoder(input);

    encoder.writeNull();
    Assert.assertNull(decoder.readNull());

    encoder.writeBool(true);
    Assert.assertTrue(decoder.readBool());
    encoder.writeBool(false);
    Assert.assertFalse(decoder.readBool());

    encoder.writeInt(0);
    Assert.assertEquals(0, decoder.readInt());
    encoder.writeInt(-1);
    Assert.assertEquals(-1, decoder.readInt());
    encoder.writeInt(1234);
    Assert.assertEquals(1234, decoder.readInt());
    encoder.writeInt(-1234);
    Assert.assertEquals(-1234, decoder.readInt());
    encoder.writeInt(Short.MAX_VALUE);
    Assert.assertEquals(Short.MAX_VALUE, decoder.readInt());
    encoder.writeInt(Short.MIN_VALUE);
    Assert.assertEquals(Short.MIN_VALUE, decoder.readInt());
    encoder.writeInt(Integer.MAX_VALUE);
    Assert.assertEquals(Integer.MAX_VALUE, decoder.readInt());
    encoder.writeInt(Integer.MIN_VALUE);
    Assert.assertEquals(Integer.MIN_VALUE, decoder.readInt());

    encoder.writeLong(0);
    Assert.assertEquals(0, decoder.readLong());
    encoder.writeLong(-20);
    Assert.assertEquals(-20, decoder.readLong());
    encoder.writeLong(30000);
    Assert.assertEquals(30000, decoder.readLong());
    encoder.writeLong(-600000);
    Assert.assertEquals(-600000, decoder.readLong());
    encoder.writeLong(Integer.MAX_VALUE);
    Assert.assertEquals(Integer.MAX_VALUE, decoder.readLong());
    encoder.writeLong(Integer.MIN_VALUE);
    Assert.assertEquals(Integer.MIN_VALUE, decoder.readLong());
    encoder.writeLong(Long.MAX_VALUE);
    Assert.assertEquals(Long.MAX_VALUE, decoder.readLong());
    encoder.writeLong(Long.MIN_VALUE);
    Assert.assertEquals(Long.MIN_VALUE, decoder.readLong());

    encoder.writeFloat(3.14f);
    Assert.assertEquals(3.14f, decoder.readFloat(), 0.0000001f);
    encoder.writeFloat(Short.MAX_VALUE);
    Assert.assertEquals(Short.MAX_VALUE, decoder.readFloat(), 0.0000001f);
    encoder.writeFloat(Integer.MIN_VALUE);
    Assert.assertEquals(Integer.MIN_VALUE, decoder.readFloat(), 0.0000001f);
    encoder.writeFloat((long) Integer.MAX_VALUE * Short.MAX_VALUE);
    Assert.assertEquals((long) Integer.MAX_VALUE * Short.MAX_VALUE, decoder.readFloat(), 0.0000001f);
    encoder.writeFloat(Float.MAX_VALUE);
    Assert.assertEquals(Float.MAX_VALUE, decoder.readFloat(), 0.0000001f);
    encoder.writeFloat(Float.MIN_VALUE);
    Assert.assertEquals(Float.MIN_VALUE, decoder.readFloat(), 0.0000001f);

    encoder.writeDouble(Math.E);
    Assert.assertEquals(Math.E, decoder.readDouble(), 0.0000001f);
    encoder.writeDouble(Integer.MAX_VALUE);
    Assert.assertEquals(Integer.MAX_VALUE, decoder.readDouble(), 0.0000001f);
    encoder.writeDouble(Long.MIN_VALUE);
    Assert.assertEquals(Long.MIN_VALUE, decoder.readDouble(), 0.0000001f);
    encoder.writeDouble((long) Integer.MAX_VALUE * Short.MAX_VALUE);
    Assert.assertEquals((long) Integer.MAX_VALUE * Short.MAX_VALUE, decoder.readDouble(), 0.0000001f);
    encoder.writeDouble(Double.MAX_VALUE);
    Assert.assertEquals(Double.MAX_VALUE, decoder.readDouble(), 0.0000001f);
    encoder.writeDouble(Double.MIN_VALUE);
    Assert.assertEquals(Double.MIN_VALUE, decoder.readDouble(), 0.0000001f);

    encoder.writeString("This is a testing message");
    Assert.assertEquals("This is a testing message", decoder.readString());
    String str = Character.toString((char) 200) + Character.toString((char) 20000) + Character.toString((char) 40000);
    encoder.writeString(str);
    Assert.assertEquals(str, decoder.readString());

    ByteBuffer buf = ByteBuffer.allocate(12);
    buf.asIntBuffer().put(10).put(1024).put(9999999);
    encoder.writeBytes(buf);
    IntBuffer inBuf = decoder.readBytes().asIntBuffer();
    Assert.assertEquals(10, inBuf.get());
    Assert.assertEquals(1024, inBuf.get());
    Assert.assertEquals(9999999, inBuf.get());
  }
}
