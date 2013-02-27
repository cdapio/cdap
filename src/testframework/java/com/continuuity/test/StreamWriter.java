package com.continuuity.test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public interface StreamWriter {

  void send(String content) throws IOException;

  void send(byte[] content) throws IOException;

  void send(byte[] content, int off, int len) throws IOException;

  void send(ByteBuffer buffer) throws IOException;
}
