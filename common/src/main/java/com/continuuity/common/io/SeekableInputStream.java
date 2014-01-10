/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract base class for {@link InputStream} that implements the {@link Seekable} interface.
 */
public abstract class SeekableInputStream extends FilterInputStream implements Seekable {

  /**
   * Creates a {@link SeekableInputStream} from the given {@link FileInputStream}.
   */
  public static SeekableInputStream create(final FileInputStream input) {
    return new SeekableInputStream(input) {
      @Override
      public void seek(long pos) throws IOException {
        input.getChannel().position(pos);
      }

      @Override
      public long getPos() throws IOException {
        return input.getChannel().position();
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
      }
    };
  }

  /**
   * Creates a {@link SeekableInputStream} from the given {@link FSDataInputStream}.
   */
  public static SeekableInputStream create(final FSDataInputStream input) {
    return new SeekableInputStream(input) {
      @Override
      public void seek(long pos) throws IOException {
        input.seek(pos);
      }

      @Override
      public long getPos() throws IOException {
        return input.getPos();
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        return input.seekToNewSource(targetPos);
      }
    };
  }

  protected SeekableInputStream(InputStream in) {
    super(in);
  }
}
