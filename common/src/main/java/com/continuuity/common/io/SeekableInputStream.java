/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.io;

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
   * Creates a {@link SeekableInputStream} from the given {@link InputStream}. Exception will be
   * thrown if failed to do so.
   *
   * @throws java.io.IOException If the given input stream is not seekable.
   */
  public static SeekableInputStream create(InputStream input) throws IOException {
    if (input instanceof SeekableInputStream) {
      return (SeekableInputStream) input;
    }
    if (input instanceof FileInputStream) {
      return create((FileInputStream) input);
    }
    if (input instanceof Seekable) {
      final Seekable seekable = (Seekable) input;
      return new SeekableInputStream(input) {
        @Override
        public void seek(long pos) throws IOException {
          seekable.seek(pos);
        }

        @Override
        public long getPos() throws IOException {
          return seekable.getPos();
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
          return seekable.seekToNewSource(targetPos);
        }
      };
    }

    throw new IOException("Failed to create SeekableInputStream from " + input.getClass());
  }

  /**
   * Creates a {@link SeekableInputStream} from the given {@link FileInputStream}.
   */
  private static SeekableInputStream create(final FileInputStream input) {
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

  protected SeekableInputStream(InputStream in) {
    super(in);
  }
}
