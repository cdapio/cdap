package com.continuuity.logging.read;

import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Seekable;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Supports seekable file input stream.
 */
public class SeekableLocalLocation implements Location {
  private final Location delegate;

  public SeekableLocalLocation(Location delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean exists() throws IOException {
    return delegate.exists();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public boolean createNew() throws IOException {
    return delegate.createNew();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    InputStream inputStream = delegate.getInputStream();

    Preconditions.checkArgument(inputStream instanceof Seekable || inputStream instanceof FileInputStream);

    if (inputStream instanceof Seekable) {
      return inputStream;
    }

    FileInputStream fileInputStream = (FileInputStream) inputStream;
    return new SeekableInputStream(fileInputStream);
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return delegate.getOutputStream();
  }

  @Override
  public Location append(String child) throws IOException {
    return delegate.append(child);
  }

  @Override
  public Location getTempFile(String suffix) throws IOException {
    return delegate.getTempFile(suffix);
  }

  @Override
  public URI toURI() {
    return delegate.toURI();
  }

  @Override
  public boolean delete() throws IOException {
    return delegate.delete();
  }

  @Override
  public boolean delete(boolean recursive) throws IOException {
    return delegate.delete(recursive);
  }

  @Override
  @Nullable
  public Location renameTo(Location destination) throws IOException {
    return delegate.renameTo(destination);
  }

  @Override
  public boolean mkdirs() throws IOException {
    return delegate.mkdirs();
  }

  @Override
  public long length() throws IOException {
    return delegate.length();
  }

  @Override
  public long lastModified() throws IOException {
    return delegate.lastModified();
  }

  private class SeekableInputStream extends FSInputStream {
    private final FileInputStream fileInputStream;

    private SeekableInputStream(FileInputStream fileInputStream) {
      this.fileInputStream = fileInputStream;
    }

    @Override
    public void seek(long pos) throws IOException {
      fileInputStream.getChannel().position(pos);
    }

    @Override
    public long getPos() throws IOException {
      return fileInputStream.getChannel().position();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read() throws IOException {
      return fileInputStream.read();
    }
  }
}
