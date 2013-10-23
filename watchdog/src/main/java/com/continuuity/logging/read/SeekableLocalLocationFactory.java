package com.continuuity.logging.read;

import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.inject.Inject;

import java.net.URI;

/**
 * LocationFactory for SeekableLocalLocation.
 */
public class SeekableLocalLocationFactory implements LocationFactory {
  private final LocationFactory delegate;

  @Inject
  public SeekableLocalLocationFactory(LocationFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public Location create(String path) {
    return new SeekableLocalLocation(delegate.create(path));
  }

  @Override
  public Location create(URI uri) {
    return new SeekableLocalLocation(delegate.create(uri));
  }

  @Override
  public Location getHomeLocation() {
    return new SeekableLocalLocation(delegate.getHomeLocation());
  }
}
