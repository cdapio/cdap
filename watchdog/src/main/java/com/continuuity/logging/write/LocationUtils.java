package com.continuuity.logging.write;

import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;

/**
 * Location utils.
 */
class LocationUtils {

  static Location normalize(LocationFactory locationFactory, Location path) {
    String p = path.toURI().getRawPath();
    if (p.length() > 1 && p.endsWith("/")) {
      return locationFactory.create(p.replaceAll("/+$", ""));
    }
    return path;
  }

  static Location getParent(LocationFactory locationFactory, Location location) {
    String path = location.toURI().getRawPath();
    if (path.length() > 1 && path.endsWith("/")) {
      path = path.replaceAll("/+$", "");
    }

    if (path.equals("/")) {
      return location;
    }
    return locationFactory.create(path.substring(0, path.lastIndexOf("/")));
  }
}
