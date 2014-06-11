package com.continuuity.logging.write;

import com.continuuity.common.io.Locations;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

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

  static Location getParent(Location location) {
    Location parent = Locations.getParent(location);
    return (parent == null) ? location : parent;
  }
}
