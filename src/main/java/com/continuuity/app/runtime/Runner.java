package com.continuuity.app.runtime;

import com.continuuity.filesystem.Location;

import java.util.Map;

/**
 *
 */
public interface Runner {

  Cancellable run(Location jarLocation, String name, Map<String, String> arguments);
}
