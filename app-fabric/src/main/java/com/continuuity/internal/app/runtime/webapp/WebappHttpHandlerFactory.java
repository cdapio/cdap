package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.weave.filesystem.Location;

/**
 * Factory to create Webapp HttpHandlers.
 */
public interface WebappHttpHandlerFactory {

  JarHttpHandler createHandler(Location jarLocation);

}
