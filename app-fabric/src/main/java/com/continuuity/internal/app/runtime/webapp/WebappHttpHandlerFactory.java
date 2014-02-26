package com.continuuity.internal.app.runtime.webapp;

import org.apache.twill.filesystem.Location;

/**
 * Factory to create Webapp HttpHandlers.
 */
public interface WebappHttpHandlerFactory {

  JarHttpHandler createHandler(Location jarLocation);

}
