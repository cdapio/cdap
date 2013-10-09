package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.weave.filesystem.Location;

/**
 * Factory to create Webapp HttpHandlers.
 */
public interface WebappHttpHandlerFactory {

  HttpHandler createHandler(Location jarLocation);

}
