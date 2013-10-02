package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.weave.filesystem.Location;

/**
 * HttpHandler for webapps.
 */
public interface WebappHttpHandler extends HttpHandler {

  void setJarLocation(Location jarLocation);
}
