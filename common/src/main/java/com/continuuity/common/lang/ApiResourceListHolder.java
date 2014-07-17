package com.continuuity.common.lang;

import java.io.IOException;

/**
 *
 */
public final class ApiResourceListHolder {

  private static Iterable<String> resourceList;

  private ApiResourceListHolder() { }

  public static synchronized Iterable<String> getResourceList() throws IOException {
    if (resourceList == null) {
      resourceList = ClassLoaders.getAPIResources(ApiResourceListHolder.class.getClassLoader());
    }
    return resourceList;
  }

}
