package com.continuuity.common.lang;

import java.io.IOException;

/**
 *
 */
public class ApiResourceList {

  private Iterable<String> resourceList;

  public synchronized Iterable<String> getResourceList() throws IOException {
    if (resourceList == null) {
      resourceList = ClassLoaders.getApiResourceList(ApiResourceList.class.getClassLoader());
    }
    return resourceList;
  }
}
