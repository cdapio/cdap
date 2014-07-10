package com.continuuity.common.lang;

import com.google.inject.AbstractModule;

/**
 *
 */
public class ApiResourceListModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ApiResourceList.class).toInstance(new ApiResourceList());
  }
}
