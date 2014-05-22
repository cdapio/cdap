package com.continuuity.api;

import com.continuuity.api.annotation.Beta;

/**
 * Base class for Reactor Application.
 *
 * TODO: in future, it will be renamed to Application and be the only available API for application configuration
 */
@Beta
public abstract class AbstractApplication {
  /**
   * Configures application.
   * @param configurer collects application configuration
   * @param context to be used to access env/app configuration and app arguments (e.g. deploy args)
   */
  public abstract void configure(ApplicationConfigurer configurer, ApplicationContext context);
}
