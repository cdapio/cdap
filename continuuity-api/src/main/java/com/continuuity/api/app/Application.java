package com.continuuity.api.app;

import com.continuuity.api.annotation.Beta;

/**
 * Defines Continuuity Reactor Application.
 *
 */
@Beta
public interface Application {
  /**
   * Configures application.
   * @param configurer collects application configuration
   * @param context to be used to access env/app configuration and app arguments (e.g. deploy args)
   */
  void configure(ApplicationConfigurer configurer, ApplicationContext context);
}
