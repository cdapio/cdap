package com.continuuity.api.app;

/**
 * Defines a Continuuity Reactor Application.
 *
 */
public interface Application {
  /**
   * Configures the Application.
   *
   * @param configurer Collects the Application configuration
   * @param context Used to access the environment, application configuration, and application (deployment) arguments
   */
  void configure(ApplicationConfigurer configurer, ApplicationContext context);
}
