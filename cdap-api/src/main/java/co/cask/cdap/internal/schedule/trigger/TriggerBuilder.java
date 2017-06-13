package co.cask.cdap.internal.schedule.trigger;

/**
 * A builder to create a trigger.
 */
public interface TriggerBuilder extends Trigger {

  /**
   * Builds a trigger.
   *
   * @param namespace the namespace
   * @param application the deployed application name
   * @param applicationVersion the deployed application version
   * @return a Trigger
   */
  Trigger build(String namespace, String application, String applicationVersion);
}

