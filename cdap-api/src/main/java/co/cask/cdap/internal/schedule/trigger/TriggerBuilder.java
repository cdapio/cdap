package co.cask.cdap.internal.schedule.trigger;

/**
 * Created by sameetsapra on 6/12/17.
 */
public interface TriggerBuilder extends Trigger {

  Trigger build(String namespace, String application, String applicationVersion);
}

