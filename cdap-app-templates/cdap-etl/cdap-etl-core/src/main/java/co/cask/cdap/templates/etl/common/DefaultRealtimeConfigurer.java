package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.Resources;
import co.cask.cdap.templates.etl.api.realtime.RealtimeConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSpecification;

/**
 * Default implementation of {@link RealtimeConfigurer}.
 */
public class DefaultRealtimeConfigurer extends DefaultStageConfigurer implements RealtimeConfigurer {
  private Resources resources;

  public DefaultRealtimeConfigurer(Class klass) {
    super(klass);
    this.resources = new Resources();
  }

  @Override
  public void setResources(Resources resources) {
    this.resources = resources;
  }

  @Override
  public RealtimeSpecification createSpecification() {
    return new RealtimeSpecification(className, name, description, properties, resources);
  }
}
