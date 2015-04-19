package co.cask.cdap;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;

/**
 *
 */
public class ActionBatchTemplate extends DummyBatchTemplate {

  @Override
  public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    configurer.setName(ActionBatchTemplate.class.getSimpleName());
    // make the description different each time to distinguish between deployed versions in unit tests
    configurer.setDescription("Hello World");
    configurer.addWorkflow(new AdapterWorkflow());
    configurer.addMapReduce(new DummyMapReduceJob());
  }
}
