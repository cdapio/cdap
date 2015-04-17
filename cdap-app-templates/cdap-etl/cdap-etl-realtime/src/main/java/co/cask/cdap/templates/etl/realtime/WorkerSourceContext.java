package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSpecification;
import co.cask.cdap.templates.etl.api.realtime.SourceContext;

import java.util.Map;

/**
 * Implementation of {@link SourceContext} for a {@link Worker} driver.
 */
public class WorkerSourceContext implements SourceContext {
  private final WorkerContext context;
  private final StageSpecification specification;
  private final ETLStage stage;

  public WorkerSourceContext(WorkerContext context, ETLStage sourceStage, StageSpecification specification) {
    this.context = context;
    this.specification = specification;
    this.stage = sourceStage;
  }

  @Override
  public RealtimeSpecification getSpecification() {
    return (RealtimeSpecification) specification;
  }

  @Override
  public int getInstanceId() {
    return context.getInstanceId();
  }

  @Override
  public int getInstanceCount() {
    return context.getInstanceCount();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return stage.getProperties();
  }
}
