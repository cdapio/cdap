package co.cask.cdap.templates.etl.common;

import co.cask.cdap.templates.etl.api.config.ETLStage;

import java.util.List;

/**
 * Common ETL Config.
 */
public class ETLConfig {
  private final ETLStage source;
  private final ETLStage sink;
  private final List<ETLStage> transforms;

  public ETLConfig(ETLStage source, ETLStage sink, List<ETLStage> transforms) {
    this.source = source;
    this.sink = sink;
    this.transforms = transforms;
  }

  public ETLStage getSource() {
    return source;
  }

  public ETLStage getSink() {
    return sink;
  }

  public List<ETLStage> getTransforms() {
    return transforms;
  }
}
