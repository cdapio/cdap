package io.cdap.cdap.spi.events.trigger;

public class ArgumentMapping {
  private final String source;
  private final String target;
  private final String pipelineNamespace;
  private final String pipelineName;

  public ArgumentMapping(String source, String target, String pipelineNamespace, String pipelineName) {
    this.source = source;
    this.target = target;
    this.pipelineNamespace = pipelineNamespace;
    this.pipelineName = pipelineName;
  }

  public String getSource() {
    return source;
  }

  public String getTarget() {
    return target;
  }

  public String getPipelineNamespace() {
    return pipelineNamespace;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  @Override
  public String toString() {
    return "ArgumentMapping{" +
      "source='" + source + '\'' +
      ", target='" + target + '\'' +
      ", pipelineNamespace='" + pipelineNamespace + '\'' +
      ", pipelineName='" + pipelineName + '\'' +
      '}';
  }
}
