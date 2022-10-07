package io.cdap.cdap.spi.events.trigger;

public class PluginPropertyMapping {
  private final String source;
  private final String target;
  private final String pipelineNamespace;
  private final String pipelineName;
  private final String stageName;

  public PluginPropertyMapping(String source, String target, String pipelineNamespace,
                               String pipelineName, String stageName) {
    this.source = source;
    this.target = target;
    this.pipelineNamespace = pipelineNamespace;
    this.pipelineName = pipelineName;
    this.stageName = stageName;
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

  public String getStageName() {
    return stageName;
  }

  @Override
  public String toString() {
    return "PluginPropertyMapping{" +
      "source='" + source + '\'' +
      ", target='" + target + '\'' +
      ", pipelineNamespace='" + pipelineNamespace + '\'' +
      ", pipelineName='" + pipelineName + '\'' +
      ", stageName='" + stageName + '\'' +
      '}';
  }
}
