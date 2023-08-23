package io.cdap.cdap.etl.proto.v2;

import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import java.util.Objects;

public class ETLStageValidationResult {
  private final String name;
  private final String label;
  private final String id;
  private final String errorType;
  private final String pluginName;
  private final String pluginType;
  private final ArtifactSelectorConfig currentArtifact;
  private final ArtifactSelectorConfig suggestedArtifact;

  public ETLStageValidationResult(
      String name,
      String label,
      String id,
      String pluginName,
      String pluginType,
      String errorType,
      ArtifactSelectorConfig currentArtifact,
      ArtifactSelectorConfig suggestedArtifact
  ) {
    this.name = name;
    this.label = label;
    this.id = id;
    this.pluginName = pluginName;
    this.pluginType = pluginType;
    this.errorType = errorType;
    this.currentArtifact = currentArtifact;
    this.suggestedArtifact = suggestedArtifact;
  }

  public String getName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public String getId() {
    return id;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getPluginType() {
    return pluginType;
  }

  public String getErrorType() {
    return errorType;
  }

  public ArtifactSelectorConfig getCurrentArtifact() {
    return currentArtifact;
  }

  public ArtifactSelectorConfig getSuggestedArtifact() {
    return suggestedArtifact;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ETLStageValidationResult)) {
      return false;
    }
    ETLStageValidationResult that = (ETLStageValidationResult) o;
    return Objects.equals(name, that.name) && Objects.equals(label, that.label)
        && Objects.equals(id, that.id) && Objects.equals(errorType,
        that.errorType) && Objects.equals(pluginName, that.pluginName)
        && Objects.equals(pluginType, that.pluginType) && Objects.equals(
        currentArtifact, that.currentArtifact) && Objects.equals(suggestedArtifact,
        that.suggestedArtifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, label, id, errorType, pluginName, pluginType, currentArtifact,
        suggestedArtifact);
  }
}
