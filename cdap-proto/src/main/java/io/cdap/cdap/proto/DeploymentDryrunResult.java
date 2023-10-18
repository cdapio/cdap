package io.cdap.cdap.proto;

import io.cdap.cdap.api.app.ApplicationValidationResult;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import java.util.Objects;


public class DeploymentDryrunResult {
  private final boolean shouldUpgradeApplicationArtifact;
  private final ArtifactSummary currentApplicationArtifact;
  private final ArtifactSummary suggestedApplicationArtifact;
  private final ApplicationValidationResult validationResult;

  public DeploymentDryrunResult(
      boolean shouldUpgradeApplicationArtifact,
      ArtifactSummary currentApplicationArtifact,
      ArtifactSummary suggestedApplicationArtifact,
      ApplicationValidationResult validationResult
  ) {
    this.shouldUpgradeApplicationArtifact = shouldUpgradeApplicationArtifact;
    this.currentApplicationArtifact = currentApplicationArtifact;
    this.suggestedApplicationArtifact = suggestedApplicationArtifact;
    this.validationResult = validationResult;
  }

  public boolean isShouldUpgradeApplicationArtifact() {
    return shouldUpgradeApplicationArtifact;
  }

  public ArtifactSummary getCurrentApplicationArtifact() {
    return currentApplicationArtifact;
  }

  public ArtifactSummary getSuggestedApplicationArtifact() {
    return suggestedApplicationArtifact;
  }

  public ApplicationValidationResult getValidationResult() { return validationResult; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeploymentDryrunResult)) {
      return false;
    }
    DeploymentDryrunResult that = (DeploymentDryrunResult) o;
    return shouldUpgradeApplicationArtifact == that.shouldUpgradeApplicationArtifact
        && Objects.equals(currentApplicationArtifact, that.currentApplicationArtifact)
        && Objects.equals(suggestedApplicationArtifact, that.suggestedApplicationArtifact)
        && Objects.equals(validationResult, that.validationResult);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shouldUpgradeApplicationArtifact, currentApplicationArtifact,
        suggestedApplicationArtifact, validationResult);
  }
}
