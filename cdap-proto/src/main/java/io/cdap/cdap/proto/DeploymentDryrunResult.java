package io.cdap.cdap.proto;

import io.cdap.cdap.api.app.ApplicationValidationResult;

public class DeploymentDryrunResult {
  private ApplicationValidationResult validationResult;

  public DeploymentDryrunResult(ApplicationValidationResult validationResult) {
    this.validationResult = validationResult;
  }

  public ApplicationValidationResult getValidationResult() { return validationResult; }
}
