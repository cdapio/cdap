package io.cdap.cdap.api.app;

public class ApplicationValidationResult {
  private final String result;

  public ApplicationValidationResult(String result) {
    this.result = result;
  }

  public String getResult() { return result; }
}
