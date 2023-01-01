package io.cdap.cdap.proto;

/*
*  Represents metadata for source control of a Namespace.
*/
public class SourceControlMeta {
  private final String repositoryURL;

  public String getRepositoryURL() {
    return repositoryURL;
  }

  public String getPersonalAccessToken() {
    return personalAccessToken;
  }

  private final String personalAccessToken;

  @Override
  public String toString() {
    return "SourceControlMeta{" +
      "repositoryURL='" + repositoryURL + '\'' +
      ", personalAccessToken='" + personalAccessToken + '\'' +
      '}';
  }

  public SourceControlMeta(String repositoryURL, String personalAccessToken) {
    this.personalAccessToken = personalAccessToken;
    this.repositoryURL = repositoryURL;
  }
}
