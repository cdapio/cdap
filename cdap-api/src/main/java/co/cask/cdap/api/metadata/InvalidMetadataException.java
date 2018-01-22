package co.cask.cdap.api.metadata;

import co.cask.cdap.api.common.HttpErrorStatusProvider;

/**
 * Created by rsinha on 1/22/18.
 */
public class InvalidMetadataException  extends Exception implements HttpErrorStatusProvider {

  private final MetadataEntity targetId;

  public InvalidMetadataException(MetadataEntity targetId, String message) {
    super("Unable to set metadata for " + targetId + " with error: " + message);
    this.targetId = targetId;
  }

  @Override
  public int getStatusCode() {
    return 400;
  }
}
