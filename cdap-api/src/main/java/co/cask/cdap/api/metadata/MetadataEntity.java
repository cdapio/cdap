package co.cask.cdap.api.metadata;

import co.cask.cdap.api.dataset.lib.KeyValue;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * A metadata entity
 */
public class MetadataEntity {
  private List<KeyValue> details;

  private MetadataEntity(List<KeyValue> details) {
    this.details = details;
  }

  public List<KeyValue> getDetails() {
    return details;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final List<KeyValue> details = new LinkedList<>();

    public Builder add(String k, String v) {
      details.add(new KeyValue<>(k, v));
      return this;
    }

    public Builder forDataset(String namespace, String datasetName) {
      details.add(new KeyValue<>("namespace", namespace));
      details.add(new KeyValue<>("dataset", datasetName));
      return this;
    }

    public Builder forDataset(String datasetName) {
      details.add(new KeyValue<>("dataset", datasetName));
      return this;
    }

    public Builder forStream(String namespace, String streamName) {
      details.add(new KeyValue<>("namespace", namespace));
      details.add(new KeyValue<>("stream", streamName));
      return this;
    }

    public Builder forStream(String streamName) {
      details.add(new KeyValue<>("stream", streamName));
      return this;
    }

    public Builder forApplication(String namespace, String applicationName) {
      details.add(new KeyValue<>("namespace", namespace));
      details.add(new KeyValue<>("application", applicationName));
      return this;
    }

    public Builder forApplication(String applicationName) {
      details.add(new KeyValue<>("application", applicationName));
      return this;
    }

    public MetadataEntity build() {
      return new MetadataEntity(Collections.unmodifiableList(details));
    }
  }
}
