package co.cask.cdap.proto;

import java.util.Map;

/**
 * POJO that specifies input parameters to create Adapter
 */
public final class AdapterConfig {
  public String type;
  public Map<String, String> properties;

  public Source source;
  public Sink sink;

  public static final class Source {
    public String name;
    public Map<String, String> properties;

    public Source() {}

    public Source(String name, Map<String, String> properties) {
      this.name = name;
      this.properties = properties;
    }
  }

  public static final class Sink {
    public String name;
    public Map<String, String> properties;

    public Sink() {}

    public Sink(String name, Map<String, String> properties) {
      this.name = name;
      this.properties = properties;
    }
  }
}