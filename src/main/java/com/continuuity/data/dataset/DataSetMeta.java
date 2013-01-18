package com.continuuity.data.dataset;

import java.util.HashMap;
import java.util.Map;

public class DataSetMeta {

  private final String name;
  private final String type;
  private final Map<String, String> properties;
  private final Map<String, DataSetMeta> dataSetMetas;

  public String getName() {
    return this.name;
  }
  public String getType() {
    return this.type;
  }
  public String getProperty(String key) {
    return properties.get(key);
  }
  public DataSetMeta getMetaFor(String dsName) {
    return dataSetMetas.get(dsName);
  }

  private DataSetMeta(String name, String type,
                      Map<String, String> properties,
                      Map<String, DataSetMeta> dataSetMetas) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.dataSetMetas = dataSetMetas;
  }

  public static class Builder {
    private String name;
    private String type;
    private Map<String, String> properties
        = new HashMap<String, String>();
    private Map<String, DataSetMeta> dataSetMetas
        = new HashMap<String, DataSetMeta>();

    public Builder(DataSet dataset) {
      this.name = dataset.getName();
      this.type = dataset.getClass().getCanonicalName();
    }

    public Builder property(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public Builder dataset(DataSetMeta.Builder builder) {
      DataSetMeta meta = builder.create();
      this.dataSetMetas.put(meta.getName(), meta);
      return this;
    }

    public DataSetMeta create() {
      return new DataSetMeta(this.name, this.type, this.properties,
          this.dataSetMetas);
    }
  }

}
