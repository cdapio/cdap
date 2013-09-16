package com.continuuity.data;

import com.continuuity.data2.dataset.api.DataSetManager;

import java.util.Map;

/**
 *
 */
public interface DataSetAccessor {
  <T> T getDataSetClient(String name, Class<? extends T> type, Namespace namespace) throws Exception;
  <T> DataSetManager getDataSetManager(Class<? extends T> type, Namespace namespace) throws Exception;

  // TODO: below API should be a part of DataSetService, which we don't have yet

  public static final String DEFAULT_TABLE_PREFIX = "continuuity";
  public static final String CFG_TABLE_PREFIX = "data.table.prefix";

  Map<String, Class<?>> list(Namespace namespace) throws Exception;

  void dropAll(Namespace namespace) throws Exception;

  void truncateAll(Namespace namespace) throws Exception;

  // TODO: this should not be exposed, but since queues do not follow dataset semantic we have to do that
  String namespace(String datasetName, Namespace namespace);

  static enum Namespace {
    USER("user"),
    SYSTEM("system");

    private final String name;

    private Namespace(String name) {
      this.name = name;
    }

    String getName() {
      return name;
    }

    public String namespace(String datasetName) {
      return name + "." + datasetName;
    }
  }

}
