package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.TimeToLiveSupported;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Holds namespacing logic.
 * NOTE: This logic should be moved into DataSetService when we have it.
 */
public abstract class NamespacingDataSetAccessor implements DataSetAccessor {
  protected abstract <T> T getDataSetClient(String name, Class<? extends T> type, @Nullable Properties props)
    throws Exception;
  protected abstract <T> DataSetManager getDataSetManager(Class<? extends T> type) throws Exception;
  // todo: for now simplest support for managing datasets, should be improved with DataSetService
  protected abstract Map<String, Class<?>> list(String prefix) throws Exception;

  private final String reactorNameSpace;

  protected NamespacingDataSetAccessor(CConfiguration conf) {
    // todo: use single namespace for everything in reactor
    this.reactorNameSpace = conf.get(CFG_TABLE_PREFIX, DEFAULT_TABLE_PREFIX);
  }

  @Override
  public <T> T getDataSetClient(String name, Class<? extends T> type, Namespace namespace) throws Exception {
    return getDataSetClient(name, type, null, namespace);
  }

  @Override
  public <T> T getDataSetClient(String name, Class<? extends T> type, @Nullable Properties props, Namespace namespace)
    throws Exception {
    return getDataSetClient(namespace(name, namespace), type, props);
  }

  @Override
  public <T> DataSetManager getDataSetManager(Class<? extends T> type, Namespace namespace) throws Exception {
    return new NamespacedDataSetManager(namespace, getDataSetManager(type));
  }

  @Override
  public String namespace(String name, Namespace namespace) {
    return reactorNameSpace + "." + namespace.namespace(name);
  }

  @Override
  public Map<String, Class<?>> list(Namespace namespace) throws Exception {
    Map<String, Class<?>> namespaced = list(namespace("", namespace));
    Map<String, Class<?>> unnamespaced = Maps.newHashMap();
    for (Map.Entry<String, Class<?>> ds : namespaced.entrySet()) {
      unnamespaced.put(unnamespace(ds.getKey(), namespace), ds.getValue());
    }
    return unnamespaced;
  }

  @Override
  public void dropAll(Namespace namespace) throws Exception {
    for (Map.Entry<String, Class<?>> dataset : list(namespace).entrySet()) {
      getDataSetManager(dataset.getValue(), namespace).drop(dataset.getKey());
    }
  }

  @Override
  public void truncateAll(Namespace namespace) throws Exception {
    truncateAllExceptBlacklist(namespace, Collections.<String>emptySet());
  }

  @Override
  public void truncateAllExceptBlacklist(Namespace namespace, Set<String> blacklist) throws Exception {
    for (Map.Entry<String, Class<?>> dataset : list(namespace).entrySet()) {
      if (!blacklist.contains(dataset.getKey())) {
        getDataSetManager(dataset.getValue(), namespace).truncate(dataset.getKey());
      }
    }
  }

  private String unnamespace(String name, Namespace namespace) {
    return name.substring(namespace("", namespace).length());
  }

  private class NamespacedDataSetManager implements DataSetManager, TimeToLiveSupported {
    private final Namespace namespace;
    private final DataSetManager delegate;

    private NamespacedDataSetManager(Namespace namespace, DataSetManager delegate) {
      this.namespace = namespace;
      this.delegate = delegate;
    }

    @Override
    public boolean exists(String name) throws Exception {
      return delegate.exists(namespace(name, namespace));
    }

    @Override
    public void create(String name) throws Exception {
      delegate.create(namespace(name, namespace));
    }

    @Override
    public void create(String name, @Nullable Properties props) throws Exception {
      delegate.create(namespace(name, namespace), props);
    }

    @Override
    public void truncate(String name) throws Exception {
      delegate.truncate(namespace(name, namespace));
    }

    @Override
    public void drop(String name) throws Exception {
      delegate.drop(namespace(name, namespace));
    }

    @Override
    public void upgrade(String name, Properties properties) throws Exception {
      delegate.upgrade(namespace(name, namespace), properties);
    }

    @Override
    public boolean isTTLSupported() {
      return (delegate instanceof TimeToLiveSupported) && ((TimeToLiveSupported) delegate).isTTLSupported();
    }
  }
}
