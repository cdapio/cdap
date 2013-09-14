package com.continuuity.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.api.DataSetManager;

/**
 * Holds namespacing logic.
 * NOTE: This logic should be moved into DataSetService when we have it.
 */
public abstract class AbstractDataSetAccessor implements DataSetAccessor {
  protected abstract <T> T getDataSetClient(String name, Class<? extends T> type) throws Exception;
  protected abstract <T> DataSetManager getDataSetManager(Class<? extends T> type) throws Exception;

  private final String reactorNameSpace;

  protected AbstractDataSetAccessor(CConfiguration conf) {
    // todo: use single namespace for everything in reactor
    this.reactorNameSpace = conf.get(CFG_TABLE_PREFIX, DEFAULT_TABLE_PREFIX);
  }

  @Override
  public <T> T getDataSetClient(String name, Class<? extends T> type, Namespace namespace) throws Exception {
    return getDataSetClient(namespace(name, namespace), type);
  }

  @Override
  public <T> DataSetManager getDataSetManager(Class<? extends T> type, Namespace namespace) throws Exception {
    return new NamespacedDataSetManager(namespace, getDataSetManager(type));
  }

  @Override
  public String namespace(String name, Namespace namespace) {
    return reactorNameSpace + "." + namespace.namespace(name);
  }

  private class NamespacedDataSetManager implements DataSetManager {
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
    public void truncate(String name) throws Exception {
      delegate.truncate(namespace(name, namespace));
    }

    @Override
    public void drop(String name) throws Exception {
      delegate.drop(namespace(name, namespace));
    }
  }
}
