/*
 * Copyright © 2014-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.TransactionContextFactory;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Objects;
import com.google.common.io.Closeables;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link DatasetContext} that allows to dynamically load datasets
 * into a started {@link TransactionContext}. Datasets acquired from this context are
 * distinct from any Datasets instantiated outside this class. Datasets are cached,
 * such that repeated calls to (@link #getDataset()} for the same dataset and arguments
 * return the same instance.
 *
 * The cache also maintains a transaction context and adds all acquired datasets to that
 * context, so that they participate in the transactions executed with that context. If a
 * dataset is dismissed during the course of a transaction, then this context delays the
 * dismissal until the transaction is complete.
 *
 * Optionally, this cache can have a set of static datasets that are added to every
 * transaction context created by the cache. Static datasets cannot be dismissed.
 *
 * Also, transaction-aware "datasets" that were not created by this DynamicDatasetCache,
 * can be added to the transaction context. This is useful for transaction-aware's that
 * do not implement a Dataset (such as queue consumers etc.).
 */
public abstract class DynamicDatasetCache implements DatasetContext, AutoCloseable, TransactionContextFactory {

  protected final SystemDatasetInstantiator instantiator;
  protected final TransactionSystemClient txClient;
  protected final NamespaceId namespace;
  protected final Map<String, String> runtimeArguments;

  /**
   * Create a dynamic dataset factory.
   *
   * @param txClient the transaction system client to use for new transaction contexts
   * @param namespace the {@link NamespaceId} in which all datasets are instantiated
   * @param runtimeArguments all runtime arguments that are available to datasets in the context. Runtime arguments
   *                         are expected to be scoped so that arguments for one dataset do not override arguments
   *                         of other datasets.
   */
  public DynamicDatasetCache(SystemDatasetInstantiator instantiator,
                             TransactionSystemClient txClient,
                             NamespaceId namespace,
                             Map<String, String> runtimeArguments) {
    this.instantiator = instantiator;
    this.txClient = txClient;
    this.namespace = namespace;
    this.runtimeArguments = runtimeArguments;
  }

  @Override
  public final <T extends Dataset> T getDataset(String name)
    throws DatasetInstantiationException {
    return getDataset(name, DatasetDefinition.NO_ARGUMENTS);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name)
    throws DatasetInstantiationException {
    return getDataset(namespace, name, DatasetDefinition.NO_ARGUMENTS);
  }

  @Override
  public final <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return getDataset(name, arguments, false);
  }

  @Override
  public final <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return getDataset(namespace, name, arguments, false);
  }

  /**
   * Instantiate a dataset, allowing to bypass the cache. This means that the dataset will not be added to
   * the in-progress transactions, and it will also not be closed when the cache is closed.
   *
   * @param name the name of the dataset
   * @param arguments arguments for the dataset
   * @param bypass whether to bypass the cache
   * @param <T> the type of the dataset
   */
  public final <T extends Dataset> T getDataset(String name, Map<String, String> arguments, boolean bypass)
    throws DatasetInstantiationException {
    return getDataset(name, arguments, bypass, AccessType.UNKNOWN);
  }

  /**
   * Instantiate a dataset, allowing to bypass the cache. This means that the dataset will not be added to
   * the in-progress transactions, and it will also not be closed when the cache is closed.
   *
   * @param namespace the namespace name of dataset
   * @param name the name of the dataset
   * @param arguments arguments for the dataset
   * @param bypass whether to bypass the cache
   * @param <T> the type of the dataset
   */
  public final <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments,
                                                boolean bypass) throws DatasetInstantiationException {
    return getDataset(namespace, name, arguments, bypass, AccessType.UNKNOWN);
  }

  /**
   * Get an instance of the specified dataset, with the specified access type.
   *
   * @param name the name of the dataset
   * @param arguments arguments for the dataset
   * @param <T> the type of the dataset
   * @param accessType the accessType
   */
  public final <T extends Dataset> T getDataset(String name, Map<String, String> arguments,
                                                AccessType accessType) throws DatasetInstantiationException {
    return getDataset(name, arguments, false, accessType);
  }

  /**
   * Get an instance of the specified dataset, with the specified access type.
   *
   * @param namespace the namespace of the dataset
   * @param name the name of the dataset
   * @param arguments arguments for the dataset
   * @param <T> the type of the dataset
   * @param accessType the accessType
   */
  public final <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments,
                                                AccessType accessType) throws DatasetInstantiationException {
    return getDataset(namespace, name, arguments, false, accessType);
  }

  /**
   * Instantiate a dataset, allowing to bypass the cache. This means that the dataset will not be added to
   * the in-progress transactions, and it will also not be closed when the cache is closed.
   *
   * @param name the name of the dataset
   * @param arguments arguments for the dataset
   * @param bypass whether to bypass the cache
   * @param <T> the type of the dataset
   * @param accessType the accessType
   */
  public final <T extends Dataset> T getDataset(String name, Map<String, String> arguments, boolean bypass,
                                                AccessType accessType) throws DatasetInstantiationException {
    return getDataset(namespace.getNamespace(), name, arguments, bypass, accessType);
  }

  /**
   * Instantiate a dataset, allowing to bypass the cache. This means that the dataset will not be added to
   * the in-progress transactions, and it will also not be closed when the cache is closed.
   *
   * @param namespace the namespace instance
   * @param name the name of the dataset
   * @param arguments arguments for the dataset
   * @param bypass whether to bypass the cache
   * @param <T> the type of the dataset
   * @param accessType the accessType
   */
  public final <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments,
                                                boolean bypass, AccessType accessType)
    throws DatasetInstantiationException {
    if (namespace.equals(NamespaceId.SYSTEM.getNamespace()) && !this.namespace.equals(NamespaceId.SYSTEM)) {
      throw new DatasetInstantiationException(
        String.format("Cannot access dataset %s in system namespace, from %s namespace",
                      name, this.namespace.getNamespace()));
    }
    // apply actual runtime arguments on top of the context's runtime arguments for this dataset
    Map<String, String> dsArguments = RuntimeArguments.extractScope(Scope.DATASET, name, runtimeArguments);
    dsArguments.putAll(arguments);

    // Need to switch the context classloader to the CDAP system since getting dataset instance is in CDAP context
    // The internal of the dataset instantiate may switch the context class loader to a different one when necessary
    ClassLoader currentClassLoader = ClassLoaders.setContextClassLoader(getClass().getClassLoader());
    try {
      return getDataset(new DatasetCacheKey(namespace, name, dsArguments, accessType), bypass);
    } finally {
      ClassLoaders.setContextClassLoader(currentClassLoader);
    }
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    discardDataset(dataset);
  }

  /**
   * To be implemented by subclasses.
   *
   * @param bypass if true, bypass the dataset cache, and do not add this to the transaction.
   */
  protected abstract <T extends Dataset> T getDataset(DatasetCacheKey key, boolean bypass)
    throws DatasetInstantiationException;

  /**
   * Return a new transaction context for the current thread. All transaction-aware static datasets and all
   * extra transaction-awares are added to this transaction initially. Also, any transaction-aware that was
   * previously dynamically acquired, and that has not been garbage-collected, is added to the transaction.
   * Any transaction-aware datasets that will subsequently be obtained via (@link #getDataset()) will then
   * also be added to this transaction context and thus participate in its transaction. These datasets can
   * also be retrieved using {@link #getTransactionAwares()}.
   *
   * @return a new transaction context
   */
  @Override
  public abstract TransactionContext newTransactionContext() throws TransactionFailureException;

  /**
   * Dismiss the current transaction context. This releases the references to the context's
   * transaction-aware datasets so that they can be collected by the garbage collector (if no one
   * else is holding a reference to them). The static datasets and the extra transaction-awares,
   * however, will not be made available to garbage collection, and will participate in the
   * next transaction (created by {@link #newTransactionContext()}).
   */
  public abstract void dismissTransactionContext();

  /**
   * @return the static datasets that are transaction-aware. This is the same independent of whether a
   * transaction context was started using {@link #newTransactionContext()}.
   */
  public abstract Iterable<TransactionAware> getStaticTransactionAwares();

  /**
   * @return the transaction-aware datasets that participate in the current transaction. If
   * {@link #newTransactionContext()} has not been called (or {@link #dismissTransactionContext()} has been
   * called), then there is no transaction and this will return an empty iterable.
   */
  public abstract Iterable<TransactionAware> getTransactionAwares();

  /**
   * @return the current list of extra {@link TransactionAware}s that were added through the
   * {@link #addExtraTransactionAware(TransactionAware)} method.
   */
  public abstract Iterable<TransactionAware> getExtraTransactionAwares();

  /**
   * Add an extra transaction aware to the static datasets. This is a transaction aware that
   * is not instantiated through this factory, but needs to participate in every transaction.
   * Note that if a transaction is in progress, then this transaction aware will join that transaction.
   */
  public abstract void addExtraTransactionAware(TransactionAware txAware);

  /**
   * Remove a transaction-aware that was added via {@link #addExtraTransactionAware(TransactionAware)}.
   * Note that if a transaction is in progress, then this transaction aware will leave that transaction.
   */
  public abstract void removeExtraTransactionAware(TransactionAware txAware);

  /**
   * Close and dismiss all datasets that were obtained through this factory, and destroy the factory.
   * If an extra transaction-awares were added to this cache (and not removed), then they will also
   * be closed.
   */
  @Override
  public void close() {
    Closeables.closeQuietly(instantiator);
  }

  /**
   * Close and dismiss all datasets that were obtained through this factory. This can be used to ensure
   * that all resources held by datasets are released, even though the factory may be still be used for
   * subsequent execution.
   */
  public abstract void invalidate();

  /**
   * A key used by implementations of {@link DynamicDatasetCache} to cache Datasets. Includes the namespace of the
   * dataset, dataset name, its arguments, and its {@link AccessType}.
   */
  protected static final class DatasetCacheKey {
    private final String namespace;
    private final String name;
    private final Map<String, String> arguments;
    private final AccessType accessType;

    protected DatasetCacheKey(String namespace, String name, @Nullable Map<String, String> arguments) {
      this(namespace, name, arguments, AccessType.UNKNOWN);
    }

    protected DatasetCacheKey(String namespace, String name, @Nullable Map<String, String> arguments,
                              AccessType accessType) {
      this.namespace = namespace;
      this.name = name;
      this.arguments = arguments == null
        ? DatasetDefinition.NO_ARGUMENTS
        : Collections.unmodifiableMap(new HashMap<>(arguments));
      this.accessType = accessType;
    }

    public String getNamespace() {
      return namespace;
    }

    public String getName() {
      return name;
    }

    public Map<String, String> getArguments() {
      return arguments;
    }

    public AccessType getAccessType() {
      return accessType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DatasetCacheKey that = (DatasetCacheKey) o;

      // Omit accessType here since we don't have to request a another dataset instance just because
      // of the different accessType. Same for the hashCode() method
      return Objects.equal(this.namespace, that.namespace) &&
        Objects.equal(this.name, that.name) &&
        Objects.equal(this.arguments, that.arguments);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, name, arguments);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("namespace", namespace)
        .add("name", name)
        .add("arguments", arguments)
        .add("accessType", accessType)
        .toString();
    }
  }
}

