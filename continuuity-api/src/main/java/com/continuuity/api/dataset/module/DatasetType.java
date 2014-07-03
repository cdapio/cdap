/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.dataset.module;

import com.continuuity.api.annotation.Beta;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare a dataset type from {@link com.continuuity.api.dataset.Dataset}.
 * The value is used as dataset type name.
 *
 * This is used when creating {@link com.continuuity.api.dataset.DatasetDefinition} from only
 * {@link com.continuuity.api.dataset.Dataset} implementation. See
 * {@link com.continuuity.api.app.ApplicationConfigurer#addDatasetType
 * com.continuuity.api.app.ApplicationConfigurer#addDatasetType(Class &lt;&#63; extends Dataset&gt; datasetClass)
 * }
 * for details.
 *
 * Example of usage:
 *
 * <pre>
 * <code>
 * {@literal @}DatasetType("KVTable")
 * public class SimpleKVTable extends AbstractDataset {
 *   public SimpleKVTable(DatasetSpecification spec, {@literal @}EmbeddedDataset("data") Table table) {
 *     super(spec.getName(), table);
 *   }
 *
 *   //...
 * }
 * </code>
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Beta
public @interface DatasetType {
  /**
   * Returns name of the dataset type.
   */
  String value();
}
