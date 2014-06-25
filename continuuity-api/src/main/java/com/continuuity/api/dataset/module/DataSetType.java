/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.dataset.module;

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
 * {@link com.continuuity.api.app.ApplicationConfigurer#addDataSetType
 * com.continuuity.api.app.ApplicationConfigurer#addDataSetType(Class &lt;&#63; extends Dataset&gt; datasetClass)
 * }
 * for details.
 *
 * Example of usage:
 *
 * <pre>
 * <code>
 * {@literal @}DataSetType("KVTable")
 * public class SimpleKVTable extends AbstractDataset {
 *   public SimpleKVTable(DatasetSpecification spec, {@literal @}EmbeddedDataSet("data") Table table) {
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
public @interface DataSetType {
  /**
   * Returns name of the dataset type.
   */
  String value();
}
