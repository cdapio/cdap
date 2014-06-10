/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.data.dataset.module;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare a dataset type from {@link com.continuuity.internal.data.dataset.Dataset}.
 * The value is used as dataset type name.
 *
 * This is used when creating {@link com.continuuity.internal.data.dataset.DatasetDefinition} from only
 * {@link com.continuuity.internal.data.dataset.Dataset} implementation.
 * See {@link com.continuuity.api.app.ApplicationConfigurer#addDatasetType(Class)} for more details.
 *
 * Example of usage:
 *
 * <pre>
 * {@code

 &#64;DatasetType("KVTable")
 public class SimpleKVTable extends AbstractDataset {
   public SimpleKVTable(DatasetSpecification spec, @EmbeddedDataset("data") Table table) {
     super(spec.getName(), table);
   }

   //...
 }

 * }
 * </pre>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DatasetType {
  /**
   * Returns name of the dataset type.
   */
  String value();
}
