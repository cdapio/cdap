/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.data.dataset.module;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to declare a parameter to be used in {@link com.continuuity.internal.data.dataset.Dataset} constructor
 * which will be injected with {@link com.continuuity.internal.data.dataset.Dataset} instance.
 *
 * This is used when creating {@link com.continuuity.internal.data.dataset.DatasetDefinition} from only
 * {@link com.continuuity.internal.data.dataset.Dataset} implementation.
 * See {@link com.continuuity.api.app.ApplicationConfigurer#addDatasetType(Class)} for more details.
 *
 * Example of usage:
 *
 * <pre>
 * {@code

  public class SimpleKVTable extends AbstractDataset {
    public SimpleKVTable(DatasetSpecification spec, @EmbeddedDataset("data") Table table) {
      super(spec.getName(), table);
    }

    //...
  }

 * }
 * </pre>
 *
 * Here, upon creation the table parameter will be a dataset that points to a embedded dataset of name "data"
 * (namespaced with this dataset name).
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface EmbeddedDataset {

  static final String DEFAULT_TYPE_NAME = "";

  /**
   * Returns name of the dataset.
   */
  String value();

  /**
   * Optionally returns name of the type of the underlying dataset. If not set, then type of the parameter will be used
   * to resolve it
   */
  String type() default DEFAULT_TYPE_NAME;
}
