/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.MetricSource;
import com.continuuity.overlord.metrics.client.RecordBuilder;
import com.continuuity.overlord.metrics.client.annotation.Metric;
import com.continuuity.overlord.metrics.client.counters.MutableCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * Implementation of {@link com.continuuity.overlord.metrics.client.MetricSource} interface. MetricSource currently needs
 * registration of classes that will require monitoring of client. It currently
 * uses introspection to determine the Counters that are part of the
 * class.
 */
public class MetricSourceImpl implements MetricSource {
  private static final Logger LOG = LoggerFactory.getLogger(MetricSourceImpl.class);
  private  Registry  registry;

  /**
   * Constructor registering a class.
   * @param source
   */
  public MetricSourceImpl(Object source) {
    String name = source.getClass().getSimpleName();
    register(name, source);
  }

  public MetricSourceImpl(String name, Object source) {
    register(name, source);
  }

  private void register(String name, Object source) {
    Class<?> cls = source.getClass();

    /**
     * We make sure that source does not have a registry already, but
     * for now we don't care.
     */
    registry = initRegistry(source);

    /**
     * iterate through all fields that are MutableCounter type and
     * add it to registry.
     */
    for (Field field : cls.getDeclaredFields()) {
      add(name, source, field);
    }
  }

  private void add(String name, Object source, Field field) {
    for (Annotation annotation : field.getAnnotations()) {
      if (!(annotation instanceof Metric)) continue;
      try {
        field.setAccessible(true);
        if (field.get(source) != null) continue;
        String metricName = name + "." + field.getName();
        Metric metricAnno = (Metric)annotation;
        MutableCounter mutable
            = registry.newCounter(metricName, metricAnno.description(), metricAnno.type(), 0);
        field.set(source, mutable);
      }
      catch (Exception e) {
        continue;
      }
    }
  }

  private Registry initRegistry(Object source) {
    Class<?> cls = source.getClass();
    Registry r = new Registry(cls.getSimpleName());
    return r;
  }
  
  @Override
  public void getMetrics(RecordBuilder builder) {
    registry.snapshot(builder);
  }

}
