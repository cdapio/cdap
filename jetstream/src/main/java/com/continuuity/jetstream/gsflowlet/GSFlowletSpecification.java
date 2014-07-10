package com.continuuity.jetstream.gsflowlet;

import com.continuuity.jetstream.api.GSSchema;

import java.util.Map;

/**
 *
 */
public interface GSFlowletSpecification {

  String getName();

  String getDescription();

  Map<String, GSSchema> getInputSchema();

  Map<String, String> getGSQL();
}
