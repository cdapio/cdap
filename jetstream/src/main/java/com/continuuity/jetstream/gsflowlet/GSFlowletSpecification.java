package com.continuuity.jetstream.gsflowlet;

import com.continuuity.jetstream.api.GSSchema;

import java.util.Map;

/**
 * Specification of a GSFlowlet.
 */
public interface GSFlowletSpecification {

  String getName();

  String getDescription();

  Map<String, GSSchema> getGdatInputSchema();

  Map<String, String> getGSQL();

}
