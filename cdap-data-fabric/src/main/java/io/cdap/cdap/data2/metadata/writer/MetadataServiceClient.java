package io.cdap.cdap.data2.metadata.writer;

import io.cdap.cdap.spi.metadata.MetadataMutation;

/**
 * This interface exposes functionality for making Metadata HTTP calls to the Metadata Service.
 */
public interface MetadataServiceClient {

  /**
   * Publishes the create mutation to Metadata Service.
   *
   * @param createMutation Metadata's create mutation to apply
   */
  void create(MetadataMutation.Create createMutation);

  /**
   * Publishes the drop mutation to Metadata Service.
   *
   * @param dropMutation Metadata's drop mutation to apply
   */
  void drop(MetadataMutation.Drop dropMutation);

  /**
   * Publishes the update mutation to Metadata Service.
   *
   * @param updateMutation Metadata's update mutation to apply
   */
  void update(MetadataMutation.Update updateMutation);

  /**
   * Publishes the remove mutation to Metadata Service.
   *
   * @param removeMutation Metadata's remove mutation to apply
   */
  void remove(MetadataMutation.Remove removeMutation);
}
