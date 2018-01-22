package co.cask.cdap.api.metadata;

import java.util.Map;
import java.util.Set;
import javax.ws.rs.NotFoundException;

/**
 * MetadataContext
 */
public interface MetadataContext {
  /**
   * Adds the specified {@link Map} to the metadata of the specified {@link MetadataEntity metadataEntity}.
   * Existing keys are updated with new values, newer keys are appended to the metadata. This API only supports adding
   * properties in {@link MetadataScope#USER}.
   *
   * @throws NotFoundException if the specified entity was not found
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addProperties(MetadataEntity metadataEntity, Map<String, String> properties)
    throws NotFoundException, InvalidMetadataException;

  /**
   * Adds the specified tags to specified {@link MetadataEntity}. This API only supports adding tags in
   * {@link MetadataScope#USER}.
   *
   * @throws NotFoundException if the specified entity was not found
   * @throws InvalidMetadataException if some of the properties violate metadata validation rules
   */
  void addTags(MetadataEntity metadataEntity, String... tags)
    throws NotFoundException, InvalidMetadataException;

  /**
   * Returns a set of {@link MetadataRecord} representing all metadata (including properties and tags) for the specified
   * {@link MetadataEntity} in both {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  Set<MetadataRecord> getMetadata(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * Returns a set of {@link MetadataRecord} representing all metadata (including properties and tags) for the specified
   * {@link MetadataEntity} in the specified {@link MetadataScope}.
   *
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: Should this return a single metadata record instead or is a set of one record ok?
  Set<MetadataRecord> getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * @return a {@link Map} representing the metadata of the specified {@link MetadataEntity} in both
   * {@link MetadataScope#USER} and {@link MetadataScope#SYSTEM}
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: This should perhaps return a Map<MetadataScope, Map<String, String>>
  Map<String, String> getProperties(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * @return a {@link Map} representing the metadata of the specified {@link MetadataEntity} in the specified
   * {@link MetadataScope}
   * @throws NotFoundException if the specified entity was not found
   */
  Map<String, String> getProperties(MetadataScope scope, MetadataEntity metadataEntity)
    throws NotFoundException;

  /**
   * @return all the tags for the specified {@link MetadataEntity} in both {@link MetadataScope#USER} and
   * {@link MetadataScope#SYSTEM}
   * @throws NotFoundException if the specified entity was not found
   */
  // TODO: This should perhaps return a Map<MetadataScope, Set<String>>
  Set<String> getTags(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * @return all the tags for the specified {@link MetadataEntity} in the specified {@link MetadataScope}
   * @throws NotFoundException if the specified entity was not found
   */
  Set<String> getTags(MetadataScope scope, MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * Removes all the metadata (including properties and tags) for the specified {@link MetadataEntity}. This
   * API only supports removing metadata in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove metadata for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeMetadata(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * Removes all properties from the metadata of the specified {@link MetadataEntity}. This API only supports
   * removing properties in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove properties for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * Removes the specified keys from the metadata properties of the specified {@link MetadataEntity}. This API only
   * supports removing properties in {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified properties for
   * @param keys the metadata property keys to remove
   * @throws NotFoundException if the specified entity was not found
   */
  void removeProperties(MetadataEntity metadataEntity, String... keys) throws NotFoundException;

  /**
   * Removes all tags from the specified {@link MetadataEntity}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove tags for
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(MetadataEntity metadataEntity) throws NotFoundException;

  /**
   * Removes the specified tags from the specified {@link MetadataEntity}. This API only supports removing tags in
   * {@link MetadataScope#USER}.
   *
   * @param metadataEntity the {@link MetadataEntity} to remove the specified tags for
   * @param tags the tags to remove
   * @throws NotFoundException if the specified entity was not found
   */
  void removeTags(MetadataEntity metadataEntity, String... tags) throws NotFoundException;
}
