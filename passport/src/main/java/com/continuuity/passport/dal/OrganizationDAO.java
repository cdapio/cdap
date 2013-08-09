package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.OrganizationAlreadyExistsException;
import com.continuuity.passport.core.exceptions.OrganizationNotFoundException;
import com.continuuity.passport.meta.Organization;

/**
 * CRUD operations for organization.
 */
public interface OrganizationDAO {

  /**
   * Create an Organization in the system.
   * @param id id of the organization.
   * @param name name of the organization.
   * @return instance of {@code Organization}
   * @throws OrganizationAlreadyExistsException when Organization to be created already exists in the system.
   */
  public Organization createOrganization(String id, String name) throws OrganizationAlreadyExistsException;

  /**
   * Read Organization based on id.
   * @param id Id of the organization.
   * @return instance of {@code Organization}
   * @throws OrganizationNotFoundException when Organization to be updated doesn't exists.
   */
  public Organization getOrganization(String id) throws OrganizationNotFoundException;

  /**
   * Update the organization name.
   * @param id id of the organization.
   * @param name name of the organization.
   * @return instance of updated {@code Organization}.
   * @throws OrganizationNotFoundException when Organization to be updated doesn't exists.
   */
  public Organization updateOrganization(String id, String name) throws OrganizationNotFoundException;

  /**
   * Delete the organization.
   * @param id id of the organization.
   * @throws OrganizationNotFoundException when Organization to be fetched doesn't exist.
   */
  public void deleteOrganization(String id) throws OrganizationNotFoundException;
}
