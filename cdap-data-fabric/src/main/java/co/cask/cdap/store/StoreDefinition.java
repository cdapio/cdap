/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.store;

import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.spi.data.table.field.Fields;

import java.io.IOException;

/**
 * A class which contains all the store definition, the table name the store will use, the schema of the table should
 * all be specified here.
 * TODO: CDAP-14674 Make sure all the store definition goes here.
 */
public final class StoreDefinition {
  private StoreDefinition() {
    // prevent instantiation
  }

  /**
   * Create all system tables.
   *
   * @param tableAdmin the table admin to create the table
   */
  public static void createAllTables(StructuredTableAdmin tableAdmin, StructuredTableRegistry registry,
                                     boolean overWrite) throws IOException, TableAlreadyExistsException {
    registry.initialize();
    if (overWrite || tableAdmin.getSpecification(ArtifactStore.ARTIFACT_DATA_TABLE) == null) {
      ArtifactStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(OwnerStore.OWNER_TABLE) == null) {
      OwnerStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(NamespaceStore.NAMESPACES) == null) {
      NamespaceStore.createTable(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(SecretStore.SECRET_STORE_TABLE) == null) {
      SecretStore.createTable(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(WorkflowStore.WORKFLOW_STATISTICS) == null) {
      WorkflowStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(ConfigStore.CONFIGS) == null) {
      ConfigStore.createTable(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(ProvisionerStore.PROVISIONER_TABLE) == null) {
      ProvisionerStore.createTable(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(AppMetadataStore.APPLICATION_SPECIFICATIONS) == null) {
      AppMetadataStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(ProfileStore.PROFILE_STORE_TABLE) == null) {
      ProfileStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(ProgramScheduleStore.PROGRAM_TRIGGER_TABLE) == null) {
      ProgramScheduleStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(DatasetInstanceStore.DATASET_INSTANCES) == null) {
      DatasetInstanceStore.createTables(tableAdmin);
    }
    if (overWrite || tableAdmin.getSpecification(DatasetTypeStore.DATASET_TYPES) == null) {
      DatasetTypeStore.createTables(tableAdmin);
    }
  }

  public static void createAllTables(StructuredTableAdmin tableAdmin, StructuredTableRegistry registry)
    throws IOException, TableAlreadyExistsException {
    createAllTables(tableAdmin, registry, false);
  }

  /**
   * Namespace store schema
   */
  public static final class NamespaceStore {
    public static final StructuredTableId NAMESPACES = new StructuredTableId("namespaces");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String NAMESPACE_METADATA_FIELD = "namespace_metadata";

    public static final StructuredTableSpecification NAMESPACE_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(NAMESPACES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(NAMESPACE_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD)
        .build();

    public static void createTable(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(NAMESPACE_TABLE_SPEC);
    }
  }

  /**
   * Schema for ConfigStore
   */
  public static final class ConfigStore {
    public static final StructuredTableId CONFIGS = new StructuredTableId("configs");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TYPE_FIELD = "type";
    public static final String NAME_FIELD = "name";
    public static final String PROPERTIES_FIELD = "properties";

    public static final StructuredTableSpecification CONFIG_TABLE_SPEC = new StructuredTableSpecification.Builder()
      .withId(CONFIGS)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(TYPE_FIELD),
                  Fields.stringType(NAME_FIELD),
                  Fields.stringType(PROPERTIES_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, TYPE_FIELD, NAME_FIELD)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(CONFIG_TABLE_SPEC);
    }
  }

  /**
   * Schema for workflow table
   */
  public static final class WorkflowStore {
    public static final StructuredTableId WORKFLOW_STATISTICS = new StructuredTableId("workflow_statistics");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_FIELD = "program";
    public static final String START_TIME_FIELD = "start_time";
    public static final String RUN_ID_FIELD = "run_id";
    public static final String TIME_TAKEN_FIELD = "time_taken";
    public static final String PROGRAM_RUN_DATA = "program_run_data";

    public static final StructuredTableSpecification WORKFLOW_TABLE_SPEC = new StructuredTableSpecification.Builder()
      .withId(WORKFLOW_STATISTICS)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(APPLICATION_FIELD),
                  Fields.stringType(VERSION_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.longType(START_TIME_FIELD),
                  Fields.stringType(RUN_ID_FIELD),
                  Fields.longType(TIME_TAKEN_FIELD),
                  Fields.stringType(PROGRAM_RUN_DATA))
      .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_FIELD, START_TIME_FIELD)
      .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(WORKFLOW_TABLE_SPEC);
    }
  }

  /**
   *
   */
  public static final class ArtifactStore {
    public static final StructuredTableId ARTIFACT_DATA_TABLE = new StructuredTableId("artifact_data");
    public static final StructuredTableId APP_DATA_TABLE = new StructuredTableId("app_data");
    public static final StructuredTableId PLUGIN_DATA_TABLE = new StructuredTableId("plugin_data");
    public static final StructuredTableId UNIV_PLUGIN_DATA_TABLE = new StructuredTableId("universal_plugin_data");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String ARTIFACT_NAMESPACE_FIELD = "artifact_namespace";
    public static final String ARTIFACT_NAME_FIELD = "artifact_name";
    public static final String ARTIFACT_VER_FIELD = "artifiact_version";
    public static final String ARTIFACT_DATA_FIELD = "artifact_data";
    public static final String CLASS_NAME_FIELD = "class_name";
    public static final String APP_DATA_FIELD = "app_data";
    public static final String PARENT_NAMESPACE_FIELD = "parent_namespace";
    public static final String PARENT_NAME_FIELD = "parent_name";
    public static final String PLUGIN_TYPE_FIELD = "plugin_type";
    public static final String PLUGIN_NAME_FIELD = "plugin_name";
    public static final String PLUGIN_DATA_FIELD = "plugin_data";

    // Artifact Data table
    public static final StructuredTableSpecification ARTIFACT_DATA_SPEC = new StructuredTableSpecification.Builder()
      .withId(ARTIFACT_DATA_TABLE)
      .withFields(Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                  Fields.stringType(ARTIFACT_NAME_FIELD),
                  Fields.stringType(ARTIFACT_VER_FIELD),
                  Fields.stringType(ARTIFACT_DATA_FIELD))
      .withPrimaryKeys(ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
      .build();

    // App Data table
    public static final StructuredTableSpecification APP_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(APP_DATA_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(CLASS_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(APP_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, CLASS_NAME_FIELD, ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD,
                         ARTIFACT_VER_FIELD)
        .build();

    // Plugin Data table
    public static final StructuredTableSpecification PLUGIN_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PLUGIN_DATA_TABLE)
        .withFields(Fields.stringType(PARENT_NAMESPACE_FIELD),
                    Fields.stringType(PARENT_NAME_FIELD),
                    Fields.stringType(PLUGIN_TYPE_FIELD),
                    Fields.stringType(PLUGIN_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(PLUGIN_DATA_FIELD))
        .withPrimaryKeys(PARENT_NAMESPACE_FIELD, PARENT_NAME_FIELD, PLUGIN_TYPE_FIELD, PLUGIN_NAME_FIELD,
                         ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
        .build();

    // Universal Plugin Data table
    public static final StructuredTableSpecification UNIV_PLUGIN_DATA_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(UNIV_PLUGIN_DATA_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(PLUGIN_TYPE_FIELD),
                    Fields.stringType(PLUGIN_NAME_FIELD),
                    Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                    Fields.stringType(ARTIFACT_NAME_FIELD),
                    Fields.stringType(ARTIFACT_VER_FIELD),
                    Fields.stringType(PLUGIN_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, PLUGIN_TYPE_FIELD, PLUGIN_NAME_FIELD,
                         ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(ARTIFACT_DATA_SPEC);
      tableAdmin.create(APP_DATA_SPEC);
      tableAdmin.create(PLUGIN_DATA_SPEC);
      tableAdmin.create(UNIV_PLUGIN_DATA_SPEC);
    }
  }

  /**
   * Table specification and create table definitions for owner store.
   */
  public static final class OwnerStore {
    public static final StructuredTableId OWNER_TABLE = new StructuredTableId("owner_data");
    public static final String PRINCIPAL_FIELD = "principal";
    public static final String KEYTAB_FIELD = "keytab";

    public static final StructuredTableSpecification OWNER_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(OWNER_TABLE)
        .withFields(Fields.stringType(PRINCIPAL_FIELD),
                    Fields.bytesType(KEYTAB_FIELD))
        .withPrimaryKeys(PRINCIPAL_FIELD).build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(OWNER_TABLE_SPEC);
    }
  }
  /**
   * Schema for {@link SecretStore}.
   */
  public static final class SecretStore {

    public static final StructuredTableId SECRET_STORE_TABLE = new StructuredTableId("secret_store");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String SECRET_NAME_FIELD = "secret_name";
    public static final String SECRET_DATA_FIELD = "secret_data";

    public static final StructuredTableSpecification SECRET_STORE_SPEC = new StructuredTableSpecification.Builder()
      .withId(SECRET_STORE_TABLE)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(SECRET_NAME_FIELD),
                  Fields.bytesType(SECRET_DATA_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, SECRET_NAME_FIELD)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(SECRET_STORE_SPEC);
    }
  }

  /**
   * Table specification and create table definitions for provisioner store.
   */
  public static final class ProvisionerStore {
    public static final StructuredTableId PROVISIONER_TABLE = new StructuredTableId("provisioner_data");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String KEY_TYPE = "type";
    public static final String PROVISIONER_TASK_INFO_FIELD = "provisioner_task_info";

    public static final StructuredTableSpecification PROVISIONER_STORE_SPEC = new StructuredTableSpecification.Builder()
      .withId(PROVISIONER_TABLE)
      .withFields(Fields.stringType(NAMESPACE_FIELD),
                  Fields.stringType(APPLICATION_FIELD),
                  Fields.stringType(VERSION_FIELD),
                  Fields.stringType(PROGRAM_TYPE_FIELD),
                  Fields.stringType(PROGRAM_FIELD),
                  Fields.stringType(RUN_FIELD),
                  Fields.stringType(KEY_TYPE),
                  Fields.stringType(PROVISIONER_TASK_INFO_FIELD))
      .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD,
                       PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD, KEY_TYPE)
      .build();

    public static void createTable(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(PROVISIONER_STORE_SPEC);
    }
  }
  /**
   *  Defines schema for AppMetadata tables
   */
  public static final class AppMetadataStore {

    public static final StructuredTableId APPLICATION_SPECIFICATIONS = new StructuredTableId("application_specs");
    public static final StructuredTableId WORKFLOW_NODE_STATES = new StructuredTableId("workflow_node_states");
    public static final StructuredTableId RUN_RECORDS = new StructuredTableId("run_records");
    public static final StructuredTableId WORKFLOWS = new StructuredTableId("workflows");
    public static final StructuredTableId PROGRAM_COUNTS = new StructuredTableId("program_counts");
    // TODO: CDAP-14876 Move this table into it's own store, along with associated methods
    public static final StructuredTableId SUBSCRIBER_STATES = new StructuredTableId("subscriber_state");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String APPLICATION_DATA_FIELD = "application_data";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String NODE_ID = "node_id";
    public static final String NODE_STATE_DATA = "node_state_data";
    public static final String RUN_STATUS = "run_status";
    public static final String RUN_START_TIME = "run_start_time";
    public static final String RUN_RECORD_DATA = "run_record_data";
    public static final String WORKFLOW_DATA = "workflow_data";
    public static final String COUNT_TYPE = "count_type";
    public static final String COUNTS = "counts";
    public static final String SUBSCRIBER_TOPIC = "subscriber_topic";
    public static final String SUBSCRIBER_MESSAGE = "subscriber_message";
    public static final String SUBSCRIBER = "subscriber";


    public static final StructuredTableSpecification APPLICATION_SPECIFICATIONS_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(APPLICATION_SPECIFICATIONS)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(APPLICATION_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD)
        .build();

    public static final StructuredTableSpecification WORKFLOW_NODE_STATES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(WORKFLOW_NODE_STATES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.stringType(RUN_FIELD),
                    Fields.stringType(NODE_ID),
                    Fields.stringType(NODE_STATE_DATA))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD,
                         RUN_FIELD, NODE_ID)
        .build();

    public static final StructuredTableSpecification RUN_RECORDS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(RUN_RECORDS)
        .withFields(Fields.stringType(RUN_STATUS),
                    Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.longType(RUN_START_TIME),
                    Fields.stringType(RUN_FIELD),
                    Fields.stringType(RUN_RECORD_DATA))
        .withPrimaryKeys(RUN_STATUS, NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD,
                         PROGRAM_FIELD, RUN_START_TIME, RUN_FIELD)
        .build();

    public static final StructuredTableSpecification WORKFLOWS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(WORKFLOWS)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.stringType(RUN_FIELD),
                    Fields.stringType(WORKFLOW_DATA))
        .withPrimaryKeys(
          NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD)
        .build();

    public static final StructuredTableSpecification PROGRAM_COUNTS_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROGRAM_COUNTS)
        .withFields(Fields.stringType(COUNT_TYPE),
                    Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(PROGRAM_TYPE_FIELD),
                    Fields.stringType(PROGRAM_FIELD),
                    Fields.longType(COUNTS))
        .withPrimaryKeys(
          COUNT_TYPE, NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD)
        .build();

    public static final StructuredTableSpecification SUBSCRIBER_STATE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(SUBSCRIBER_STATES)
        .withFields(Fields.stringType(SUBSCRIBER_TOPIC),
                    Fields.stringType(SUBSCRIBER),
                    Fields.stringType(SUBSCRIBER_MESSAGE))
        .withPrimaryKeys(SUBSCRIBER_TOPIC, SUBSCRIBER)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(APPLICATION_SPECIFICATIONS_TABLE_SPEC);
      tableAdmin.create(WORKFLOW_NODE_STATES_SPEC);
      tableAdmin.create(RUN_RECORDS_SPEC);
      tableAdmin.create(WORKFLOWS_SPEC);
      tableAdmin.create(PROGRAM_COUNTS_SPEC);
      tableAdmin.create(SUBSCRIBER_STATE_SPEC);
    }
  }

  /**
   * Dataset instance store schema
   */
  public static final class DatasetInstanceStore {

    public static final StructuredTableId DATASET_INSTANCES =
      new StructuredTableId("dataset_instances");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String DATASET_FIELD = "dataset";
    public static final String DATASET_METADATA_FIELD = "dataset_metadata";

    public static final StructuredTableSpecification DATASET_INSTANCES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(DATASET_INSTANCES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(DATASET_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, DATASET_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(DATASET_INSTANCES_SPEC);
    }
  }

  /**
   * Table schema for profile store.
   */
  public static final class ProfileStore {

    public static final StructuredTableId PROFILE_STORE_TABLE =
      new StructuredTableId("profile_store");
    public static final StructuredTableId PROFILE_ENTITY_STORE_TABLE =
      new StructuredTableId("profile_entity_store");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String PROFILE_ID_FIELD = "profile_id";
    public static final String ENTITY_ID_FIELD = "entity_id";
    public static final String PROFILE_DATA_FIELD = "profile_data";
    public static final String ENTITY_DATA_FIELD = "entity_data";

    public static final StructuredTableSpecification PROFILE_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROFILE_STORE_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(PROFILE_ID_FIELD),
                    Fields.stringType(PROFILE_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, PROFILE_ID_FIELD)
        .build();

    public static final StructuredTableSpecification PROFILE_ENTITY_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROFILE_ENTITY_STORE_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(PROFILE_ID_FIELD),
                    Fields.stringType(ENTITY_ID_FIELD),
                    Fields.stringType(ENTITY_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, PROFILE_ID_FIELD, ENTITY_ID_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(PROFILE_STORE_SPEC);
      tableAdmin.create(PROFILE_ENTITY_STORE_SPEC);
    }
  }

  /**
   * Table schema for program schedule store.
   */
  public static final class ProgramScheduleStore {

    public static final StructuredTableId PROGRAM_SCHEDULE_TABLE =
      new StructuredTableId("program_schedule_store");
    public static final StructuredTableId PROGRAM_TRIGGER_TABLE =
      new StructuredTableId("program_trigger_store");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String SCHEDULE_NAME = "schedule_name";
    public static final String SEQUENCE_ID = "sequence_id";
    public static final String SCHEDULE = "schedule";
    public static final String UPDATE_TIME = "update_time";
    public static final String STATUS = "status";
    public static final String TRIGGER_KEY = "trigger_key";


    public static final StructuredTableSpecification PROGRAM_SCHEDULE_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROGRAM_SCHEDULE_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(SCHEDULE_NAME),
                    Fields.stringType(SCHEDULE),
                    Fields.longType(UPDATE_TIME),
                    Fields.stringType(STATUS))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, SCHEDULE_NAME)
        .build();

    public static final StructuredTableSpecification PROGRAM_TRIGGER_STORE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(PROGRAM_TRIGGER_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(APPLICATION_FIELD),
                    Fields.stringType(VERSION_FIELD),
                    Fields.stringType(SCHEDULE_NAME),
                    Fields.intType(SEQUENCE_ID),
                    Fields.stringType(TRIGGER_KEY))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, SCHEDULE_NAME, SEQUENCE_ID)
        .withIndexes(TRIGGER_KEY)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(PROGRAM_SCHEDULE_STORE_SPEC);
      tableAdmin.create(PROGRAM_TRIGGER_STORE_SPEC);
    }
  }

  /**
   * Dataset type store schema
   */
  public static final class DatasetTypeStore {

    public static final StructuredTableId DATASET_TYPES = new StructuredTableId("dataset_types");
    public static final StructuredTableId MODULE_TYPES = new StructuredTableId("module_types");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String MODULE_NAME_FIELD = "module_name";
    public static final String TYPE_NAME_FIELD = "type_name";
    public static final String DATASET_METADATA_FIELD = "dataset_metadata";

    public static final StructuredTableSpecification DATASET_TYPES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(DATASET_TYPES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(TYPE_NAME_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, TYPE_NAME_FIELD)
        .build();
    public static final StructuredTableSpecification MODULE_TYPES_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(MODULE_TYPES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(MODULE_NAME_FIELD),
                    Fields.stringType(DATASET_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, MODULE_NAME_FIELD)
        .build();

    public static void createTables(StructuredTableAdmin tableAdmin) throws IOException, TableAlreadyExistsException {
      tableAdmin.create(DATASET_TYPES_SPEC);
      tableAdmin.create(MODULE_TYPES_SPEC);
    }
  }
}
