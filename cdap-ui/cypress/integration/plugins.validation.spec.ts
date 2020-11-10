import {
  getArtifactsPoll,
  getGenericEndpoint,
  loginIfRequired,
  setNewSchemaEditor,
} from '../helpers';
import {
  DEFAULT_BIGQUERY_DATASET,
  DEFAULT_BIGQUERY_TABLE,
  DEFAULT_GCP_PROJECTID,
  DEFAULT_GCP_SERVICEACCOUNT_PATH,
} from '../support/constants';
import { INodeIdentifier, INodeInfo } from '../typings';
import { dataCy } from '../helpers';

const sourceNode: INodeInfo = {
  nodeName: 'BigQueryTable',
  nodeType: 'batchsource',
};
const sourceNodeId: INodeIdentifier = {
  ...sourceNode,
  nodeId: '0',
};
const valueMapperNode: INodeInfo = {
  nodeName: 'ValueMapper',
  nodeType: 'transform',
};
const valueMapperNodeId: INodeIdentifier = {
  ...valueMapperNode,
  nodeId: '1',
};

const sourceProperties = {
  referenceName: 'BQ_Source',
  project: DEFAULT_GCP_PROJECTID,
  datasetProject: DEFAULT_GCP_PROJECTID,
  dataset: DEFAULT_BIGQUERY_DATASET,
  table: DEFAULT_BIGQUERY_TABLE,
  serviceFilePath: DEFAULT_GCP_SERVICEACCOUNT_PATH,
};

const ERROR_BOUNDARY_COLOR = 'rgb(164, 4, 3)';
const VALIDATE_BTN_SELECTOR = 'plugin-properties-validate-btn';
const WIDGET_WRAPPER_SELECTOR = 'widget-wrapper-container';
const REFERENCE_NAME_PROP_SELECTOR = 'referenceName';
const GET_SCHEMA_BTN_SELECTOR = 'get-schema-btn';

let headers = {};

describe('Plugin properties', () => {
  // Uses API call to login instead of logging in manually through UI
  before(() => {
    loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token').then((cookie) => {
        if (!cookie) {
          return;
        }
        headers = {
          Authorization: 'Bearer ' + cookie.value,
        };
      });
    });
    cy.visit('/pipelines/ns/default/studio');
  });

  beforeEach(() => {
    setNewSchemaEditor();
    getArtifactsPoll(headers);
  });
  afterEach(() => {
    cy.clearLocalStorage();
  });

  it('should be validated and errors found in properties', () => {
    cy.add_node_to_canvas(sourceNode);
    cy.open_node_property(sourceNodeId);

    // Testing for errors found
    cy.get(dataCy(VALIDATE_BTN_SELECTOR)).click();
    cy.get(dataCy('plugin-properties-errors-found')).should('have.text', '3 errors found.');
    cy.get(`${dataCy(REFERENCE_NAME_PROP_SELECTOR)} > ${dataCy(WIDGET_WRAPPER_SELECTOR)}`).should(
      'have.css',
      'border-color',
      ERROR_BOUNDARY_COLOR
    );
    // Testing if the widget is highlighted when there is an error
    // and the error text
    cy.get(`${dataCy(REFERENCE_NAME_PROP_SELECTOR)} ~ ${dataCy('property-row-error')}`).should(
      'have.text',
      "Required property 'referenceName' has no value."
    );
    cy.get(`${dataCy(REFERENCE_NAME_PROP_SELECTOR)} > ${dataCy(WIDGET_WRAPPER_SELECTOR)}`).should(
      'have.css',
      'border-color',
      ERROR_BOUNDARY_COLOR
    );
  });

  it('should be validated and no errors found in schema', () => {
    // configuring properties for the plugin
    cy.get(`input${dataCy(REFERENCE_NAME_PROP_SELECTOR)}`)
      .clear()
      .type(sourceProperties.referenceName);
    cy.get(`input${dataCy('project')}`)
      .clear()
      .type(sourceProperties.project);
    cy.get(`input${dataCy('datasetProject')}`)
      .clear()
      .type(sourceProperties.datasetProject);
    cy.get(`input${dataCy('dataset')}`)
      .clear()
      .type(sourceProperties.dataset);
    cy.get(`input${dataCy('table')}`)
      .clear()
      .type(sourceProperties.table);
    cy.get(`input${dataCy('serviceFilePath')}`)
      .clear()
      .type(sourceProperties.serviceFilePath);
    cy.get(`${dataCy(VALIDATE_BTN_SELECTOR)}`).click();
    cy.get(`${dataCy('plugin-validation-success-msg')}`).should('have.text', 'No errors found.');
  });

  it('should be validated and errors found in schema', () => {
    // retrieving schema
    cy.get(dataCy(GET_SCHEMA_BTN_SELECTOR)).click();
    cy.get(dataCy(GET_SCHEMA_BTN_SELECTOR)).contains('Get Schema');
    // Changing type of one of the fields in schema to timestamp and validating
    // This will cause validation to fail and an error would show up in schema
    // modal
    cy.get(`select${dataCy('schema-row-type-select-0')}`).select('string:timestamp');
    cy.get(dataCy(VALIDATE_BTN_SELECTOR)).click();
    cy.get(dataCy('schema-row-0')).should(
      'have.css',
      'border',
      `2px solid ${ERROR_BOUNDARY_COLOR}`
    );
    cy.get(dataCy('schema-row-error-0')).should(
      'contain',
      "Field 'name' of type 'timestamp in microseconds' has incompatible type with column 'name' in BigQuery table 'cdap_gcp_ui_test.users'"
    );
    // close the existing modal
    cy.get('[data-testid="close-config-popover"]').click();
  });

  it('should be validated and nested errors should be highlighted', () => {
    // properties to interact with
    const nested_property = 'defaults';
    const required_property = 'mapping';
    // adding value mapper node to canvas and connecting to source
    cy.open_transform_panel();
    cy.add_node_to_canvas(valueMapperNode);
    cy.connect_two_nodes(sourceNodeId, valueMapperNodeId, getGenericEndpoint);
    cy.open_node_property(valueMapperNodeId);

    // Filling property values - mapping field
    cy.get(dataCy(required_property)).should('exist');
    cy.get(`${dataCy(required_property)} ${dataCy(0)} ${dataCy('multiple-values-input-0')}`).type(
      'field1'
    );
    cy.get(`${dataCy(required_property)} ${dataCy(0)} ${dataCy('multiple-values-input-1')}`).type(
      'field2'
    );
    cy.get(`${dataCy(required_property)} ${dataCy(0)} ${dataCy('multiple-values-input-2')}`).type(
      'field3'
    );

    // Filling property values for nested property -defaults field - key value multirow widget
    cy.get(dataCy(nested_property)).should('exist');
    cy.get(`${dataCy(nested_property)} ${dataCy(0)} ${dataCy('key')}`).type('key1');
    cy.get(`${dataCy(nested_property)} ${dataCy(0)} ${dataCy('value')}`).type('value1');
    cy.get(`${dataCy(nested_property)} ${dataCy(0)} ${dataCy('add-row')}`).click();
    cy.get(`${dataCy(nested_property)}`).should('exist');
    cy.get(`${dataCy(nested_property)} ${dataCy(1)} ${dataCy('key')}`).type('key2');
    cy.get(`${dataCy(nested_property)} ${dataCy(1)} ${dataCy('value')}`).type('value2');
    // validating properties
    cy.get(dataCy(VALIDATE_BTN_SELECTOR)).click();
    cy.get(`${dataCy(required_property)} ${dataCy('error-text-0')}`).should(
      'have.text',
      "Map key 'field1' must be present in the input schema."
    );
    cy.get(`${dataCy(nested_property)} ${dataCy('error-text-0')}`).should(
      'have.text',
      "Defaults key 'key1' must be present as a source in mapping."
    );
    cy.get(`${dataCy(nested_property)} ${dataCy('error-text-1')}`).should(
      'have.text',
      "Defaults key 'key2' must be present as a source in mapping."
    );
  });
});
