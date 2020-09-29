/*
 * Copyright © 2020 Cask Data, Inc.
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

import * as Helpers from '../helpers';
let headers = {};

const { dataCy } = Helpers;

const MOCK_PLUGIN_INFO = {
  pluginName: 'Database',
  displayName: 'Database',
  pluginType: 'batchsource',
};

const MOCK_CONFIGURATION_GROUPS: any[] = [
  {
    label: 'new group label',
    properties: [
      {
        'widget-type': 'number',
        label: 'new widget label',
        name: 'new widget name',
        'widget-category': 'plugin',
        'widget-attributes': {
          min: '0',
          max: '1',
          default: '1',
        },
      },
      {
        'widget-type': 'textarea',
        label: 'new new widget label',
        name: 'new new widget name',
        'widget-category': 'plugin',
        'widget-attributes': {
          default: 'Hello',
          rows: '20',
        },
      },
    ],
  },
];

const MOCK_EXPLICIT_OUTPUT = {
  name: 'schema',
  'widget-type': 'schema',
  'widget-attributes': {
    'schema-default-type': 'string',
    'schema-types': ['boolean', 'bytes', 'string', 'int', 'long', 'float', 'double'],
  },
};

const MOCK_IMPLICIT_OUTPUT = {
  'widget-type': 'non-editable-schema-editor',
  schema: {
    name: 'etlSchemaBody',
    type: 'record',
    fields: [
      {
        name: 'key',
        type: 'bytes',
      },
      {
        name: 'value',
        type: 'bytes',
      },
    ],
  },
};

const MOCK_FILTERS: any[] = [
  {
    name: 'Proxy authentication',
    condition: {
      property: MOCK_CONFIGURATION_GROUPS[0].properties[0].name,
      operator: 'exists',
    },
    show: [
      {
        name: MOCK_CONFIGURATION_GROUPS[0].properties[1].name,
        type: 'property',
      },
    ],
  },
];

function checkImportedConfigurationGroups(mockConfigurationGroups) {
  mockConfigurationGroups.forEach((group, groupIndex) => {
    const groupSelector = dataCy(`configuration-group-panel-${groupIndex}`);

    cy.get(dataCy(`open-configuration-group-panel-${groupIndex}`)).click();

    cy.get(`${groupSelector} ${dataCy('label')}`)
      .invoke('val')
      .then((val) => {
        expect(val).equals(group.label);
      });

    group.properties.forEach((widget, widgetIndex) => {
      const widgetSelector = `${groupSelector} ${dataCy(`widget-panel-${widgetIndex}`)}`;

      cy.get(`${widgetSelector} ${dataCy('name')}`)
        .invoke('val')
        .then((val) => {
          if (widget.name) {
            expect(val).equals(widget.name);
          }
        });

      cy.get(`${widgetSelector} ${dataCy('label')}`)
        .invoke('val')
        .then((val) => {
          if (widget.label) {
            expect(val).equals(widget.label);
          }
        });

      cy.get(`${widgetSelector} ${dataCy('category')}`)
        .invoke('val')
        .then((val) => {
          if (widget['widget-category']) {
            expect(val).equals(widget['widget-category']);
          }
        });

      cy.get(`${widgetSelector} ${dataCy('widget-type')}`)
        .invoke('val')
        .then((val) => {
          if (widget['widget-type']) {
            expect(val).equals(widget['widget-type']);
          }
        });
    });
  });
}

function checkImportedOutputs(mockOutput) {
  if (mockOutput['widget-type'] === 'schema') {
    cy.get(dataCy('output-name'))
      .invoke('val')
      .then((val) => {
        expect(val).equals(mockOutput.name);
      });
    cy.get(dataCy('schema-types'))
      .invoke('val')
      .then((val) => {
        expect(val).equals(mockOutput['widget-attributes']['schema-types'].join(','));
      });
    cy.get(dataCy('schema-default-type'))
      .invoke('val')
      .then((val) => {
        expect(val).equals(mockOutput['widget-attributes']['schema-default-type']);
      });
  } else if (mockOutput['widget-type'] === 'non-editable-schema-editor') {
    cy.get(dataCy('schema'))
      .invoke('val')
      .then((val) => {
        expect(val).equals(mockOutput.schema);
      });
  }
}

function checkImportedFilters(mockFilters) {
  if (mockFilters) {
    mockFilters.forEach((filter, filterIndex) => {
      cy.get(dataCy(`open-filter-panel-${filterIndex}`)).click();

      const filterSelector = dataCy(`filter-panel-${filterIndex}`);

      cy.get(filterSelector).should('exist');

      cy.get(`${filterSelector} ${dataCy('filter-name')}`)
        .invoke('val')
        .then((val) => {
          expect(val).equals(filter.name);
        });

      if (filter.condition) {
        if (filter.condition.expression) {
          cy.get(`${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('expression')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(filter.condition.expression);
            });
        } else {
          cy.get(`${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('property')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(filter.condition.property);
            });
          cy.get(`${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('operator')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(filter.condition.operator);
            });
          if (filter.value) {
            cy.get(`${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('expression')}`)
              .invoke('val')
              .then((val) => {
                expect(val).equals(filter.condition.value);
              });
          }
        }
      }

      if (filter.show) {
        filter.show.map((showVal, showIndex) => {
          cy.get(
            `${filterSelector} ${dataCy('filter-showlist-input')} ${dataCy(
              `show-${showIndex}`
            )} ${dataCy('name')}`
          )
            .invoke('val')
            .then((val) => {
              expect(val).equals(showVal.name);
            });

          cy.get(
            `${filterSelector} ${dataCy('filter-showlist-input')} ${dataCy(
              `show-${showIndex}`
            )} ${dataCy('type')}`
          )
            .invoke('val')
            .then((val) => {
              expect(val).equals(showVal.type);
            });
        });
      }
    });
  }
}

describe('Plugin Information Page', () => {
  // Uses API call to login instead of logging in manually through UI
  before(() => {
    Helpers.loginIfRequired().then(() => {
      cy.getCookie('CDAP_Auth_Token').then((cookie) => {
        if (!cookie) {
          return;
        }
        headers = {
          Authorization: 'Bearer ' + cookie.value,
        };
      });
    });
    const stub = cy.stub();
    cy.window().then((win) => {
      win.onbeforeunload = null;
    });
    cy.on('window:confirm', stub);

    Helpers.getArtifactsPoll(headers);
  });

  describe('Editing data', () => {
    before(() => {
      cy.visit('/cdap/ns/default/plugincreation');
    });

    it('should edit PluginInfoPage', () => {
      // Fill out plugin name
      cy.get(dataCy('plugin-name'))
        .click()
        .type(MOCK_PLUGIN_INFO.pluginName);

      // Fill out plugin type
      cy.get(dataCy('select-plugin-type')).click();
      cy.get(dataCy(`option-${MOCK_PLUGIN_INFO.pluginType}`)).click();

      // Fill out display name
      cy.get(dataCy('display-name'))
        .click()
        .type(MOCK_PLUGIN_INFO.displayName);

      // Validate whether input has been filled
      cy.get(dataCy('plugin-name'))
        .invoke('val')
        .then((val) => {
          expect(val).equals(MOCK_PLUGIN_INFO.pluginName);
        });
      cy.get(dataCy('display-name'))
        .invoke('val')
        .then((val) => {
          expect(val).equals(MOCK_PLUGIN_INFO.displayName);
        });

      // Check whether live JSON output now includes displayName.
      cy.get(dataCy('live-json')).contains(`"display-name": "${MOCK_PLUGIN_INFO.displayName}",`);
      // Check whether JSON filename is now ${pluginName}-${pluginType}.json.
      cy.get(dataCy('plugin-json-filename')).contains(
        `${MOCK_PLUGIN_INFO.pluginName}-${MOCK_PLUGIN_INFO.pluginType}.json`
      );

      // Button that moves to the next page is now enabled,
      // since all the required fields are filled.
      cy.get(dataCy('next-step-btn')).should('not.be.disabled');

      // Move to the next page
      cy.get(dataCy('next-step-btn')).click();
    });

    it('should edit ConfigurationGroupsPage', () => {
      MOCK_CONFIGURATION_GROUPS.forEach((group, groupIndex) => {
        const groupSelector = dataCy(`configuration-group-panel-${groupIndex}`);

        // Add a new configuration group
        cy.get(dataCy('add-configuration-group-btn')).click();
        cy.get(groupSelector).should('exist');

        // Edit configuration group label
        const groupLabel = group.label;
        cy.get(`${groupSelector} ${dataCy('label')}`)
          .click()
          .type(groupLabel);

        group.properties.forEach((widget, widgetIndex) => {
          // Add a new widget under the new configuration group
          const widgetSelector = dataCy(`widget-panel-${widgetIndex}`);
          if (widgetIndex === 0) {
            cy.get(dataCy('add-properties-btn')).click(); // this adds the first widget
          } else {
            const previousWidgetSelector = dataCy(`widget-panel-${widgetIndex - 1}`);
            cy.get(`${previousWidgetSelector} ${dataCy('add-widget-btn')}`).click(); // this adds another widget
          }

          // Edit a basic information of the existing widget
          cy.get(`${widgetSelector} ${dataCy('name')}`)
            .click()
            .type(widget.name);
          cy.get(`${widgetSelector} ${dataCy('label')}`)
            .click()
            .type(widget.label);
          cy.get(`${widgetSelector} ${dataCy('select-category')}`).click();
          cy.get(dataCy(`option-${widget['widget-category']}`)).click();
          cy.get(`${widgetSelector} ${dataCy('select-widget-type')}`).click();
          cy.get(dataCy(`option-${widget['widget-type']}`)).click();

          // Open a widget-attributes dialog
          cy.get(`${widgetSelector} ${dataCy('open-widget-attributes')}`).click();
          cy.get(dataCy('widget-attributes-dialog')).should('exist');

          // Check whether widget has been created
          cy.get(widgetSelector).should('exist');

          // Check whether the dialog contains widget input with previously set values
          cy.get(`${dataCy('widget-attributes-dialog')} ${dataCy('name')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(widget.name);
            });
          cy.get(`${dataCy('widget-attributes-dialog')} ${dataCy('label')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(widget.label);
            });
          cy.get(`${dataCy('widget-attributes-dialog')} ${dataCy('category')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(widget['widget-category']);
            });
          cy.get(`${dataCy('widget-attributes-dialog')} ${dataCy('widget-type')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(widget['widget-type']);
            });

          // Edits widget-attributes of the existing widget.
          Object.entries(widget['widget-attributes']).map(([key, value]) => {
            cy.get(
              `${dataCy('widget-attributes-dialog')} ${dataCy('widget-attributes-inputs')} ${dataCy(
                key
              )}`
            )
              .click()
              .type(value as string);
          });

          // Save the widget attributes
          cy.get(
            `${dataCy('widget-attributes-dialog')} ${dataCy('save-widget-attributes-btn')}`
          ).click();
        });
      });

      // Compare the live JSON code with our mock data
      cy.get(dataCy('live-json')).should((jsonContent) => {
        const obj = JSON.parse(jsonContent.text());
        const groups = obj['configuration-groups'];

        expect(groups).to.deep.equal(MOCK_CONFIGURATION_GROUPS);
      });

      // Move to the next page
      cy.get(dataCy('next-step-btn')).click();
    });

    it('should edit OutputPage', () => {
      // Switch to explicit output schema
      cy.get('[type="radio"]')
        .eq(0)
        .check({ force: true });
      cy.get(dataCy('explicit-schema-definer')).should('exist');
      cy.get(dataCy('implicit-schema-definer')).should('not.exist');

      // Fill out output name
      cy.get(dataCy('output-name'))
        .click()
        .type(MOCK_EXPLICIT_OUTPUT.name);

      // Fill out schema types
      const newSchemaTypes = MOCK_EXPLICIT_OUTPUT['widget-attributes']['schema-types'];
      cy.get(dataCy('multiselect-schema-types')).click();
      newSchemaTypes.forEach((type) => {
        cy.get(dataCy(`multioption-${type}`)).scrollIntoView();
        cy.get(dataCy(`multioption-${type}`)).click();
      });
      cy.get('body').type('{esc}', { release: true }); // close the multiselect

      // Fill out schema default type
      const newSchemaDefaultType = MOCK_EXPLICIT_OUTPUT['widget-attributes']['schema-default-type'];
      cy.get(dataCy('select-schema-default-type')).click();
      cy.get(dataCy(`option-${newSchemaDefaultType}`)).scrollIntoView();
      cy.get(dataCy(`option-${newSchemaDefaultType}`)).click();

      // Compare the live JSON code with our mock data
      cy.get(dataCy('live-json')).should((jsonContent) => {
        const obj = JSON.parse(jsonContent.text());
        const output = obj.outputs[0];

        expect(output).to.deep.equal(MOCK_EXPLICIT_OUTPUT);
      });

      // Move to the next page
      cy.get(dataCy('next-step-btn')).click();
    });

    it('should edit FilterPage', () => {
      MOCK_FILTERS.forEach((filter, filterIndex) => {
        // Add a new filter
        const filterSelector = dataCy(`filter-panel-${filterIndex}`);
        if (filterIndex === 0) {
          cy.get(dataCy('add-filter-btn')).click(); // this adds the first filter
        } else {
          const previousFilterSelector = dataCy(`filter-panel-${filterIndex - 1}`);
          cy.get(`${previousFilterSelector} ${dataCy('add-filter-btn')}`).click(); // this adds another filter
        }
        cy.get(filterSelector).should('exist');

        // Fill out filter name
        cy.get(`${filterSelector} ${dataCy('filter-name')}`)
          .click()
          .type(filter.name);

        // Fill out filter condition
        cy.get(
          `${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('select-property')}`
        ).click();
        cy.get(dataCy(`option-${filter.condition.property}`)).click();

        cy.get(
          `${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('select-operator')}`
        ).click();
        cy.get(dataCy(`option-${filter.condition.operator}`)).click();

        // Fill out filter's show list
        filter.show.forEach((show, showIndex) => {
          cy.get(
            `${filterSelector} ${dataCy('filter-showlist-input')} ${dataCy(
              `show-${showIndex}`
            )} ${dataCy('select-name')}`
          ).click();
          cy.get(dataCy(`option-${show.name}`)).click();

          cy.get(
            `${filterSelector} ${dataCy('filter-showlist-input')} ${dataCy(
              `show-${showIndex}`
            )} ${dataCy('select-type')}`
          ).click();
          cy.get(dataCy(`option-${show.type}`)).click();
        });

        // Validate whether input has been filled
        cy.get(`${filterSelector} ${dataCy('filter-name')}`)
          .invoke('val')
          .then((val) => {
            expect(val).equals(filter.name);
          });
        if (filter.condition.expression) {
          cy.get(`${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('expression')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(filter.condition.expression);
            });
        } else {
          cy.get(`${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('property')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(filter.condition.property);
            });
          cy.get(`${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('operator')}`)
            .invoke('val')
            .then((val) => {
              expect(val).equals(filter.condition.operator);
            });
          if (filter.value) {
            cy.get(`${filterSelector} ${dataCy('filter-condition-input')} ${dataCy('expression')}`)
              .invoke('val')
              .then((val) => {
                expect(val).equals(filter.condition.value);
              });
          }
        }
        filter.show.map((showVal, showIndex) => {
          cy.get(
            `${filterSelector} ${dataCy('filter-showlist-input')} ${dataCy(
              `show-${showIndex}`
            )} ${dataCy('name')}`
          )
            .invoke('val')
            .then((val) => {
              expect(val).equals(showVal.name);
            });

          cy.get(
            `${filterSelector} ${dataCy('filter-showlist-input')} ${dataCy(
              `show-${showIndex}`
            )} ${dataCy('type')}`
          )
            .invoke('val')
            .then((val) => {
              expect(val).equals(showVal.type);
            });
        });
      });

      // Compare the live JSON code with our mock data
      cy.get(dataCy('live-json')).should((jsonContent) => {
        const obj = JSON.parse(jsonContent.text());
        const filters = obj.filters;

        expect(filters).to.deep.equal(MOCK_FILTERS);
      });
    });
  });

  describe('Importing data', () => {
    beforeEach(() => {
      cy.visit('/cdap/ns/default/plugincreation');
    });

    it('throw valid error when trying to upload invalid files', () => {
      const filename = 'invalid-plugin.2json';
      const fileUploader = 'plugin-json-uploader';

      cy.get(dataCy('plugin-json-import-btn')).click();

      cy.upload_plugin_json(filename, fileUploader);
      cy.contains('SyntaxError');
    });

    it('should populate the imported results for HTTP-batchsource.json', () => {
      const filename = 'HTTP-batchsource.json';
      const fileUploader = 'plugin-json-uploader';
      const filenameWithoutExtension = filename.substring(0, filename.lastIndexOf('.'));
      const [pluginName, pluginType] = filenameWithoutExtension.split('-');

      cy.get(dataCy('plugin-json-import-btn')).click();

      cy.upload_plugin_json(filename, fileUploader);

      cy.fixture(filename).then((data) => {
        // Plugin Information page has been populated with import results.
        cy.get(dataCy('plugin-json-filename')).contains(`${pluginName}-${pluginType}.json`);

        if (data['display-name']) {
          cy.get(dataCy('display-name'))
            .invoke('val')
            .then((val) => {
              expect(val).equals(data['display-name'] || '');
            });
          cy.get(dataCy('live-json')).should((jsonContent) => {
            const renderedObj = JSON.parse(jsonContent.text());
            expect(renderedObj['display-name']).to.eq(data['display-name']);
          });
        }

        // Configuration Group page has been populated with import results.
        cy.get(dataCy('next-step-btn')).click();
        checkImportedConfigurationGroups(data['configuration-groups']);

        // Output page has been populated with import results.
        cy.get(dataCy('next-step-btn')).click();
        checkImportedOutputs(data.outputs[0]);

        // Filters page has been populated with import results.
        cy.get(dataCy('next-step-btn')).click();
        checkImportedFilters(data.filters);

        // Compare the live JSON code with our mock data
        cy.get(dataCy('live-json')).should((jsonContent) => {
          const obj = JSON.parse(jsonContent.text());

          expect(obj).to.deep.equal(data);
        });
      });
    });
  });
});
