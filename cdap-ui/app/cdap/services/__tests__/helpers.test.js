/*
 * Copyright © 2017 Cask Data, Inc.
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

import { getArtifactNameAndVersion, sanitizeNodeNamesInPluginProperties } from 'services/helpers';
jest.disableAutomock();

describe('Unit Tests for Helpers: "getArtifactNameAndVersion"', () => {

  it('Usecase 1 - Valid: Should return correct name & version', () => {
    var jarfileName = 'wrangler-service-1.3.0-SNAPSHOT';
    let {name, version} = getArtifactNameAndVersion(jarfileName);
    expect(name).toBe('wrangler-service');
    expect(version).toBe('1.3.0-SNAPSHOT');
  });

  it('Usecase 2 - Invalid: Should return 1.0.0 for version if it could not find', () => {
    var jarfileName = 'invalid-file-name-without-a-version';
    let {name, version} = getArtifactNameAndVersion(jarfileName);
    expect(name).toBe(jarfileName);
    expect(version).toBe('1.0.0');
  });

  it('Usecase 3: Should ignore unnecessary patterns & return correct name, version', () => {
    var jarfileName = 'redshifttos3-action-plugin-1.0.0-SNAPSHOT';
    let {name, version} = getArtifactNameAndVersion(jarfileName);
    expect(name).toBe('redshifttos3-action-plugin');
    expect(version).toBe('1.0.0-SNAPSHOT');
  });

  it('Usecase 4: Should return undefined for name & version if provided with an undefined input', () => {
    let {name, version} = getArtifactNameAndVersion();
    expect(name).toBe(undefined);
    expect(version).toBe(undefined);
  });

  it('Usecase 5: Should return "" for name & undefined for version if provided with an undefined input', () => {
    let {name, version} = getArtifactNameAndVersion('');
    expect(name).toBe('');
    expect(version).toBe(undefined);
  });

  it('Usecase 6: Should return null for name & undefined for version if provided with an undefined input', () => {
    let {name, version} = getArtifactNameAndVersion(null);
    expect(name).toBe(null);
    expect(version).toBe(undefined);
  });

  it('Usecase 7: Should return filename for name and version when filename is the version i.e 1.2.3.jar', () => {
    let {name, version} = getArtifactNameAndVersion('1.2.3');
    expect(name).toBe('1.2.3');
    expect(version).toBe('1.2.3');
  });

  it('Usecase 8: Should return filename for name and version when filename ends with a version i.e ojdbc8.jar', () => {
    const FILE_NAME = 'ojdbc8';
    let {name, version} = getArtifactNameAndVersion(FILE_NAME);
    expect(name).toBe(FILE_NAME);
    expect(version).toBe('8');
  });

});
describe.skip('Unit Tests for Helpers: "Sanitize nodes for copy/paste"', () => {
  const availablePlugins = {
    plugins: {
      pluginsMap: {
        'Wrangler-transform-wrangler-transform-4.2.2-SNAPSHOT-SYSTEM': {
          "widgets": {
            "outputs": [
              {
                "name": "schema",
                "widget-type": "schema",
                "label": "schema",
                "widget-attributes": {
                  "schema-types": [
                    "boolean",
                    "int",
                    "long",
                    "float",
                    "double",
                    "bytes",
                    "string",
                    "array",
                    "enum",
                    "record",
                    "map",
                    "union"
                  ],
                  "schema-default-type": "string",
                  "property-watch": "format"
                }
              }
            ],
            "metadata": {
              "spec-version": "1.6"
            },
            "configuration-groups": [
              {
                "label": "Input Selection and Prefilters",
                "properties": [
                  {
                    "widget-type": "hidden",
                    "name": "workspace"
                  },
                  {
                    "widget-type": "textbox",
                    "name": "field",
                    "label": "Input field name",
                    "widget-attributes": {
                      "default": "*"
                    }
                  },
                  {
                    "widget-type": "textbox",
                    "name": "precondition",
                    "label": "Precondition",
                    "widget-attributes": {
                      "default": "false"
                    }
                  }
                ]
              },
              {
                "label": "Directives",
                "properties": [
                  {
                    "widget-type": "wrangler-directives",
                    "name": "directives",
                    "label": "Recipe",
                    "widget-attributes": {
                      "placeholder": "#pragma load-directives my-directive; my-directive :body;"
                    }
                  },
                  {
                    "widget-type": "csv",
                    "name": "udd",
                    "label": "User Defined Directives(UDD)"
                  }
                ]
              },
              {
                "label": "Failure Conditions and Handling",
                "properties": [
                  {
                    "widget-type": "textbox",
                    "name": "threshold",
                    "label": "Failure Threshold",
                    "widget-attributes": {
                      "default": "1"
                    }
                  }
                ]
              }
            ],
            "emit-errors": true,
            "emit-alerts": true
          }
        },
        'Joiner-batchjoiner-core-plugins-2.4.2-SNAPSHOT-SYSTEM': {
          "widgets": {
            "outputs": [
              {
                "name": "schema",
                "widget-type": "schema"
              }
            ],
            "metadata": {
              "spec-version": "1.3"
            },
            "configuration-groups": [
              {
                "label": "Basic",
                "properties": [
                  {
                    "widget-type": "sql-select-fields",
                    "name": "selectedFields",
                    "description": "List of fields to be selected and/or renamed in the Joiner output from each stages. There must not be a duplicate fields in the output.",
                    "label": "Fields"
                  },
                  {
                    "widget-type": "join-types",
                    "name": "requiredInputs",
                    "description": "Type of joins to be performed. Inner join means all stages are required, while Outer join allows for 0 or more input stages to be required input.",
                    "label": "Join Type"
                  },
                  {
                    "widget-type": "sql-conditions",
                    "name": "joinKeys",
                    "description": "List of join keys to perform join operation.",
                    "label": "Join Condition"
                  },
                  {
                    "widget-type": "get-schema",
                    "widget-category": "plugin"
                  }
                ]
              },
              {
                "label": "Advanced",
                "properties": [
                  {
                    "widget-type": "textbox",
                    "name": "inMemoryInputs",
                    "label": "Inputs to Load in Memory"
                  },
                  {
                    "widget-type": "toggle",
                    "name": "joinNullKeys",
                    "label": "Join on Null Keys",
                    "widget-attributes": {
                      "default": "true",
                      "off": {
                        "label": "False",
                        "value": "false"
                      },
                      "on": {
                        "label": "True",
                        "value": "true"
                      }
                    }
                  },
                  {
                    "widget-type": "textbox",
                    "name": "numPartitions",
                    "label": "Number of Partitions"
                  }
                ]
              }
            ],
            "inputs": {
              "multipleInputs": true
            }
          }
        }
      }
    }
  };
  const nodes = [
    {
      "id": "Wrangler",
      "name": "Wrangler",
      "type": "transform",
      "plugin": {
        "name": "Wrangler",
        "artifact": {
          "name": "wrangler-transform",
          "version": "4.2.2-SNAPSHOT",
          "scope": "SYSTEM"
        },
        "properties": {
          "workspaceId": "c17c4dbd-37b0-47e9-ad25-5f36c8e4e1d8",
          "directives": "parse-as-csv :body '\\t' true\ndrop body",
          "schema": "{\"name\":\"avroSchema\",\"type\":\"record\",\"fields\":[{\"name\":\"Price\",\"type\":[\"string\",\"null\"]},{\"name\":\"Street_Address\",\"type\":[\"string\",\"null\"]},{\"name\":\"City\",\"type\":[\"string\",\"null\"]},{\"name\":\"State\",\"type\":[\"string\",\"null\"]},{\"name\":\"Zip\",\"type\":[\"string\",\"null\"]},{\"name\":\"Type\",\"type\":[\"string\",\"null\"]},{\"name\":\"Beds\",\"type\":[\"string\",\"null\"]},{\"name\":\"Baths\",\"type\":[\"string\",\"null\"]},{\"name\":\"Size\",\"type\":[\"string\",\"null\"]},{\"name\":\"Lot_Size\",\"type\":[\"string\",\"null\"]},{\"name\":\"Stories\",\"type\":[\"string\",\"null\"]},{\"name\":\"Built_In\",\"type\":[\"string\",\"null\"]},{\"name\":\"Sale_Date\",\"type\":[\"string\",\"null\"]}]}",
          "field": "body",
          "precondition": "false",
          "threshold": "1"
        },
        "label": "Wrangler"
      }
    },
    {
      "id": "Joiner",
      "name": "Joiner",
      "type": "batchjoiner",
      "plugin": {
        "name": "Joiner",
        "artifact": {
          "name": "core-plugins",
          "version": "2.4.2-SNAPSHOT",
          "scope": "SYSTEM"
        },
        "properties": {
          "schema": "{\"type\":\"record\",\"name\":\"join.typeoutput\",\"fields\":[{\"name\":\"Price1\",\"type\":[\"string\",\"null\"]},{\"name\":\"Price\",\"type\":[\"string\",\"null\"]},{\"name\":\"Street_Address\",\"type\":[\"string\",\"null\"]},{\"name\":\"City\",\"type\":[\"string\",\"null\"]},{\"name\":\"Type\",\"type\":[\"string\",\"null\"]},{\"name\":\"Beds\",\"type\":[\"string\",\"null\"]},{\"name\":\"Baths\",\"type\":[\"string\",\"null\"]},{\"name\":\"Size\",\"type\":[\"string\",\"null\"]},{\"name\":\"Lot_Size\",\"type\":[\"string\",\"null\"]},{\"name\":\"Stories\",\"type\":[\"string\",\"null\"]},{\"name\":\"Built_In\",\"type\":[\"string\",\"null\"]},{\"name\":\"Sale_Date\",\"type\":[\"string\",\"null\"]}]}",
          "joinKeys": "Wrangler.Price = new_wrangler.Price & Wrangler.Size = new_wrangler.Size",
          "selectedFields": "Wrangler.Price as Price1,new_wrangler.Price as Price,new_wrangler.Street_Address as Street_Address,new_wrangler.City as City,new_wrangler.Type as Type,new_wrangler.Beds as Beds,new_wrangler.Baths as Baths,new_wrangler.Size as Size,new_wrangler.Lot_Size as Lot_Size,new_wrangler.Stories as Stories,new_wrangler.Built_In as Built_In,new_wrangler.Sale_Date as Sale_Date",
          "requiredInputs": "Wrangler"
        },
        "label": "Joiner"
      }
    },
    {
      "id": "new_wrangler",
      "name": "new_wrangler",
      "type": "transform",
      "plugin": {
        "name": "Wrangler",
        "artifact": {
          "name": "wrangler-transform",
          "version": "4.2.2-SNAPSHOT",
          "scope": "SYSTEM"
        },
        "properties": {
          "workspaceId": "9a4ba87d-310c-4d4a-b306-0e0f63f3a08d",
          "directives": "parse-as-csv :body '\\t' true\ndrop body\ndrop Zip,State",
          "schema": "{\"name\":\"avroSchema\",\"type\":\"record\",\"fields\":[{\"name\":\"Price\",\"type\":[\"string\",\"null\"]},{\"name\":\"Street_Address\",\"type\":[\"string\",\"null\"]},{\"name\":\"City\",\"type\":[\"string\",\"null\"]},{\"name\":\"Type\",\"type\":[\"string\",\"null\"]},{\"name\":\"Beds\",\"type\":[\"string\",\"null\"]},{\"name\":\"Baths\",\"type\":[\"string\",\"null\"]},{\"name\":\"Size\",\"type\":[\"string\",\"null\"]},{\"name\":\"Lot_Size\",\"type\":[\"string\",\"null\"]},{\"name\":\"Stories\",\"type\":[\"string\",\"null\"]},{\"name\":\"Built_In\",\"type\":[\"string\",\"null\"]},{\"name\":\"Sale_Date\",\"type\":[\"string\",\"null\"]}]}",
          "field": "body",
          "precondition": "false",
          "threshold": "1"
        },
        "label": "new_wrangler"
      }
    }
  ];
  const oldNameToNewNameMap = {
    "Wrangler": "Wrangler21",
    "Joiner": "Joiner27",
    "new_wrangler": "new_wrangler46"
  };

  it('Should correctly replace joiner properties with new plugin reference names', () => {
    const newNodes = sanitizeNodeNamesInPluginProperties(nodes, availablePlugins, oldNameToNewNameMap);
    const newJoiner = newNodes.find(node => node.plugin.name === 'Joiner');
    const newJoinKeys = "Wrangler21.Price=new_wrangler46.Price&Wrangler21.Size=new_wrangler46.Size";
    const newSelectedKeys = "Wrangler21.Price as Price1,new_wrangler46.Price as Price,new_wrangler46.Street_Address as Street_Address,new_wrangler46.City as City,new_wrangler46.Type as Type,new_wrangler46.Beds as Beds,new_wrangler46.Baths as Baths,new_wrangler46.Size as Size,new_wrangler46.Lot_Size as Lot_Size,new_wrangler46.Stories as Stories,new_wrangler46.Built_In as Built_In,new_wrangler46.Sale_Date as Sale_Date";
    expect(newJoiner.plugin.properties.joinKeys).toBe(newJoinKeys.trim());
    expect(newJoiner.plugin.properties.selectedFields).toBe(newSelectedKeys);
    expect(newJoiner.plugin.properties.requiredInputs).toBe('Wrangler21');
  });

  it('Should not modify any other properties that doesn\'t reference stage reference name', () => {
    const newNodes = sanitizeNodeNamesInPluginProperties(nodes, availablePlugins, oldNameToNewNameMap);
    const newWrangler = newNodes.find(node => node.id === 'new_wrangler');
    expect(newWrangler.plugin.properties.directives).toBe("parse-as-csv :body '\\t' true\ndrop body\ndrop Zip,State");
    expect(newWrangler.plugin.properties.schema).toBe("{\"name\":\"avroSchema\",\"type\":\"record\",\"fields\":[{\"name\":\"Price\",\"type\":[\"string\",\"null\"]},{\"name\":\"Street_Address\",\"type\":[\"string\",\"null\"]},{\"name\":\"City\",\"type\":[\"string\",\"null\"]},{\"name\":\"Type\",\"type\":[\"string\",\"null\"]},{\"name\":\"Beds\",\"type\":[\"string\",\"null\"]},{\"name\":\"Baths\",\"type\":[\"string\",\"null\"]},{\"name\":\"Size\",\"type\":[\"string\",\"null\"]},{\"name\":\"Lot_Size\",\"type\":[\"string\",\"null\"]},{\"name\":\"Stories\",\"type\":[\"string\",\"null\"]},{\"name\":\"Built_In\",\"type\":[\"string\",\"null\"]},{\"name\":\"Sale_Date\",\"type\":[\"string\",\"null\"]}]}");
  });

  it('Should not break if widget json for plugin is not available', () => {
    delete availablePlugins.plugins.pluginsMap['Joiner-batchjoiner-core-plugins-2.4.2-SNAPSHOT-SYSTEM'].widgets;
    const newNodes = sanitizeNodeNamesInPluginProperties(nodes, availablePlugins, oldNameToNewNameMap);
    const newJoiner = newNodes.find(node => node.plugin.name === 'Joiner');
    const oldJoiner = nodes.find(node => node.plugin.name === 'Joiner');
    expect(newJoiner.plugin.properties.joinKeys).toBe(oldJoiner.plugin.properties.joinKeys);
    expect(newJoiner.plugin.properties.selectedFields).toBe(oldJoiner.plugin.properties.selectedFields);
    expect(newJoiner.plugin.properties.requiredInputs).toBe(oldJoiner.plugin.properties.requiredInputs);
  });
});
