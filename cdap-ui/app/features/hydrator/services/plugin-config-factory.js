/*
 * Copyright © 2015 Cask Data, Inc.
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
class PluginConfigFactory {
  constructor (MyCDAPDataSource, $q, myHelpers) {
    this.dataSrc = new MyCDAPDataSource();
    this.$q = $q;
    this.myHelpers = myHelpers;
  }
  // Seems super lame. Need to remove this.
  fetch(templateid, pluginid) {
    return this.dataSrc.config({
      templateid: templateid, //'etlRealtime',
      pluginid: pluginid //'TwitterSource'
    });
  }

  generateNodeConfig(backendProperties, nodeConfig) {
    var isNewSpecVersion = this.myHelpers.objectQuery(nodeConfig, 'metadata', 'spec-version');
    if (isNewSpecVersion) {
      return this.generateConfigForNewSpec(backendProperties, nodeConfig);
    } else {
      return this.generateConfigForOlderSpec(backendProperties, nodeConfig);
    }
  }

  generateConfigForNewSpec(backendProperties, nodeConfig) {
    var propertiesFromBackend = Object.keys(backendProperties);
    var groupsConfig = {
      backendProperties: propertiesFromBackend,
      outputSchema: {
        isOutputSchemaExists: false,
        schemaProperty: null,
        isOutputSchemaRequired: null,
        implicitSchema: null
      },
      groups: []
    };
    var missedFieldsGroup = {
      display: 'Generic',
      fields: []
    };

    // Parse configuration groups
    nodeConfig['configuration-groups'].forEach( (group) => {
      let matchedProperties = group.properties.filter( (property) => {
        let index = propertiesFromBackend.indexOf(property.name);
        if (index !== -1) {
          propertiesFromBackend.splice(index, 1);
          let description = property.description;
          if (!description || (description && !description.length)) {
            description = this.myHelpers.objectQuery(groupsConfig, 'backendProperties', property.name, 'description');
            property.description = description || 'No Description Available';
          }
          property.label = property.label || property.name;
          return true;
        }
        return false;
      });
      groupsConfig.groups.push({
        display: group.label,
        fields: matchedProperties
      });
    });

    // Parse 'outputs' and find the property that needs to be used as output schema.
    if (nodeConfig.outputs && nodeConfig.outputs.length) {
      nodeConfig.outputs.forEach( output => {
        if (output['widget-type'] === 'non-editable-schema-editor') {
          groupsConfig.outputSchema.isOutputSchemaExists = true;
          groupsConfig.outputSchema.implicitSchema = this.myHelpers.objectQuery(nodeConfig, 'widget-attributes', 'schema');
        }
      });
    }

    // Parse properties that are from backend but not from config json.
    if (propertiesFromBackend.length) {
      propertiesFromBackend.forEach( property => {
        missedFieldsGroup.fields.push({
          'widget-type': 'textbox',
          label: property,
          info: 'Info',
          description: this.myHelpers.objectQuery(backendProperties, property, 'description') || 'No Description Available'
        });
      });
      groupsConfig.groups.push(missedFieldsGroup);
    }

    return groupsConfig;
  }

  generateConfigForOlderSpec(backendProperties, nodeConfig) {
    var propertiesFromBackend = Object.keys(backendProperties);
    var groupConfig = {
      backendProperties: propertiesFromBackend,
      outputSchema: {
        isOutputSchemaExists: false,
        schemaProperty: null,
        isOutputSchemaRequired: null,
        implicitSchema: false
      },
      groups: []
    };
    var missedFieldsGroup = {
      display: 'Generic',
      position: [],
      fields: []
    };
    var index;
    var schemaProperty;

    // Parse 'outputs' and find the property that needs to be used as output schema.
    if (nodeConfig.outputschema) {
      groupConfig.outputSchemaProperty = Object.keys(nodeConfig.outputschema);

      if (!nodeConfig.outputschema.implicit) {
        groupConfig.outputSchema.isOutputSchemaExists = (propertiesFromBackend.indexOf(groupConfig.outputSchemaProperty[0]) !== -1);
        if (groupConfig.outputSchema.isOutputSchemaExists) {
          schemaProperty = groupConfig.outputSchemaProperty[0];
          index = propertiesFromBackend.indexOf(schemaProperty);
          groupConfig.outputSchema.schemaProperty = nodeConfig.outputschema[schemaProperty];
          groupConfig.isOutputSchemaRequired = backendProperties[schemaProperty].required;
          propertiesFromBackend.splice(index, 1);
        }
      } else if (nodeConfig.outputschema && nodeConfig.outputschema.implicit) {
        groupConfig.outputSchema.implicitSchema = true;
        groupConfig.outputSchema.isOutputSchemaExists = true;
      }
    } else {
      groupConfig.outputSchema.isOutputSchemaExists = false;
    }

    // Parse configuration groups
    angular.forEach( nodeConfig.groups.position, (groupName) => {
      var group = nodeConfig.groups[groupName];
      var newGroup = {};
      newGroup.label = group.display;
      newGroup.fields = [];
      angular.forEach(group.position, (fieldName) => {
        var copyOfField = group.fields[fieldName];
        var index = propertiesFromBackend.indexOf(fieldName);
        if (index!== -1) {
          propertiesFromBackend.splice(index, 1);

          copyOfField.name = fieldName;
          copyOfField.info = this.myHelpers.objectQuery(groupConfig, 'groups', groupName, 'fields', fieldName, 'info') || 'Info';
          copyOfField.defaultValue = this.myHelpers.objectQuery(groupConfig, 'groups', groupName, 'fields', fieldName, 'properties', 'default');

          // If there is a description in the config from nodejs use that otherwise fallback to description from backend.
          let description = this.myHelpers.objectQuery(nodeConfig, 'groups', groupName, 'fields', fieldName, 'description');
          if (!description || (description && !description.length)) {
            description = this.myHelpers.objectQuery(groupConfig, 'backendProperties', fieldName, 'description');
            copyOfField.description = description || 'No Description Available';
          }
          let label = this.myHelpers.objectQuery(nodeConfig, 'groups', groupName, 'fields', fieldName, 'label');
          if (!label) {
            copyOfField.label = fieldName;
          }
        }
        newGroup.fields.push(copyOfField);
      });
      groupConfig.groups.push(newGroup);
    });

    // After iterating over all the groups check if the propertiesFromBackend is still empty
    // If not there are some fields from backend for which we don't have configuration from the nodejs.
    // Add them to the 'missedFieldsGroup' and show it as a separate group.
    if (propertiesFromBackend.length) {
      let genericGroup = {
        label: 'Generic',
        fields: []
      };
      angular.forEach(propertiesFromBackend, (property) => {
        genericGroup.fields.push({
          widget: 'textbox',
          label: property,
          info: 'Info',
          description: this.myHelpers.objectQuery(backendProperties, property, 'description') || 'No Description Available'
        });
      });
      groupConfig.groups.push(missedFieldsGroup);
    }
    return groupConfig;
  }
}

PluginConfigFactory.$inject = ['MyCDAPDataSource', '$q', 'myHelpers'];
angular.module(PKG.name + '.feature.hydrator')
  .service('PluginConfigFactory', PluginConfigFactory);
