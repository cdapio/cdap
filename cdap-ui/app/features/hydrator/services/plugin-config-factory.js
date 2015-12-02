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
      label: 'Generic',
      properties: []
    };
    // Parse configuration groups
    nodeConfig['configuration-groups'].forEach( (group) => {
      let matchedProperties = group.properties.filter( (property) => {
        let index = propertiesFromBackend.indexOf(property.name);
        if (index !== -1) {
          propertiesFromBackend.splice(index, 1);
          return true;
        }
        return false;
      });
      groupsConfig.groups.push({
        label: group.label,
        properties: matchedProperties
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
        missedFieldsGroup.properties.push({
          'widget-type': 'textbox',
          label: property,
          info: 'Info',
          description: this.myHelpers.objectQuery(backendProperties, property, 'description') || 'No Description Available'
        });
      });
      nodeConfig['configuration-groups'].push(missedFieldsGroup);
    }

    return nodeConfig;
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
      groups: {}
    };
    var missedFieldsGroup = {
      display: 'Generic',
      position: [],
      fields: {}
    };
    var index;
    var schemaProperty;

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

    groupConfig.groups.position = nodeConfig.groups.position;
    angular.forEach(
      nodeConfig.groups.position,
      this.setGroups.bind(this, propertiesFromBackend, nodeConfig, groupConfig)
    );

    // After iterating over all the groups check if the propertiesFromBackend is still empty
    // If not there are some fields from backend for which we don't have configuration from the nodejs.
    // Add them to the 'missedFieldsGroup' and show it as a separate group.
    if (propertiesFromBackend.length) {
      angular.forEach(
        propertiesFromBackend,
        this.setMissedFields.bind(this, backendProperties, missedFieldsGroup)
      );
      groupConfig.groups.position.push('generic');
      groupConfig.groups['generic'] = missedFieldsGroup;
    }

    return groupConfig;
  }

  setGroups(propertiesFromBackend, nodeConfig, groupConfig, group) {
    // For each group in groups iterate over its fields in position (order of all fields)
    var fieldsInGroup = nodeConfig.groups[group].position;
    // Add an entry for the group in our local copy.
    groupConfig.groups[group] = {
      display: nodeConfig.groups[group].display,
      position: [],
      fields: {}
    };
    angular.forEach(fieldsInGroup, this.setGroupFields.bind(this, propertiesFromBackend, nodeConfig, group, groupConfig));
  }

  setGroupFields(propertiesFromBackend, nodeConfig, group, groupConfig, field) {
    // For each field in the group check if its been provided by the backend.
    // If yes add it to the local copy of groups
    // and mark the field as added.(remove from propertiesFromBackend array)
    var index = propertiesFromBackend.indexOf(field);
    if (index!== -1) {
      propertiesFromBackend.splice(index, 1);
      groupConfig.groups[group].position.push(field);
      groupConfig.groups[group].fields[field] = nodeConfig.groups[group].fields[field];
      // If there is a description in the config from nodejs use that otherwise fallback to description from backend.
      var description = this.myHelpers.objectQuery(nodeConfig, 'groups', group, 'fields', field, 'description');
      var info = this.myHelpers.objectQuery(groupConfig, 'groups', group, 'fields', field, 'info') ;
      var label = this.myHelpers.objectQuery(groupConfig, 'groups', group, 'fields', field, 'label');

      if (!description || (description && !description.length)) {
        description = this.myHelpers.objectQuery(groupConfig, 'backendProperties', field, 'description');
        groupConfig.groups[group].fields[field].description = description || 'No Description Available';
      }
      groupConfig.groups[group].fields[field].info = info || 'Info';
      if (!label) {
        groupConfig.groups[group].fields[field].label = field;
      }
      groupConfig.groups[group].fields[field].defaultValue = this.myHelpers.objectQuery(groupConfig, 'groups', group, 'fields', field, 'properties', 'default');
    }
  }

  setMissedFields (backendProperties, missedFieldsGroup, property) {
    missedFieldsGroup.position.push(property);
    missedFieldsGroup.fields[property] = {
      widget: 'textbox',
      label: property,
      info: 'Info',
      description: this.myHelpers.objectQuery(backendProperties, property, 'description') || 'No Description Available'
    };
  }

}

PluginConfigFactory.$inject = ['MyCDAPDataSource', '$q', 'myHelpers'];
angular.module(PKG.name + '.feature.hydrator')
  .service('PluginConfigFactory', PluginConfigFactory);
