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
class HydratorPlusPlusPluginConfigFactory {
  constructor ($q, myHelpers, myPipelineApi, $state, myAlertOnValium) {
    this.$q = $q;
    this.myHelpers = myHelpers;
    this.myPipelineApi = myPipelineApi;
    this.myAlertOnValium = myAlertOnValium;
    this.$state = $state;
    this.configurationGroupUtilities = window.CaskCommon.ConfigurationGroupUtilities;
    this.data = {};
    this.validatePluginProperties = this.validatePluginProperties.bind(this);
  }
  fetchWidgetJson(artifactName, artifactVersion, artifactScope, key) {
    let cache = this.data[`${artifactName}-${artifactVersion}-${artifactScope}-${key}`];
    if (cache) {
      return this.$q.when(cache);
    }

    return this.myPipelineApi.fetchArtifactProperties({
      namespace: this.$state.params.namespace || this.$state.params.nsadmin,
      artifactName: artifactName,
      artifactVersion: artifactVersion,
      scope: artifactScope,
      keys: key
    })
      .$promise
      .then(
        (res) => {
          try {
            let config = res[key];
            if (config) {
              config = JSON.parse(config);
              this.data[`${artifactName}-${artifactVersion}-${key}`] = config;
              return config;
            } else {
              throw 'NO_JSON_FOUND';
            }
          } catch (e) {
            throw (e && e.name === 'SyntaxError')? 'CONFIG_SYNTAX_JSON_ERROR': e;
          }
        },
        () => {
          throw 'NO_JSON_FOUND';
        }
      );
  }
  fetchDocJson(artifactName, artifactVersion, artifactScope, key) {
    return this.myPipelineApi.fetchArtifactProperties({
      namespace: this.$state.params.namespace,
      artifactName: artifactName,
      artifactVersion: artifactVersion,
      scope: artifactScope,
      keys: key
    }).$promise;
  }

  generateNodeConfig(backendProperties, nodeConfig) {
    var specVersion = this.myHelpers.objectQuery(nodeConfig, 'metadata', 'spec-version') || '0.0';
    switch (specVersion) {
      case '0.0':
        return this.generateConfigForOlderSpec(backendProperties, nodeConfig);
      case '1.0':
      case '1.1':
        return this.generateConfigForNewSpec(backendProperties, nodeConfig);
      case '1.2':
        return this.generateConfigFor12Spec(backendProperties, nodeConfig);
      case '1.3':
      case '1.4':
      case '1.5':
      case '1.6':
        return this.generateConfigFor13Spec(backendProperties, nodeConfig);
      default: // No spec version which means
        throw 'NO_JSON_FOUND';
    }
  }

  generateConfigFor13Spec(backendProperties, nodeConfig) {
    var config = this.generateConfigFor12Spec(backendProperties, nodeConfig);
    config.jumpConfig = {};
    if (typeof nodeConfig['jump-config'] === 'object') {
      config.jumpConfig = nodeConfig['jump-config'];
    }
    return config;
  }

  generateConfigFor12Spec(backendProperties, nodeConfig) {
    var config = this.generateConfigForNewSpec(backendProperties, nodeConfig);
    config.inputs = {};
    if (typeof nodeConfig.inputs === 'object') {
      if (nodeConfig.inputs.multipleInputs) {
        config.inputs.multipleInputs = true;
      }
    }
    return config;
  }

  generateConfigForNewSpec(backendProperties, nodeConfig) {
    var propertiesFromBackend = Object.keys(backendProperties);
    var groupsConfig = {
      backendProperties: propertiesFromBackend,
      outputSchema: {
        isOutputSchemaExists: false,
        schemaProperties: null,
        outputSchemaProperty: null,
        isOutputSchemaRequired: null,
        implicitSchema: null,
        watchProperty: null
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
            description = this.myHelpers.objectQuery(backendProperties, property.name, 'description');
            property.description = description || 'No Description Available';
          }
          property.label = property.label || property.name;
          property.defaultValue = this.myHelpers.objectQuery(property, 'widget-attributes', 'default');
          return true;
        }
        return false;
      });
      groupsConfig.groups.push({
        display: group.label,
        description: group.description,
        fields: matchedProperties
      });
    });

    // Parse 'outputs' and find the property that needs to be used as output schema.
    if (nodeConfig.outputs && nodeConfig.outputs.length) {
      nodeConfig.outputs.forEach( output => {
        var index;
        if (output['widget-type'] === 'non-editable-schema-editor') {
          groupsConfig.outputSchema.isOutputSchemaExists = true;
          groupsConfig.outputSchema.implicitSchema = output.schema;
        } else {
          index = propertiesFromBackend.indexOf(output.name);
          if (index !== -1) {
            propertiesFromBackend.splice(index, 1);
            groupsConfig.outputSchema.isOutputSchemaExists = true;
            groupsConfig.outputSchema.outputSchemaProperty = [output.name];
            groupsConfig.outputSchema.schemaProperties = output['widget-attributes'];
            groupsConfig.outputSchema.isOutputSchemaRequired = backendProperties[output.name].required;
          }
        }
      });
    }

    // Parse properties that are from backend but not from config json.
    if (propertiesFromBackend.length) {
      propertiesFromBackend.forEach( property => {
        missedFieldsGroup.fields.push({
          'widget-type': 'textbox',
          label: property,
          name: property,
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
        schemaProperties: null,
        outputSchemaProperty: null,
        isOutputSchemaRequired: null,
        implicitSchema: false
      },
      groups: []
    };
    var index;
    var schemaProperty;

    // Parse 'outputs' and find the property that needs to be used as output schema.
    if (nodeConfig.outputschema) {
      groupConfig.outputSchema.outputSchemaProperty = Object.keys(nodeConfig.outputschema);

      if (!nodeConfig.outputschema.implicit) {
        groupConfig.outputSchema.isOutputSchemaExists = (propertiesFromBackend.indexOf(groupConfig.outputSchema.outputSchemaProperty[0]) !== -1);
        if (groupConfig.outputSchema.isOutputSchemaExists) {
          schemaProperty = groupConfig.outputSchema.outputSchemaProperty[0];
          index = propertiesFromBackend.indexOf(schemaProperty);
          groupConfig.outputSchema.schemaProperties = nodeConfig.outputschema[schemaProperty];
          groupConfig.outputSchema.isOutputSchemaRequired = backendProperties[schemaProperty].required;
          propertiesFromBackend.splice(index, 1);
        }
      } else if (nodeConfig.outputschema && nodeConfig.outputschema.implicit) {
        groupConfig.outputSchema.implicitSchema = nodeConfig.outputschema.implict;
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

          // If there is a description in the config from nodejs use that otherwise fallback to description from backend.
          let description = this.myHelpers.objectQuery(nodeConfig, 'groups', groupName, 'fields', fieldName, 'description');
          if (!description || (description && !description.length)) {
            description = this.myHelpers.objectQuery('backendProperties', fieldName, 'description');
            copyOfField.description = description || 'No Description Available';
          }

          let label = this.myHelpers.objectQuery(nodeConfig, 'groups', groupName, 'fields', fieldName, 'label');
          if (!label) {
            copyOfField.label = fieldName;
          }

          copyOfField.defaultValue = this.myHelpers.objectQuery(nodeConfig, 'groups', groupName, 'fields', fieldName, 'properties', 'default');
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
      groupConfig.groups.push(genericGroup);
    }
    return groupConfig;
  }

  validatePluginProperties(nodeInfo, errorCb) {
    // for post-run plugins, use nodeInfo. For other plugins, get plugin property. 
    const pluginInfo = nodeInfo.plugin? nodeInfo.plugin : nodeInfo;
    const plugin = angular.copy(pluginInfo);
    if (!plugin.type) {
      plugin.type = nodeInfo.type;
    }
    const requestBody = {
      stage: {
        name: this.myHelpers.objectQuery(pluginInfo, 'name'),
        plugin
      }, 
      inputSchemas: !nodeInfo.inputSchema ? [] : nodeInfo.inputSchema.map(input => {
        let schema;
        try {
          schema = JSON.parse(input.schema);
        } catch (e) {
          // no-op
        }
      })
    };

    const parseResSchema = (res) => {
      if (res.name && res.type && res.fields) {
        return [this.getOutputSchemaObj(res)];
      }
      let schemaArr = [];
      angular.forEach(res, (value, key) => {
        if (value.name && value.type && value.fields) {
          schemaArr.push(this.getOutputSchemaObj(value, key));
        }
      });
      let recordSchemas = schemaArr.filter(schema => schema.name.substring(0, 6) === 'record');
      let schemaArrWithoutRecordSchemas = _.difference(schemaArr, recordSchemas);
      let schemaArrWithSortedRecordSchemas = schemaArrWithoutRecordSchemas.concat(_.sortBy(recordSchemas, 'name'));
      return schemaArrWithSortedRecordSchemas;
    };

    const params = {
      context: this.$state.params.namespace || this.$state.params.nsadmin
    };

    this.myPipelineApi.validateStage(params, requestBody)
      .$promise
      .then((res) => {
        let errorCount; 
        if (res.failures.length > 0) {
          const { propertyErrors, inputSchemaErrors,outputSchemaErrors } = this.configurationGroupUtilities.constructErrors(res.failures);
          errorCount = this.configurationGroupUtilities.countErrors(propertyErrors, inputSchemaErrors, outputSchemaErrors);
          this.myAlertOnValium.show({
            type: 'danger',
            content: `${errorCount} error${errorCount > 1 ? 's': ''} found.`
          });
          errorCb({ errorCount, propertyErrors, inputSchemaErrors, outputSchemaErrors });
        } else {
          errorCount = 0;
          const outputSchema = this.myHelpers.objectQuery(res, 'spec', 'outputSchema');
          const portSchemas = this.myHelpers.objectQuery(res, 'spec', 'portSchemas');
          let schemas;
          if (outputSchema || portSchemas) {
            schemas = parseResSchema(outputSchema || portSchemas).map(schema => {
              return {
                name: schema.name,
                schema: JSON.stringify(schema.schema)
              };
            });
          }
          errorCb({ errorCount });
          this.myAlertOnValium.show({
            type: 'success',
            content: `No validation errors.`
          });
          if (schemas.length) {
            this.EventPipe.emit('schema.import', schemas);
          }
          else {
            this.EventPipe.emit('schema.clear');
          }
        }
      }, (err) => {
        this.myAlertOnValium.show({
            type: 'danger',
            content: 'Error occurred while validating.'
        });
        const propertyErrors =  { 'orphanErrors': [{ msg:err.data }]};
        errorCb({ propertyErrors });
      });
  }
}

HydratorPlusPlusPluginConfigFactory.$inject = ['$q', 'myHelpers', 'myPipelineApi', '$state', 'myAlertOnValium'];
angular.module(PKG.name + '.feature.hydrator')
  .service('HydratorPlusPlusPluginConfigFactory', HydratorPlusPlusPluginConfigFactory);
