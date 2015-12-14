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

angular.module(PKG.name + '.feature.hydrator')
  .factory('HydratorErrorFactory', function (GLOBALS, ConfigStore, NodesStore, ConsoleActionsFactory) {

    var ERROR_MESSAGES = GLOBALS.en.hydrator.studio;

    function isModelValid () {

      var validationRules = [
        hasName,
        hasOnlyOneSource,
        hasAtLeastOneSink,
        requiredFieldsHaveContent
      ];

      var isValid = true;

      validationRules.forEach(function(rule) {
        if (!rule.call(this)) {
          isValid = false;
        }
      });

      return isValid;

    }

    function hasName () {
      var name = ConfigStore.getName();
      if (!name || name.length === 0) {
        ConsoleActionsFactory.addMessage({
          type: 'error',
          content: ERROR_MESSAGES.nameError
        });
        return false;
      }

      return hasValidName();
    }

    function hasValidName () {
      var name = ConfigStore.getName();

      var pattern = /^[\w]+$/;

      if (!pattern.test(name)) {
        ConsoleActionsFactory.addMessage({
          type: 'error',
          content: ERROR_MESSAGES.nameValidationError
        });
        return false;
      }
      return true;
    }

    function hasOnlyOneSource () {
      var nodes = NodesStore.getNodes();
      var countSource = 0;

      angular.forEach(nodes, function (node) {
        if (GLOBALS.pluginConvert[node.type] === 'source') {
          countSource++;
        }
      });

      if (countSource === 0) {
        ConsoleActionsFactory.addMessage({
          type: 'error',
          content: ERROR_MESSAGES.noSourceError
        });
        return false;
      }

      if (countSource > 1) {
        ConsoleActionsFactory.addMessage({
          type: 'error',
          content: ERROR_MESSAGES.oneSourceError
        });
        return false;
      }
      return true;
    }

    function hasAtLeastOneSink () {
      var nodes = NodesStore.getNodes();
      var countSink = 0;

      angular.forEach(nodes, function (node) {
        if (GLOBALS.pluginConvert[node.type] === 'sink') {
          countSink++;
        }
      });

      if (countSink === 0) {
        ConsoleActionsFactory.addMessage({
          type: 'error',
          content: ERROR_MESSAGES.atLeastOneSinkError
        });
        return false;
      }

      return true;
    }

    function requiredFieldsHaveContent () {
      var nodes = NodesStore.getNodes();

      var plugins = [];
      angular.forEach(nodes, function (node) {
        if (node.requiredFieldCount > 0) {
          plugins.push(node.label);
          node.warning = false;
          ConsoleActionsFactory.addMessage({
            type: 'error',
            content: node.plugin.label + ERROR_MESSAGES.pluginNameMissingRequiredFieldsError
          });
        }
      });

      if (plugins.length > 0) {
        return false;
      }

      return true;
    }


    function isValidPlugin(plugin) {
      var i;
      var keys = Object.keys(plugin.properties);
      plugin.valid = true;
      plugin.requiredFieldCount = 0;
      if (!keys.length) {
        keys = Object.keys(plugin._backendProperties);
        for (i =0; i<keys.length; i++) {
          if (plugin._backendProperties[keys[i]] && plugin._backendProperties[keys[i]].required) {
            plugin.requiredFieldCount += 1;
            plugin.valid = false;
          }
        }
        return plugin.valid;
      }
      for (i=0; i< keys.length; i++) {
        var property = plugin.properties[keys[i]];
        if (plugin._backendProperties[keys[i]] && plugin._backendProperties[keys[i]].required && (!property || property === '')) {
          plugin.valid = false;
          plugin.requiredFieldCount += 1;
        }
      }
      return plugin.valid;
    }

    function countRequiredFields(node) {
      var requiredFieldCount = 0;
      if (angular.isObject(node._backendProperties) && Object.keys(node._backendProperties).length) {

        angular.forEach(node._backendProperties, function (value, key) {
          if (value.required) {
            if (!node.plugin.properties || !node.plugin.properties[key]) {
              requiredFieldCount++;
            }
          }
        });
      }
      return requiredFieldCount;
    }

    return {
      isModelValid: isModelValid,
      isValidPlugin: isValidPlugin,
      countRequiredFields: countRequiredFields
    };

  });
