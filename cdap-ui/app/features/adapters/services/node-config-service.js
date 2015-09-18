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

angular.module(PKG.name + '.feature.adapters')
  .service('MyNodeConfigService', function() {
    this.pluginChangeListeners = {};
    this.pluginResetListeners = {};
    this.pluginRemoveListeners = {};
    this.pluginSaveListeners = {};

    this.resetPlugin = function(plugin) {
      this.plugin = plugin;
      this.notifyPluginResetListeners();
    };

    this.notifyPluginResetListeners = function() {
      angular.forEach(this.pluginResetListeners, function(callback) {
        callback(this.plugin);
      }.bind(this));
    };

    this.registerPluginResetCallback = function(id, callback) {
      this.pluginResetListeners[id] = callback;
    };

    this.unRegisterPluginResetCallback = function(id) {
      delete this.pluginResetListeners[id];
    };

    this.setPlugin = function(plugin) {
      this.plugin = plugin;
      this.notifyPluginSetListeners();
    };

    this.notifyPluginSetListeners = function () {
      angular.forEach(this.pluginChangeListeners, function(callback) {
        callback(this.plugin);
      }.bind(this));
    };

    this.registerPluginSetCallback = function(id, callback) {
      this.pluginChangeListeners[id] = callback;
    };

    this.unRegisterPluginSetCallback = function(id) {
      delete this.pluginChangeListeners[id];
    };

    this.removePlugin = function(nodeId) {
      this.plugin = null;
      this.notifyPluginRemoveListeners(nodeId);
    };

    this.registerRemovePluginCallback = function(id, callback) {
      this.pluginRemoveListeners[id] = callback;
    };

    this.notifyPluginRemoveListeners = function(nodeId) {
      angular.forEach(this.pluginRemoveListeners, function(callback) {
        callback(nodeId);
      });
    };

    this.unRegisterRemovePluginCallback = function(id) {
      delete this.pluginRemoveListeners[id];
    };

    this.notifyPluginSaveListeners = function(nodeId) {
      angular.forEach(this.pluginSaveListeners, function(callback) {
        callback(nodeId);
      });
    };

    this.registerPluginSaveCallback = function(id, callback) {
      this.pluginSaveListeners[id] = callback;
    };

    this.unRegisterPluginSaveCallback = function(id) {
      delete this.pluginSaveListeners[id];
    };

  });
