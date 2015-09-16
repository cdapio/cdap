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
    this.pluginChangeListeners = [];
    this.setPlugin = function(plugin) {
      this.plugin = plugin;
      this.notifyListeners();
    };

    this.notifyListeners = function () {
      this.pluginChangeListeners.forEach(function(callback) {
        callback(this.plugin);
      }.bind(this));
    };

    this.registerPluginCallback = function(callback) {
      this.pluginChangeListeners.push(callback);
    };

  });
