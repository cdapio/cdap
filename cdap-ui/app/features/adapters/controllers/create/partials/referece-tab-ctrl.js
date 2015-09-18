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
  .controller('ReferenceTabController', function(MyNodeConfigService, MyAppDAGService, GLOBALS, $rootScope, $sce) {
    if (MyAppDAGService.metadata.template.type === GLOBALS.etlBatch) {
      this.infoPluginType = 'batch';
    } else if (MyAppDAGService.metadata.template.type === GLOBALS.etlRealtime) {
      this.infoPluginType = 'real-time';
    }
    var plugin = MyNodeConfigService.plugin;
    if (!plugin) {
      return;
    }


    this.constructUrl = function() {
      this.infoPluginCategory = plugin.type;
      this.infoPluginName = plugin.name.toLowerCase();
      var hideNav = '.html?hidenav';
      var baseUrl = [
        'http://docs.cask.co/cdap/' + $rootScope.cdapVersion + '/en/included-applications/etl/plugins/'
      ];

      if (this.infoPluginCategory === 'transform') {
        this.infoPluginCategory = 'transform';
      }

      baseUrl.push(this.infoPluginCategory + 's', '/' , this.infoPluginName, hideNav);
      this.infoUrl = baseUrl.join('');
    };

    this.trustSrc = function(src) {
      return $sce.trustAsResourceUrl(src);
    };

    this.constructUrl();
  });
