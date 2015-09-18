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
  .controller('LeftPanelController', function(myAdapterApi, MyAppDAGService, MyDAGFactory, mySettings, $state, MySidebarService, $scope, rVersion, $stateParams, GLOBALS) {
    this.pluginTypes = [
      {
        name: 'source'
      },
      {
        name: 'transform'
      },
      {
        name: 'sink'
      }
    ];

    this.plugins= {
      items: []
    };

    this.panelstatus = {};
    this.panelstatus.isExpanded = true;

    $scope.$watch(function() {
      return this.panelstatus.isExpanded;
    }.bind(this), function() {
      MySidebarService.setIsExpanded(this.panelstatus.isExpanded);
    }.bind(this));

    this.onLeftSideGroupItemClicked = function(group) {
      var prom;
      var templateType = MyAppDAGService.metadata.template.type;
      var params = {
        namespace: $stateParams.namespace,
        adapterType: templateType,
        version: rVersion.version
      };
      switch(group.name) {
        case 'source':
          params.extensionType = GLOBALS.pluginTypes[templateType].source;
          prom = myAdapterApi.fetchSources(params).$promise;
          break;
        case 'transform':
          params.extensionType = GLOBALS.pluginTypes[templateType].transform;
          prom = myAdapterApi.fetchTransforms(params).$promise;
          break;
        case 'sink':
          params.extensionType = GLOBALS.pluginTypes[templateType].sink;
          prom = myAdapterApi.fetchSinks(params).$promise;
          break;

      }
      prom
        .then(function(res) {
          this.plugins.items = [];
          res.forEach(function(plugin) {
            this.plugins.items.push(
              angular.extend(
                {
                  type: group.name,
                  icon: MyDAGFactory.getIcon(plugin.name)
                },
                plugin
              )
            );
          }.bind(this));
          // This request is made only first time. Subsequent requests are fetched from
          // cache and not actual backend calls are made unless we force it.
          return mySettings.get('pluginTemplates');
        }.bind(this))
        .then(
          function success(res) {
            if (!angular.isObject(res)) {
              return;
            }
            if (!res || !res[$state.params.namespace]) {
              return;
            }
            var templates = res[$state.params.namespace][MyAppDAGService.metadata.template.type];
            if (!templates) {
              return;
            }

            this.plugins.items = this.plugins.items.concat(
              objectToArray(templates[GLOBALS.pluginTypes[templateType][group.name]])
            );
          }.bind(this),
          function error() {
            console.log('ERROR: fetching plugin templates');
          }
        );

    };

    this.onLeftSidePanelItemClicked = function(event, item) {
      if (item.type === 'source' && this.pluginTypes[0].error) {
        delete this.pluginTypes[0].error;
      } else if (item.type === 'sink' && this.pluginTypes[2].error) {
        delete this.pluginTypes[2].error;
      }

      // TODO: Better UUID?
      var id = item.name + '-' + item.type + '-' + Date.now();
      event.stopPropagation();

      var config;

      if (item.pluginTemplate) {
        config = {
          id: id,
          name: item.pluginName,
          icon: MyDAGFactory.getIcon(item.pluginName),
          type: item.pluginType,
          properties: item.properties,
          outputSchema: item.outputSchema,
          pluginTemplate: item.pluginTemplate,
          lock: item.lock
        };
      } else {
        config = {
          id: id,
          name: item.name,
          icon: item.icon,
          description: item.description,
          type: item.type
        };
      }

      MyAppDAGService.addNodes(config, config.type, true);
    };

    function objectToArray(obj) {
      var arr = [];

      angular.forEach(obj, function (val) {
        if (val.templateType === MyAppDAGService.metadata.template.type) {
          val.icon = 'fa-plug';
          val.name = val.pluginTemplate;

          arr.push(val);
        }
      });

      return arr;
    }
  });
