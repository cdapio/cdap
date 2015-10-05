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
  .controller('LeftPanelController', function(myPipelineApi, MyAppDAGService, MyDAGFactory, mySettings, $state, MySidebarService, $scope, rVersion, $stateParams, GLOBALS) {
    this.pluginTypes = [
      {
        name: 'source',
        expanded: false,
        plugins: []
      },
      {
        name: 'transform',
        expanded: false,
        plugins: []
      },
      {
        name: 'sink',
        expanded: false,
        plugins: []
      }
    ];

    angular.forEach(this.pluginTypes, function (group) {
      var templateType = MyAppDAGService.metadata.template.type;
      var params = {
        namespace: $stateParams.namespace,
        pipelineType: templateType,
        version: rVersion.version,
        extensionType: GLOBALS.pluginTypes[templateType][group.name],
        scope: $scope
      };

      myPipelineApi.fetchPlugins(params)
        .$promise
        .then(function (res) {
          var plugins = res.map(function (p) {
            p.type = params.extensionType;
            p.icon = MyDAGFactory.getIcon(p.name);
            return p;
          });
          group.plugins = group.plugins.concat(plugins);
        });
    });

    mySettings.get('pluginTemplates')
      .then(function (res) {
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

        var templateType = MyAppDAGService.metadata.template.type;

        angular.forEach(GLOBALS.pluginTypes[templateType], function (value, key) {
          var group;

          switch (key) {
            case 'source':
              group = this.pluginTypes[0];
              break;
            case 'transform':
              group = this.pluginTypes[1];
              break;
            case 'sink':
              group = this.pluginTypes[2];
              break;
          }

          group.plugins = group.plugins.concat(objectToArray(templates[value]));

        }.bind(this));
      }.bind(this));


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
          val.icon = MyDAGFactory.getIcon(val.pluginName);
          val.name = val.pluginTemplate;

          arr.push(val);
        }
      });

      return arr;
    }
  });
