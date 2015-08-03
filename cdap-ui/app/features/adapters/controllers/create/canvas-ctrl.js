angular.module(PKG.name + '.feature.adapters')
  .controller('CanvasController', function (myAdapterApi, MyPlumbService, $bootstrapModal, $state, $scope, $alert, myHelpers, CanvasFactory, MyPlumbFactory, $modalStack, $timeout, ModalConfirm) {
    this.nodes = [];
    this.reloadDAG = false;
    if ($scope.AdapterCreateController.data) {
      this.reloadDAG = true;
      setNodesAndConnectionsFromDraft.call(this, $scope.AdapterCreateController.data);
    }

    this.pluginTypes = [
      {
        name: 'source',
        icon: 'icon-ETLsources'
      },
      {
        name: 'transform',
        icon: 'icon-ETLtransforms'
      },
      {
        name: 'sink',
        icon: 'icon-ETLsinks'
      }
    ];

    this.canvasOperations = [
      {
        name: 'Settings',
        icon: 'fa fa-sliders'
      },
      {
        name: 'Publish',
        icon: 'fa fa-cloud-upload'
      },
      {
        name: 'Save Draft',
        icon: 'fa fa-save'
      },
      {
        name: 'Config',
        icon: 'fa fa-eye'
      },
      {
        name: 'Export',
        icon: 'fa fa-download'
      },
      {
        name: 'Import',
        icon: 'fa fa-upload'
      }
    ];

    // Utterly naive. We need to be more efficient and this code should look better.
    // Pushing for functionality for now. Will revisit this back.
    this.importFile = function(files) {
      var reader = new FileReader();
      var config = MyPlumbService.getConfigForBackend();
      reader.readAsText(files[0], "UTF-8");
      reader.onload = function (evt) {
        var result;
        try {
          result = JSON.parse(evt.target.result);
        } catch(e) {
          result = null;
          $alert({
            type: 'danger',
            content: 'The imported config json is incorrect. Please check the JSON content'
          });
          return;
        }

        if (result.template !== MyPlumbService.metadata.template.type) {
          $alert({
            type: 'danger',
            content: 'Template imported is for ' + result.template + '. Please switch to ' + result.template + ' creation to import.'
          });
          return;
        }
        // We need to perform more validations on the uploaded json.
        if (!result.config.source ||
            !result.config.sink ||
            !result.config.transforms) {
          $alert({
            type: 'danger',
            content: 'The structure of imported config is incorrect. To the base structure of the config please try creating a new adpater and viewing the config.'
          });
          return;
        }
        $scope.config = JSON.stringify(result);
        this.reloadDAG = true;
        MyPlumbService.resetToDefaults(true);
        setNodesAndConnectionsFromDraft.call(this, result);
        if ($scope.config.name) {
          MyPlumbService.metadata.name = $scope.config.name;
        }

        MyPlumbService.notifyError({});
        MyPlumbService.notifyResetListners();
      }.bind(this)
      reader.onerror = function (evt) {
        console.error('Upload config failed', evt);
      };
    };

    this.onCanvasOperationsClicked = function(group) {
      var config;
      switch(group.name) {
        case 'Export':
          var detailedConfig = MyPlumbService.getConfigForBackend();
          if (!MyPlumbService.metadata.name || MyPlumbService.metadata.name === '') {
            detailedConfig.name = 'noname';
          } else {
            detailedConfig.name =  MyPlumbService.metadata.name;
          }

          detailedConfig.ui = {
            nodes: MyPlumbService.nodes,
            connections: MyPlumbService.connections
          };
          var content = JSON.stringify(detailedConfig, null, 4);
          var blob = new Blob([content], { type: 'application/json'});
          this.exportFileName = detailedConfig.name + '-' + detailedConfig.template;
          this.url = URL.createObjectURL(blob);

          $scope.$on('$destroy', function () {
            URL.revokeObjectURL(this.url);
          });
          // Clicking on the hidden download button. #hack.
          $timeout(function() {
            document.getElementById('adapter-export-config-link').click();
          });
          break;
        case 'Import':
          // Clicking on the hidden upload button. #hack.
          $timeout(function() {
            document.getElementById('adapter-import-config-link').click();
          });
          break;
        case 'Config':
          config = angular.copy(MyPlumbService.getConfigForBackend());
          modalInstance = $bootstrapModal.open({
            templateUrl: '/assets/features/adapters/templates/create/viewconfig.html',
            size: 'lg',
            windowClass: 'adapter-modal',
            keyboard: true,
            controller: ['$scope', 'config', '$timeout', function($scope, config, $timeout) {
              $scope.config = JSON.stringify(config);
            }],
            resolve: {
              config: function() {
                return config;
              }
            }
          });
          break;
        case 'Publish':
          MyPlumbService
            .save()
            .then(
              function sucess(adapter) {
                $alert({
                  type: 'success',
                  content: adapter + ' successfully published.'
                });
                $state.go('apps.list');
              },
              function error(errorObj) {
                console.info('ERROR!: ', errorObj);
              }.bind(this)
            );
          break;
        case 'Settings':
          $bootstrapModal.open({
            templateUrl: '/assets/features/adapters/templates/create/settings.html',
            size: 'lg',
            windowClass: 'adapter-modal',
            keyboard: true,
            controller: ['$scope', 'metadata', 'EventPipe', function($scope, metadata, EventPipe) {
              $scope.metadata = metadata;
              var metadataCopy = angular.copy(metadata);
              $scope.reset = function() {
                $scope.metadata.template.schedule.cron = metadataCopy.template.schedule.cron;
                $scope.metadata.template.instance = metadataCopy.template.instance;
                EventPipe.emit('plugin.reset');
              };

              function closeFn() {
                $scope.reset();
                $scope.$close('cancel');
              }

              ModalConfirm.confirmModalAdapter(
                $scope,
                $scope.metadata,
                metadataCopy,
                closeFn
              );

            }],
            resolve: {
              'metadata': function() {
                return MyPlumbService.metadata;
              }
            }
          });
          break;
        case 'Save Draft':
          MyPlumbService
            .saveAsDraft()
            .then(
              function success() {
                $alert({
                  type: 'success',
                  content: MyPlumbService.metadata.name + ' successfully saved as draft.'
                });
                $state.go('adapters.list');
              },
              function error(message) {
                console.info('Failed saving as draft');
              }
            )
      }
    };

    this.plugins= {
      items: []
    };

    this.onPluginTypesClicked = function(group) {
      var prom;
      switch(group.name) {
        case 'source':
          prom = myAdapterApi.fetchSources({ adapterType: MyPlumbService.metadata.template.type }).$promise;
          break;
        case 'transform':
          prom = myAdapterApi.fetchTransforms({ adapterType: MyPlumbService.metadata.template.type }).$promise;
          break;
        case 'sink':
          prom = myAdapterApi.fetchSinks({ adapterType: MyPlumbService.metadata.template.type }).$promise;
          break;
      }
      prom.then(function(res) {
        this.plugins.items = [];
        res.forEach(function(plugin) {
          this.plugins.items.push(
            angular.extend(
              {
                type: group.name,
                icon: MyPlumbFactory.getIcon(plugin.name)
              },
              plugin
            )
          );
        }.bind(this));
      }.bind(this));
    };

    this.onPluginItemClicked = function(event, item) {
      if (item.type === 'source' && this.pluginTypes[0].error) {
        delete this.pluginTypes[0].error;
      } else if (item.type === 'sink' && this.pluginTypes[2].error) {
        delete this.pluginTypes[2].error;
      }

      // TODO: Better UUID?
      var id = item.name + '-' + item.type + '-' + Date.now();
      event.stopPropagation();
      var config = {
        id: id,
        name: item.name,
        icon: item.icon,
        description: item.description,
        type: item.type
      };
      MyPlumbService.addNodes(config, config.type, true);
    };

    function errorNotification(errors) {
      angular.forEach(this.pluginTypes, function (type) {
        delete type.error;
        if (errors[type.name]) {
          type.error = errors[type.name];
        }
      });
    }

    MyPlumbService.errorCallback(errorNotification.bind(this));

    function setNodesAndConnectionsFromDraft(data) {
      var ui = data.ui;
      var config = data.config;
      var nodes;
      var config1 = CanvasFactory.extractMetadataFromDraft(data.config, data);

      if (config1.name) {
        MyPlumbService.metadata.name = config1.name;
      }
      MyPlumbService.metadata.description = config1.description;
      MyPlumbService.metadata.template = config1.template;

      // Purely for feeding my-plumb to draw the diagram
      // if I already have the nodes and connections
      if (ui && ui.nodes) {
        nodes = ui.nodes;
        while(this.nodes.length) {
          this.nodes.pop();
        }
        angular.forEach(nodes, function(value) {
          this.nodes.push(value);
        }.bind(this));
      } else {
        this.nodes = CanvasFactory.getNodes(config);
      }
      this.nodes.forEach(function(node) {
        MyPlumbService.addNodes(node, node.type);
      });

      if (ui && ui.connections) {
        MyPlumbService.connections = ui.connections;
      } else {
        MyPlumbService.connections = CanvasFactory.getConnectionsBasedOnNodes(this.nodes);
      }
    }

    $scope.$on('$destroy', function() {
      $modalStack.dismissAll();
    });

  });
