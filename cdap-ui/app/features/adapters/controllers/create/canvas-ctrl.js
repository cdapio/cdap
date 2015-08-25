angular.module(PKG.name + '.feature.adapters')
  .controller('CanvasController', function (myAdapterApi, MyPlumbService, $bootstrapModal, $state, $scope, $alert, CanvasFactory, MyPlumbFactory, $modalStack, $timeout, ModalConfirm, myAdapterTemplatesApi, $q, mySettings, EventPipe) {

    this.nodes = [];
    this.reloadDAG = false;
    if ($scope.AdapterCreateController.data) {
      this.reloadDAG = true;
      setNodesAndConnectionsFromDraft.call(this, $scope.AdapterCreateController.data);
    }

    this.onImportSuccess = function(result) {
      // EventPipe.emit('popovers.close');
      EventPipe.emit('popovers.reset');
      $scope.config = JSON.stringify(result);
      this.reloadDAG = true;
      MyPlumbService.resetToDefaults(true);
      setNodesAndConnectionsFromDraft.call(this, result);
      if ($scope.config.name) {
        MyPlumbService.metadata.name = $scope.config.name;
      }

      MyPlumbService.notifyError({});
      MyPlumbService.notifyResetListners();
    };

    this.importFile = function(files) {
      CanvasFactory
        .importAdapter(files, MyPlumbService.metadata.template.type)
        .then(
          this.onImportSuccess.bind(this),
          function error(errorEvent) {
            console.error('Upload config failed', errorEvent);
          }
        );
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
