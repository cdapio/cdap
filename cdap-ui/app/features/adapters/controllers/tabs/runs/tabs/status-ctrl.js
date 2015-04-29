angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterRunDetailStatusController', function($scope, MyDataSource, $state, $timeout) {
    var dataSrc = new MyDataSource($scope);
    $scope.isDraggable = true;
    $scope.transforms = [{
      name: '',
      properties: {}
    }];
    $scope.source = {
      name: '',
      properties: {}
    };
    $scope.sink = {
      name: '',
      properties: {}
    };

    dataSrc.request({
      _cdapNsPath: '/adapters/' + $state.params.adapterId
    })
      .then(function(res) {
        $scope.source = res.config.source;
        $scope.sink = res.config.sink;
        $scope.transforms = res.config.transforms || [];

        instance.connect({
          source:'sourceWrapper',
          target:'transformsContainer',
          anchors: ['Right', 'Left']
        });

        $timeout(function() {
          for (var i=0; i<$scope.transforms.length-1; i++) {
            instance.connect({
              source: 'transformWrapper'+i,
              target: 'transformWrapper'+(i+1),
              anchors: ['Bottom', 'Top']
            })
          }

          instance.connect({
            source: 'transformWrapper'+i,
            target: 'sinkWrapper',
            anchors: ['Right', 'Left']
          })

        });
      });
  jsPlumb.bind('ready', function() {});
  var instance = jsPlumb.getInstance({
    Endpoint: ["Dot", {radius: 2}],
    Connector: ['Straight', {}],
    ConnectionOverlays: [
       [ "Arrow", {
           location: 1,
           id: "arrow",
           length: 10,
           foldback: 0.8
       } ],
       // Should be later used to add metrics.
      //  [ "Label", { label: "FOO", id: "label", cssClass: "aLabel" }]
    ]
  });
  instance.setContainer('etlFlowContainer');
});
