var module = angular.module(PKG.name+'.commons');

module.directive('myFlowGraph', function () {
  return {
    restrict: 'E',
    templateUrl: 'flow-graph/flow.html',
    scope: {
      model: '='
    },
    link: function (scope, elem, attr) {

      scope.$watch('model', function(newVal, oldVal) {
        if (angular.isObject(newVal) && Object.keys(newVal).length) {
          scope.render();
        }
      });

      scope.render = function (){
        var nodes = scope.model.nodes;
        var edges = scope.model.edges;

        var inner = d3.select('g');

        var renderer = new dagreD3.render();
        var g = new dagreD3.graphlib.Graph();

        g.setGraph({
          nodesep: 70,
          ranksep: 30,
          rankdir: 'LR',
          marginx: 30,
          marginy: 30
        })
          .setDefaultEdgeLabel(function () { return {}; });

        angular.forEach(nodes, function (node) {
          g.setNode(node, { label: node });
        });

        angular.forEach(edges, function (edge) {
          g.setEdge(edge.sourceName, edge.targetName);
        });

        renderer(inner, g);
      }

    }
  };
});
