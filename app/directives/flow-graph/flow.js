var module = angular.module(PKG.name+'.commons');

module.factory('d3', function ($window) {
  return $window.d3;
});

module.factory('dagreD3', function ($window) {
  return $window.dagreD3;
});


module.directive('myFlowGraph', function (d3, dagreD3, $state) {
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

        // Add our custom shape (a flowlet)
        renderer.shapes().flowlet = function(parent, bbox, node) {
          var w = bbox.width,
              h = bbox.height,
              shapeSvg = parent.insert("circle")
               .attr("cx", 30)
               .attr("cy", -25)
               .attr("r", 60)
               .attr("transform", "translate(" + (-w/2) + "," + (h * 3/4) + ")")
               .attr("class", "flowlet-svg");
          
          node.intersect = function(point) {
            return dagreD3.intersect.circle(node, 60, {x: point.x, y: point.y});
          };


          return shapeSvg;
        };

        renderer.shapes().stream = function(parent, bbox, node) {
          console.log(bbox);
          var w = bbox.width,
              h = bbox.height,
              points = [
                { x:   0, y: 20}, //e
                { x:   0, y: -h - 20}, //a
                { x:   w/2, y: -h - 20}, //b
                { x: w, y: -h/2 - 10}, //c
                { x: w/2, y: 20}, //d
              ];
              shapeSvg = parent.insert("polygon", ":first-child")
                .attr("points", points.map(function(d) { return d.x + "," + d.y; }).join(" "))
                .attr("transform", "translate(" + (-w/2) + "," + (h * 3/4) + ")")
                .attr("class", "stream-svg");

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        angular.forEach(nodes, function (node) {
          var nodeLabel = node.length > 8 ? node.substr(0, 5) + '...' : node;
          if (node === "wordStream") {
            g.setNode(node, { shape: "stream", label: nodeLabel});
          } else {
            g.setNode(node, { shape: "flowlet", label: nodeLabel});
          }
          
          
        });

        angular.forEach(edges, function (edge) {
          g.setEdge(edge.sourceName, edge.targetName);
        });
        renderer(inner, g);
        
        // Set up onclick after rendering.
        inner
          .selectAll("g.node")
          .on("click", function(nodeId) {
            console.log(nodeId);
             $state.go('flows.detail.runs.detail.flowlets.detail', {flowletId: nodeId});
          });
      };

    }
  };
});
