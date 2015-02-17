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

        var renderer = new dagreD3.render();
        var g = new dagreD3.graphlib.Graph();

        g.setGraph({
          nodesep: 60,
          ranksep: 70,
          rankdir: 'LR',
          marginx: 30,
          marginy: 30
        })
          .setDefaultEdgeLabel(function () { return {}; });

        
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


        renderer.shapes().flowlet = function(parent, bbox, node) {
          var r = 60,
          shapeSvg = parent.insert("circle", ":first-child")
            .attr("x", -bbox.width / 2)
            .attr("y", -bbox.height / 2)
            .attr("r", r)
            .attr("class", "flow-shapes foundation-shape flowlet-svg");
          
          parent.insert("circle")
            .attr("cx", 50)
            .attr("cy", -30)
            .attr("r", 10)
            .attr("class", "flow-shapes flowlet-instances");
          
          parent.insert("text")
            .attr("x", 46)
            .attr("y", -26)
            .text('1')
            .attr("class", "flow-shapes flowlet-instance-count");

          parent.insert("circle")
            .attr("cx", -58)
            .attr("cy", 0)
            .attr("r", 25)
            .attr("class", "flow-shapes flowlet-events");
          parent.insert("text")
            .attr("x", -62)
            .attr("y", 5)
            .text('1')
            .attr("class", "flow-shapes flowlet-event-count");
          
          node.intersect = function(point) {
            return dagreD3.intersect.circle(node, r, point);
          };

          return shapeSvg;
        };

        renderer.shapes().stream = function(parent, bbox, node) {
          var w = bbox.width,
          h = bbox.height,
          points = [
            { x:   -40, y: 30}, //e
            { x:   -40, y: -h - 30}, //a
            { x:   w/2, y: -h - 30}, //b
            { x: w, y: -h/2}, //c
            { x: w/2, y: 30}, //d
          ];
          shapeSvg = parent.insert("polygon", ":first-child")
            .attr("points", points.map(function(d) { return d.x + "," + d.y; }).join(" "))
            .attr("transform", "translate(" + (-w/8) + "," + (h * 1/2) + ")")
            .attr("class", "flow-shapes foundation-shape stream-svg");

          parent.insert("circle")
            .attr("cx", -58)
            .attr("cy", 0)
            .attr("r", 25)
            .attr("class", "flow-shapes stream-events");
          
          parent.insert("text")
            .attr("x", -62)
            .attr("y", 5)
            .text('1')
            .attr("class", "flow-shapes stream-event-count");

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        // Set up an SVG group so that we can translate the final graph.
        var svg = d3.select("svg").attr("fill", "white");
        var svgGroup = d3.select("svg g");

        // Set up zoom support
        var zoom = d3.behavior.zoom().on("zoom", function() {
          svgGroup.attr("transform", "translate(" + d3.event.translate + ")" +
                                      "scale(" + d3.event.scale + ")");
        });
        svg.call(zoom);

        // Run the renderer. This is what draws the final graph.
        renderer(d3.select("svg g"), g);
        
        // Set up onclick after rendering.
        svg
          .selectAll("g.node .foundation-shape")
          .on("click", function(nodeId) {
            console.log(nodeId);
             $state.go('flows.detail.runs.detail.flowlets.detail', {flowletId: nodeId});
          });

        var initialScale = 1.1;
        var svgWidth = svg.node().getBoundingClientRect().width;
        zoom
          .translate([(svgWidth - g.graph().width * initialScale) / 2, 20])
          .scale(initialScale)
          .event(svg);
        svg.attr('height', g.graph().height * initialScale + 40);
        
      };

    }
  };
});
