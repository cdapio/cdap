var module = angular.module(PKG.name+'.commons');

module.factory('d3', function ($window) {
  return $window.d3;
});

module.factory('dagreD3', function ($window) {
  return $window.dagreD3;
});

module.directive('myWorkflowGraph', function (d3, dagreD3, $state, $filter) {
  return {
    restrict: 'E',
    templateUrl: 'flow-graph/flow.html',
    scope: {
      model: '='
    },
    controller: function($scope) {

      function update(newVal, oldVal) {
        if (angular.isObject(newVal) && Object.keys(newVal).length) {
          $scope.render();
        }
      }

      $scope.$watch('model', update);
      $scope.$watchCollection('model.metrics', update);

    },
    link: function (scope, elem, attr) {

      scope.render = function () {
        var nodes = scope.model.nodes;
        var edges = scope.model.edges;
        var instanceMap = {};
        var labelMap = {};

        var numberFilter = $filter('myNumber');

        var renderer = new dagreD3.render();
        var g = new dagreD3.graphlib.Graph();
        var metricCircleRadius = 25;
        var instanceCircleRadius = 10;
        var flowletCircleRadius = 60;
        // Since names are padded inside of shapes, this needs the same padding to be vertically center aligned.
        var metricCountPadding = 5;

        g.setGraph({
          nodesep: 60,
          ranksep: 70,
          rankdir: 'LR',
          marginx: 30,
          marginy: 30
        })
          .setDefaultEdgeLabel(function () { return {}; });

        // First set nodes and edges.
        angular.forEach(nodes, function (node) {
          var nodeLabel = node.name.length > 8 ? node.name.substr(0, 5) + '...' : node.name;
          instanceMap[node.name] = node;
          labelMap[nodeLabel] = node;
          g.setNode(node.name, { shape: 'flowlet', label: nodeLabel});
        });

        angular.forEach(edges, function (edge) {
          g.setEdge(edge.sourceName, edge.targetName);
        });

        // Draw the flowlet shape.
        renderer.shapes().flowlet = function(parent, bbox, node) {
          var instances = getInstances(node.elem.__data__); // No other way to get name from node.
          var instanceCircleScaled = getInstancesScaledRadius(instances, instanceCircleRadius);
          var shapeSvg = parent.insert('circle', ':first-child')
            .attr('x', -bbox.width / 2)
            .attr('y', -bbox.height / 2)
            .attr('r', flowletCircleRadius)
            .attr('class', 'flow-shapes foundation-shape flowlet-svg');

          // Elements are positioned with respect to shapeSvg.
          parent.insert('circle')
            .attr('cx', - flowletCircleRadius)
            .attr('r', metricCircleRadius)
            .attr('class', 'flow-shapes flowlet-events');

          parent.insert('text')
            .attr('x', - flowletCircleRadius)
            .attr('y', metricCountPadding)
            .text(numberFilter(scope.model.metrics[labelMap[node.label].name]))
            .attr('class', 'flow5shapes flowlet-event-count');

          node.intersect = function(point) {
            return dagreD3.intersect.circle(node, flowletCircleRadius, point);
          };

          return shapeSvg;
        };

        // Set up an SVG group so that we can translate the final graph and tooltip.
        var svg = d3.select('svg').attr('fill', 'white');
        var svgGroup = d3.select('svg g');
        var tip = d3.tip()
          .attr('class', 'd3-tip')
          .offset([-10, 0]);
        svg.call(tip);

        // Set up zoom support
        var zoom = d3.behavior.zoom().on('zoom', function() {
          svgGroup.attr('transform', 'translate(' + d3.event.translate + ')' +
                                      'scale(' + d3.event.scale + ')');
        });
        // svg.call(zoom);

        // Run the renderer. This is what draws the final graph.
        renderer(d3.select('svg g'), g);

        // Set up onclick after rendering.
        svg
          .selectAll('g.node')
          .on('click', handleNodeClick);

        svg
          .selectAll('g.node text')
          .on('mouseover', handleShowTip)
          .on('mouseout', handleHideTip);


        // Center svg.
        var initialScale = 1.1;
        var svgWidth = svg.node().getBoundingClientRect().width;
        zoom
          .translate([(svgWidth - g.graph().width * initialScale) / 2, 20])
          .scale(initialScale)
          .event(svg);
        svg.attr('height', g.graph().height * initialScale + 40);

        /**
         * Handles node click and sends to flowlet page.
         */
        function handleNodeClick(nodeId) {
          // Temporary fix for 2.8.0. Should be removed first thing post 2.8.
          if ($state.includes('**.workflows.**')) {
            return;
          }
          handleHideTip(nodeId);
          var instance = instanceMap[nodeId];
          $state.go('flows.detail.runs.tabs.status.flowletsDetail', {flowletId: nodeId});

        }

    }
  };
});
