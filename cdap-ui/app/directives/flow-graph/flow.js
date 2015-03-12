var module = angular.module(PKG.name+'.commons');

var baseDirective = {
  restrict: 'E',
  // replace: true,
  templateUrl: 'flow-graph/flow.html',
  scope: {
    model: '='
  },
  controller: 'myFlowController'
};

module.factory('d3', function ($window) {
  return $window.d3;
});

module.factory('dagreD3', function ($window) {
  return $window.dagreD3;
});

module.controller('myFlowController', function($scope, d3, dagreD3, $state) {
  function update(newVal, oldVal) {
    if (angular.isObject(newVal) && Object.keys(newVal).length) {
      $scope.render();
    }
  }

  $scope.$watch('model', update);
  $scope.$watchCollection('model.metrics', update);
});

module.directive('myFlowGraph', function ($filter) {
  return angular.extend({
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

        var streamDiagramWidth = 40;
        var streamDiagramHeight = 30;
        var instanceBufferHeight = 30;

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
          if (node.type === 'STREAM') {
            g.setNode(node.name, { shape: 'stream', label: nodeLabel});

          } else {
            g.setNode(node.name, { shape: 'flowlet', label: nodeLabel});
          }
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
            .attr('cx', flowletCircleRadius - instanceCircleScaled)
            .attr('cy', -instanceBufferHeight)
            .attr('r', instanceCircleScaled)
            .attr('class', 'flow-shapes flowlet-instances');

          parent.insert('text')
            .attr('x', flowletCircleRadius - instanceCircleScaled)
            .attr('y', -instanceBufferHeight + metricCountPadding)
            .text(instances)
            .attr('class', 'flow-shapes flowlet-instance-count');

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

        // Draw the stream shape.
        renderer.shapes().stream = function(parent, bbox, node) {
          var w = bbox.width,
          h = bbox.height,
          points = [
            { x:   -streamDiagramWidth, y: streamDiagramHeight}, //e
            { x:   -streamDiagramWidth, y: -h - streamDiagramHeight}, //a
            { x:   w/2, y: -h - streamDiagramHeight}, //b
            { x: w, y: -h/2}, //c
            { x: w/2, y: streamDiagramHeight} //d
          ],
          shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(d) { return d.x + ',' + d.y; }).join(' '))
            .attr('transform', 'translate(' + (-w/8) + ',' + (h * 1/2) + ')')
            .attr('class', 'flow-shapes foundation-shape stream-svg');

          // Elements are positioned with respect to shapeSvg.
          var width = shapeSvg.node().getBBox().width;
          var circleXPos = -1 * width/2;

          parent.append('circle')
            .attr('cx', circleXPos)
            .attr('r', metricCircleRadius)
            .attr('class', 'flow-shapes stream-events');

          parent.append('text')
            .attr('x', circleXPos)
            .attr('y', metricCountPadding)
            .text(numberFilter(scope.model.metrics[labelMap[node.label].name]))
            .attr('class', 'flow-shapes stream-event-count');

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
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
         * Radius for instances circle in flowlets. This is a determined as a factor of the size of the
         * instances text.
         */
        function getInstancesScaledRadius(instances, radius) {
          var base = radius;
          var extra = (instances.toString().length - 1) * base / 2;
          return base + extra;
        }

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
          if (instance.type === 'STREAM') {
            $state.go('flows.detail.runs.tabs.status.streamsDetail', {streamId: nodeId});
          } else {
            $state.go('flows.detail.runs.tabs.status.flowletsDetail', {flowletId: nodeId});
          }

        }

        /**
         * Gets number of instances from node map.
         */
        function getInstances(nodeId) {
          return instanceMap[nodeId].instances ? instanceMap[nodeId].instances : 0;
        }

        /**
         * Handles showing tooltip on mouseover of node name.
         */
        function handleShowTip(nodeId) {
          tip
            .html(function(d) {
              return '<strong>' + instanceMap[nodeId].type +':</strong> <span class="tip-node-name">'+ nodeId +'</span>';
            })
            .show();
        }

        /**
         * Handles hiding tooltip on mouseout of node name.
         */
        function handleHideTip(nodeId) {
          tip.hide();
        }
      };
    }
  }, baseDirective);
});

module.directive('myWorkflowGraph', function ($filter) {
  return angular.extend({
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

        var streamDiagramWidth = 40;
        var streamDiagramHeight = 30;
        var instanceBufferHeight = 30;

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
          if (node.type === 'ACTION') {
            g.setNode(node.name, { shape: 'job', label: nodeLabel});
          } else {
            g.setNode(node.name, { shape: 'conditional', label: nodeLabel});
          }
        });

        angular.forEach(edges, function (edge) {
          g.setEdge(edge.sourceName, edge.targetName);
        });

        renderer.shapes().job = function(parent, bbox, node) {
          var instances = getInstances(node.elem.__data__); // No other way to get name from node.
          var instanceCircleScaled = getInstancesScaledRadius(instances, instanceCircleRadius);
          var shapeSvg = parent.insert('circle', ':first-child')
            .attr('x', -bbox.width / 2)
            .attr('y', -bbox.height / 2)
            .attr('r', flowletCircleRadius)
            .attr('class', 'workflow-shapes foundation-shape job-svg');

          node.intersect = function(point) {
            return dagreD3.intersect.circle(node, flowletCircleRadius, point);
          };

          return shapeSvg;
        };

        renderer.shapes().conditional = function(parent, bbox, node) {
          var instances = getInstances(node.elem.__data__); // No other way to get name from node.
          var instanceCircleScaled = getInstancesScaledRadius(instances, instanceCircleRadius);
          var shapeSvg = parent.insert('circle', ':first-child')
            .attr('x', -bbox.width / 2)
            .attr('y', -bbox.height / 2)
            .attr('r', flowletCircleRadius)
            .attr('class', 'workflow-shapes foundation-shape conditional-svg');

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
         * Radius for instances circle in flowlets. This is a determined as a factor of the size of the
         * instances text.
         */
        function getInstancesScaledRadius(instances, radius) {
          var base = radius;
          var extra = (instances.toString().length - 1) * base / 2;
          return base + extra;
        }

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
          // if (instance.type === 'STREAM') {
          //   $state.go('flows.detail.runs.tabs.status.streamsDetail', {streamId: nodeId});
          // } else {
            $state.go('flows.detail.runs.tabs.status.flowletsDetail', {flowletId: nodeId});
          // }

        }

        /**
         * Gets number of instances from node map.
         */
        function getInstances(nodeId) {
          return instanceMap[nodeId].instances ? instanceMap[nodeId].instances : 0;
        }

        /**
         * Handles showing tooltip on mouseover of node name.
         */
        function handleShowTip(nodeId) {
          tip
            .html(function(d) {
              return '<strong>' + instanceMap[nodeId].type +':</strong> <span class="tip-node-name">'+ nodeId +'</span>';
            })
            .show();
        }

        /**
         * Handles hiding tooltip on mouseout of node name.
         */
        function handleHideTip(nodeId) {
          tip.hide();
        }
      };
    }
  }, baseDirective);
});
