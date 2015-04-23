var module = angular.module(PKG.name+'.commons');

var baseDirective = {
  restrict: 'E',
  templateUrl: 'flow-graph/flow.html',
  scope: {
    model: '=',
    click: '&'
  },
  controller: 'myFlowController'
};

module.factory('d3', function ($window) {
  return $window.d3;
});

module.factory('dagreD3', function ($window) {
  return $window.dagreD3;
});

module.controller('myFlowController', function($scope, d3, dagreD3) {
  function update(newVal, oldVal) {
    if (angular.isObject(newVal) && Object.keys(newVal).length) {
      $scope.render();
    }
  }

  $scope.instanceMap = {};
  $scope.labelMap = {};

  /**
   * Gets number of instances from node map.
   */
  $scope.getInstances = function(nodeId) {
    return $scope.instanceMap[nodeId].instances ? $scope.instanceMap[nodeId].instances : 0;
  };

  $scope.$watch('model', update);
  $scope.$watchCollection('model.metrics', update);
});

module.directive('myFlowGraph', function ($filter, $state, $alert, myStreamService) {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.render = genericRender.bind(null, scope, $filter);
      scope.parentSelector = attr.parent;
      /**
       * Circle radius for instance count.
       * @type {Number}
       */
      var instanceCircleRadius = 10;
      /**
       * Circle radius for flowlets.
       */
      var flowletCircleRadius = 50;

      // Since names are padded inside of shapes, this needs the same padding to be vertically center aligned.
      /**
       * Inside padding for metric count.
       */
      var metricCountPadding = 5;
      /**
       * Width of stream diagram.
       */
      var streamDiagramWidth = 40;
      /**
       * Height of stream diagram.
       */
      var streamDiagramHeight = 30;
      /**
       * Number of pixes instance should display above base.
       */
      var instanceBufferHeight = 30;

      /**
       * Leaf node variables.
       */
      /**
       * Width of the leaf diagram relative to flowlet circle radius.
       */
      var leafDiagramWidth = flowletCircleRadius * 0.75;
      /**
       * Shrinks or expands height of leaf shape.
       */
      var leafYFactor = 0.9;
      /**
       * Shrinks or expands width of leaf shape.
       */
      var leafXFactor = 1.25;
      /**
       * Overflow of leaf into the flowlet/stream shape.
       */
      var leafBuffer = 20;

      var numberFilter = $filter('myNumber');
      scope.getShapes = function() {
        var shapes = {};
        shapes.flowlet = function(parent, bbox, node) {
          var w = bbox.width;
          var h = bbox.height;
          var points = [
            //clockwise points from top
            { x: -streamDiagramWidth, y: streamDiagramHeight}, //e
            { x: -streamDiagramWidth, y: -h - streamDiagramHeight}, //a
            { x: w/2, y: -h - streamDiagramHeight}, //b
            { x: w, y: -h/2}, //c
            { x: w/2, y: streamDiagramHeight} //d
          ];
          var instances = scope.getInstances(node.elem.__data__); // No other way to get name from node.
          var instanceCircleScaled = scope.getInstancesScaledRadius(instances, instanceCircleRadius);
          var shapeSvg = parent.insert('circle', ':first-child')
            .attr('x', -bbox.width / 2)
            .attr('y', -bbox.height / 2)
            .attr('r', flowletCircleRadius)
            .attr('class', 'flow-shapes foundation-shape flowlet-svg');

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

          var leafOptions = {
            classNames: ['flowlet-events'],
            circleRadius: flowletCircleRadius,
            diagramWidth: leafDiagramWidth
          };
          drawLeafShape(parent, leafOptions);
          
          parent.insert('text')
            .attr('x', calculateLeafBuffer(parent, leafOptions))
            .attr('y', metricCountPadding)
            .text(numberFilter(scope.model.metrics[scope.labelMap[node.label].name]))
            .attr('class', 'flow-shapes flowlet-event-count');

          node.intersect = function(point) {
            return dagreD3.intersect.circle(node, flowletCircleRadius, point);
          };

          return shapeSvg;
        };

        shapes.stream = function(parent, bbox, node) {
          var w = bbox.width,
          h = bbox.height/2,
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
          var width = shapeSvg.node().getBBox().width+10;
          var circleXPos = -1 * width/2;
          
          var leafOptions = {
            classNames: ['stream-events'],
            circleRadius: flowletCircleRadius,
            diagramWidth: leafDiagramWidth
          };
          drawLeafShape(parent, leafOptions);
          
          parent.append('text')
            .attr('x', calculateLeafBuffer(parent, leafOptions))
            .attr('y', metricCountPadding)
            .text(numberFilter(scope.model.metrics[scope.labelMap[node.label].name]))
            .attr('class', 'flow-shapes stream-event-count');

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        return shapes;
      };

      scope.getShape = function(name) {
        var shapeName;

        switch(name) {
          case 'STREAM':
            shapeName = 'stream';
            break;
          default:
            shapeName = 'flowlet';
            break;
        }
        return shapeName;
      };

      scope.handleNodeClick = function(nodeId) {
        scope.handleHideTip(nodeId);
        var instance = scope.instanceMap[nodeId];
        if (instance.type === 'STREAM') {
          myStreamService.show(nodeId);
        } else {
          // $state.go('flows.detail.flowlets.flowlet', { flowletid: nodeId });

          scope.$apply(function(scope) {
            var fn = scope.click();
            if ('undefined' !== typeof fn) {
              fn(nodeId);
            }
          });
        }
      };

      /**
       * Radius for instances circle in flowlets. This is a determined as a factor of the size of the
       * instances text.
       */
      scope.getInstancesScaledRadius = function(instances, radius) {
        var base = radius;
        var extra = (instances.toString().length - 1) * base / 2;
        return base + extra;
      };

      /**
       * Draws a leaf shape and positions it next to the parent svg.
       */
      function drawLeafShape(svgParent, properties) {
        var diagramWidth = leafDiagramWidth;
        var yFactor = leafYFactor;
        var xFactor = leafXFactor;
        var circleRadius = flowletCircleRadius;
        var classNamesStr = 'flow-shapes leaf-shape';
        
        if (properties && Object.prototype.toString.call(properties) === '[object Object]') {
          diagramWidth = properties.diagramWidth || diagramWidth;
          yFactor = properties.yFactor || yFactor;
          xFactor = properties.xFactor || xFactor;
          circleRadius = properties.circleRadius || circleRadius;
          if (angular.isArray(properties.classNames)) {
            var classNames = properties.classNames.join(' ');
            classNamesStr = classNames ? 'flow-shapes leaf-shape ' + classNames : 'flow-shapes leaf-shape';
          }
        }

        var pathinfo = [
          {x: 0, y: 0},
          {x: diagramWidth * xFactor, y: -diagramWidth * yFactor},
          {x: diagramWidth * 2, y: 0},
          {x: diagramWidth * xFactor, y: diagramWidth * yFactor},
          {x: 0, y: 0}
        ];

        var line = d3.svg.line()
          .x(function(d){return d.x;})
          .y(function(d){return d.y;})
          // Must use basis interpolation for curve.
          .interpolate("basis-closed");

        svgParent.insert("svg:path")
          .attr("d", line(pathinfo))
          .attr('class', classNamesStr)
          .attr("transform", function(d) {
            return "translate(" 
              + (- circleRadius + leafBuffer) + ", 0) rotate(-180)";
          });
      }

      /**
       * Calcualtes where event count should be placed relative to leaf shape and centers it.
       */
      function calculateLeafBuffer(parent, nodeOptions) {
        var w = parent.select(".leaf-shape").node().getBBox().width;
        return - nodeOptions.circleRadius - w / 2 + leafBuffer / 2;
      }

    }
  }, baseDirective);
});

module.directive('myWorkflowGraph', function ($filter, $state) {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.render = genericRender.bind(null, scope, $filter);
      var defaultRadius = 50;
      scope.getShapes = function() {
        var shapes = {};
        shapes.job = function(parent, bbox, node) {
          var w = bbox.width;
          var h = bbox.height;
          var points = [
            //clockwise points from top
            { x: -defaultRadius * 2/3, y: -defaultRadius * 2/3}, //a
            { x: 0, y: -defaultRadius}, // b
            { x: defaultRadius * 2/3, y: -defaultRadius * 2/3}, // c
            { x: defaultRadius, y: 0}, // d
            { x: defaultRadius * 2/3, y: defaultRadius * 2/3}, // e
            { x: 0, y: defaultRadius}, // f
            { x: -defaultRadius * 2/3, y: defaultRadius * 2/3}, // g
            { x: -defaultRadius, y: -0}, //h
          ];
          var shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '))
            .attr('class', 'workflow-shapes foundation-shape job-svg');

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        shapes.start = function(parent, bbox, node) {
          var w = bbox.width;
          var h = bbox.height;
          var points = [
            // draw a triangle facing right
            { x: -30, y: -40},
            { x: 30, y: 0},
            { x: -30, y: 40},
          ];
          var shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '))
            .attr('transform', 'translate(' + (w/6) + ')')
            .attr('class', 'workflow-shapes foundation-shape start-svg');

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        shapes.end = function(parent, bbox, node) {
          var w = bbox.width;
          var h = bbox.height;
          var points = [
            // draw a triangle facing right
            { x: -30, y: 0},
            { x: 30, y: 40},
            { x: 30, y: -40},
          ];
          var shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '))
            .attr('transform', 'translate(' + (-w/6) + ')')
            .attr('class', 'workflow-shapes foundation-shape end-svg');

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        shapes.conditional = function(parent, bbox, node) {
          var w = (bbox.width * Math.SQRT2) / 2,
          h = (bbox.height * Math.SQRT2) / 2,
          points = [
            // draw a diamond
            { x:  0, y: -defaultRadius },
            { x: -defaultRadius, y:  0 },
            { x:  0, y:  defaultRadius },
            { x:  defaultRadius, y:  0 }
          ],
          shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '))
            .attr('class', 'workflow-shapes foundation-shape conditional-svg');

          node.intersect = function(p) {
            return dagreD3.intersect.polygon(node, points, p);
          };
          return shapeSvg;
        };

        return shapes;
      };

      scope.getShape = function(name) {
        var shapeName;

        switch(name) {
          case 'ACTION':
            shapeName = 'job';
            break;
          case 'START':
            shapeName = 'start';
            break;
          case 'END':
            shapeName = 'end';
            break;
          default:
            shapeName = 'conditional';
            break;
        }
        return shapeName;
      }

      scope.handleNodeClick = function(nodeId) {
        // Temporary fix for 2.8.0. Should be removed first thing post 2.8.
        if ($state.includes('**.workflows.**')) {
          return;
        }
        scope.handleHideTip(nodeId);
        var instance = scope.instanceMap[nodeId];
        $state.go('flows.detail.runs.tabs.status.flowletsDetail', {flowletId: nodeId});
      }

    }
  }, baseDirective);
});

function genericRender(scope, $filter) {
  var nodes = scope.model.nodes;
  var edges = scope.model.edges;

  var renderer = new dagreD3.render();
  var g = new dagreD3.graphlib.Graph();

  g.setGraph({
    nodesep: 60,
    ranksep: 100,
    rankdir: 'LR',
    marginx: 30,
    marginy: 30
  })
    .setDefaultEdgeLabel(function () { return {}; });

  // First set nodes and edges.
  angular.forEach(nodes, function (node) {
    var nodeLabel = node.name.length > 8 ? node.name.substr(0, 5) + '...' : node.name;
    scope.instanceMap[node.name] = node;
    scope.labelMap[nodeLabel] = node;
    g.setNode(node.name, { shape: scope.getShape(node.type), label: nodeLabel});
  });

  angular.forEach(edges, function (edge) {
    g.setEdge(edge.sourceName, edge.targetName);
  });

  angular.extend(renderer.shapes(), scope.getShapes());
  var selector = '';
  // Making the query to be more specific instead of doing
  // it under the entire DOM. This allows us to draw the diagram
  // in multiple places.
  if (scope.parentSelector) {
    selector += scope.parentSelector;
  }
  selector += ' svg';
  // Set up an SVG group so that we can translate the final graph and tooltip.
  var svg = d3.select(selector).attr('fill', 'white');
  var svgGroup = d3.select(selector + ' g');
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
  renderer(d3.select(selector + ' g'), g);

  // Set up onclick after rendering.
  svg
    .selectAll('g.node')
    .on('click', scope.handleNodeClick);

  svg
    .selectAll('g.node text')
    .on('mouseover', scope.handleShowTip)
    .on('mouseout', scope.handleHideTip);

  scope.$on('$destroy', scope.handleHideTip);

  // Center svg.
  var initialScale = 1.1;
  var svgWidth = svg.node().getBoundingClientRect().width;
  zoom
    .translate([(svgWidth - g.graph().width * initialScale) / 2, 20])
    .scale(initialScale)
    .event(svg);
  svg.attr('height', g.graph().height * initialScale + 40);

  /**
   * Handles showing tooltip on mouseover of node name.
   */
  scope.handleShowTip = function(nodeId) {
    tip
      .html(function(d) {
        return '<strong>' + scope.instanceMap[nodeId].type +':</strong> <span class="tip-node-name">'+ nodeId +'</span>';
      })
      .show();
  };

  /**
   * Handles hiding tooltip on mouseout of node name.
   */
  scope.handleHideTip = function(nodeId) {
    tip.hide();
  };
}
