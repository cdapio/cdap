var module = angular.module(PKG.name+'.commons');

var tip;
var baseDirective = {
  restrict: 'E',
  templateUrl: 'flow-graph/flow.html',
  scope: {
    model: '=',
    onChangeFlag: '=',
    clickContext: '=',
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

module.controller('myFlowController', function($scope, myHelpers) {
  function update(newVal) {
    // Avoid rendering the graph without nodes and edges.
    if (myHelpers.objectQuery(newVal, 'nodes') && myHelpers.objectQuery(newVal, 'edges')) {
      $scope.render();
    }
  }

  $scope.instanceMap = {};
  $scope.labelMap = {};

  // This is done because of performance reasons.
  // Earlier we used to have scope.$watch('model', function, true); which becomes slow with large set of
  // nodes. So the controller/component that is using this directive need to pass in this flag and update it
  // whenever there is a change in the model. This way the watch becomes smaller.

  // The ideal solution would be to use a service and have this directive register a callback to the service.
  // Once the service updates the data it could call the callbacks by updating them with data. This way there
  // is no watch. This is done in adapters and we should fix this ASAP.
  $scope.$watch('onChangeFlag', function(newVal) {
    if (newVal) {
      update($scope.model);
    }
  });

});

module.directive('myFlowGraph', function ($filter, $state, $alert, myStreamService, $location) {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.render = genericRender.bind(null, scope, $filter, $location);
      scope.parentSelector = attr.parent;

      /**
       * Circle radius for flowlets.
       */
      var flowletCircleRadius = 45;

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
      var leafBuffer = flowletCircleRadius * 0.2;

      var numberFilter = $filter('myNumber');
      scope.getShapes = function() {
        var shapes = {};
        shapes.flowlet = function(parent, bbox, node) {
          var instances = scope.model.instances[node.elem.__data__] || 1;

          // Pushing labels down
          parent.select('.label')
            .attr('transform', 'translate(0,'+ bbox.height / 3 + ')');

          var shapeSvg = parent.insert('circle', ':first-child')
            .attr('x', -bbox.width / 2)
            .attr('y', -bbox.height / 2)
            .attr('r', flowletCircleRadius)
            .attr('class', 'flow-shapes foundation-shape flowlet-svg');

          parent.insert('text')
            .attr('y', -bbox.height/4)
            .text('x' + instances)
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
            .text(numberFilter(scope.model.metrics[scope.labelMap[node.elem.__data__].name]))
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

          var leafOptions = {
            classNames: ['stream-events'],
            circleRadius: flowletCircleRadius,
            diagramWidth: leafDiagramWidth
          };
          drawLeafShape(parent, leafOptions);

          parent.append('text')
            .attr('x', calculateLeafBuffer(parent, leafOptions))
            .attr('y', metricCountPadding)
            .text(numberFilter(scope.model.metrics[scope.labelMap[node.elem.__data__].name]))
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
              fn.call(scope.clickContext, nodeId);
            }
          });
        }
      };

      scope.handleTooltip = function(tip, nodeId) {
        tip
          .html(function() {
            return '<span>' + nodeId + '</span>';
          })
          .show();
      };

      scope.arrowheadRule = function() {
        return false;
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
          .interpolate('basis-closed');

        svgParent.insert('svg:path')
          .attr('d', line(pathinfo))
          .attr('class', classNamesStr)
          .attr('transform', function() {
            return 'translate(' + (- circleRadius + leafBuffer) + ', 0) rotate(-180)';
          });
      }

      /**
       * Calcualtes where event count should be placed relative to leaf shape and centers it.
       */
      function calculateLeafBuffer(parent, nodeOptions) {
        var w = parent.select('.leaf-shape').node().getBBox().width;
        return - nodeOptions.circleRadius - w / 2 + leafBuffer / 2;
      }

    }
  }, baseDirective);
});

module.directive('myWorkflowGraph', function ($filter, $location) {
  return angular.extend({
    link: function (scope) {
      scope.render = genericRender.bind(null, scope, $filter, $location);

      var defaultRadius = 50;
      scope.getShapes = function() {
        var shapes = {};
        shapes.job = function(parent, bbox, node) {

          // Creating Hexagon
          var xPoint = defaultRadius * 7/8;
          var yPoint = defaultRadius * 1/2;
          var points = [
            // points are listed from top and going clockwise
            { x: 0, y: defaultRadius},
            { x: xPoint, y: yPoint},
            { x: xPoint, y: -yPoint },
            { x: 0, y: -defaultRadius},
            { x: -xPoint, y: -yPoint},
            { x: -xPoint, y: yPoint}
          ];
          var shapeSvg = parent.insert('polygon', ':first-child')
              .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '));
          var status = (scope.model.current && scope.model.current[node.elem.__data__]) || '';
          switch(status) {
            case 'COMPLETED':
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg completed');
              break;
            case 'RUNNING':
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg running');
              break;
            case 'FAILED':
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg failed');
              break;
            case 'KILLED':
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg killed');
              break;
            default:
              shapeSvg.attr('class', 'workflow-shapes foundation-shape job-svg');
          }

          node.intersect = function(point) {
            return dagreD3.intersect.polygon(node, points, point);
          };

          return shapeSvg;
        };

        shapes.start = function(parent, bbox, node) {
          var w = bbox.width;
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
          var points = [
            // draw a diamond
            { x:  0, y: -defaultRadius*3/4 },
            { x: -defaultRadius*3/4, y:  0 },
            { x:  0, y:  defaultRadius*3/4 },
            { x:  defaultRadius*3/4, y:  0 }
          ],
          shapeSvg = parent.insert('polygon', ':first-child')
            .attr('points', points.map(function(p) { return p.x + ',' + p.y; }).join(' '))
            .attr('class', 'workflow-shapes foundation-shape conditional-svg');

          node.intersect = function(p) {
            return dagreD3.intersect.polygon(node, points, p);
          };
          return shapeSvg;
        };

        shapes.forkjoin = function(parent, bbox, node) {

          parent.select('.label')
            .attr('style', 'display: none;');

          var shapeSvg = parent.insert('circle', ':first-child')
            .attr('x', -bbox.width / 2)
            .attr('y', -bbox.height / 2)
            .attr('r', 0);


          node.intersect = function(p) {
            return dagreD3.intersect.circle(node, 1, p);
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
          case 'FORKNODE':
            shapeName = 'forkjoin';
            break;
          case 'JOINNODE':
            shapeName = 'forkjoin';
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
      };

      scope.handleNodeClick = function(nodeId) {

        scope.handleHideTip(nodeId);
        var instance = scope.instanceMap[nodeId];
        scope.$apply(function(scope) {
          var fn = scope.click();
          if ('undefined' !== typeof fn) {
            fn.call(scope.clickContext, instance);
          }
        });
      };

      scope.handleTooltip = function(tip, nodeId) {
        if (['Start', 'End'].indexOf(nodeId) === -1) {
          tip
            .html(function() {
              return '<span>'+ scope.instanceMap[nodeId].nodeId + ' : ' + scope.instanceMap[nodeId].program.programName +'</span>';
            })
            .show();
        }

      };

      scope.arrowheadRule = function(edge) {
        if (edge.targetType === 'JOINNODE' || edge.targetType === 'FORKNODE') {
          return false;
        } else {
          return true;
        }
      };
    }
  }, baseDirective);
});

function genericRender(scope, filter, location) {
  var nodes = scope.model.nodes;
  var edges = scope.model.edges;
  if (tip) {
    tip.destroy();
  }

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
    var nodeLabel = '';
    if (node.label && node.label.length) {
      nodeLabel = node.label.length > 8? node.label.substr(0,5) + '...': node.label;
    } else {
      nodeLabel = node.name.length > 8? node.name.substr(0,5) + '...': node.name;
    }
    scope.instanceMap[node.name] = node;
    scope.labelMap[node.label || node.name] = node;
    g.setNode(node.name, { shape: scope.getShape(node.type), label: nodeLabel});
  });

  angular.forEach(edges, function (edge) {
    if (scope.arrowheadRule(edge)) {
      g.setEdge(edge.sourceName, edge.targetName);
    } else {
      g.setEdge(edge.sourceName, edge.targetName, { arrowhead: 'undirected' });
    }
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
  tip = d3.tip()
    .attr('class', 'd3-tip')
    .offset([-10, 0]);
  svg.call(tip);

  // initializing value
  scope.translateX = 0;
  scope.translateY = 0;
  scope.currentScale = 1.1;


  // only being used to center and fit diagram
  var zoom = d3.behavior.zoom();
  zoom.on('zoom', function() {
    svgGroup.attr('transform', 'translate(' + d3.event.translate + ')' + ' scale(' + scope.currentScale + ')');
  });

  var drag = d3.behavior.drag();
  drag.on('drag', function () {
    d3.event.sourceEvent.stopPropagation();
    scope.translateX = scope.translateX + d3.event.dx;
    scope.translateY = scope.translateY + d3.event.dy;

    var boundingClient = svg.node().getBoundingClientRect(),
        gGraph = g.graph();

    if (scope.translateX > boundingClient.width) {
      scope.translateX = boundingClient.width;
    }
    if (scope.translateX < -(gGraph.width * scope.currentScale)) {
      scope.translateX = -(gGraph.width * scope.currentScale);
    }

    if (scope.translateY > boundingClient.height) {
      scope.translateY = boundingClient.height;
    }
    if (scope.translateY < -(gGraph.height * scope.currentScale)) {
      scope.translateY = -(gGraph.height * scope.currentScale);
    }

    var arr = [scope.translateX, scope.translateY];

    svgGroup.attr('transform', 'translate(' + arr + ')' + ' scale(' + scope.currentScale + ')');
  });
  svg.call(drag);

  scope.zoomIn = function() {
    scope.currentScale += 0.1;

    if (scope.currentScale > 2.5) {
      scope.currentScale = 2.5;
    }

    scope.translateX = scope.translateX - (scope.translateX * 0.1);
    scope.translateY = scope.translateY - (scope.translateY * 0.1);

    var arr = [scope.translateX, scope.translateY];
    svgGroup.attr('transform', 'translate(' + arr + ')' + ' scale(' + scope.currentScale + ')');
  };

  scope.zoomOut = function() {
    scope.currentScale -= 0.1;

    if (scope.currentScale < 0.1) {
      scope.currentScale = 0.1;
    }

    var arr = [scope.translateX, scope.translateY];
    svgGroup.attr('transform', 'translate(' + arr + ')' + ' scale(' + scope.currentScale + ')');
  };

  // Run the renderer. This is what draws the final graph.
  renderer(d3.select(selector + ' g'), g);

  /**
   * We need to specify the full URL for the arrowhead.
   * http://stackoverflow.com/questions/19742805/angular-and-svg-filters
   */
  var paths = svgGroup.selectAll('g.edgePath > path.path');
  angular.forEach(paths[0], function(p) {
    p.attributes['marker-end'].nodeValue = 'url(' + location.absUrl() + p.attributes['marker-end'].nodeValue.substr(4);
  });

  /**
   * Handles showing tooltip on mouseover of node name.
   */
  scope.handleShowTip = scope.handleTooltip.bind(null, tip);

  /**
   * Handles hiding tooltip on mouseout of node name.
   */
  scope.handleHideTip = function() {
    tip.hide();
  };

  // Set up onclick after rendering.
  svg
    .selectAll('g.node')
    .on('click', scope.handleNodeClick);

  svg
    .selectAll('g.node text')
    .on('mouseover', scope.handleShowTip)
    .on('mouseout', scope.handleHideTip);

  scope.$on('$destroy', scope.handleHideTip);

  scope.centerImage = function() {
    // Center svg.
    var initialScale = 1.1;
    var svgWidth = svg.node().getBoundingClientRect().width;
    if (svgWidth - g.graph().width <= 0) {
      scope.currentScale = (svgWidth -25) / g.graph().width;
      scope.translateX = 25;
      scope.translateY = 0;
    } else {
      scope.translateX = (svgWidth - g.graph().width * initialScale) / 2;
      scope.translateY = 20;
      scope.currentScale = initialScale;
    }

    zoom
      .translate([scope.translateX, scope.translateY])
      .scale(scope.currentScale)
      .event(svg);
    svg.attr('height', g.graph().height * initialScale + 40);
  };

  scope.centerImage();

}
