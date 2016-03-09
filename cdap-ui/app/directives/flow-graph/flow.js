/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

var module = angular.module(PKG.name+'.commons');

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

module.directive('myFlowGraph', function ($filter, $state, myStreamService, $location, FlowFactories, $tooltip) {
  return angular.extend({
    link: function (scope, elem, attr) {
      scope.render = FlowFactories.genericRender.bind(null, scope, $filter, $location);
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
          var instances = scope.model.instances[node.labelId] || 1;

          // Pushing labels down
          parent.select('.label')
            .attr('transform', 'translate(0,'+ bbox.height / 3 + ')')
            .attr('class', 'node-label');

          var shapeSvg = parent.insert('circle', ':first-child')
            .attr('x', -bbox.width / 2)
            .attr('y', -bbox.height / 2)
            .attr('r', flowletCircleRadius)
            .attr('class', 'flow-shapes foundation-shape flowlet-svg');

          parent.insert('text')
            .attr('y', -bbox.height/4)
            .text('x' + instances)
            .attr('class', 'flow-shapes flowlet-instance-count')
            .attr('id', 'instance-' + node.labelId);

          var leafOptions = {
            classNames: ['flowlet-events'],
            circleRadius: flowletCircleRadius,
            diagramWidth: leafDiagramWidth
          };
          drawLeafShape(parent, leafOptions);

          parent.insert('text')
            .attr('x', calculateLeafBuffer(parent, leafOptions))
            .attr('y', metricCountPadding)
            .text(numberFilter(scope.model.metrics[scope.labelMap[node.labelId].name]))
            .attr('class', 'flow-shapes flowlet-event-count')
            .attr('id', 'metrics-' + node.labelId);

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
            .text(numberFilter(scope.model.metrics[scope.labelMap[node.labelId].name]))
            .attr('class', 'flow-shapes stream-event-count')
            .attr('id', 'metrics-' + node.labelId);

          parent.select('.label')
            .attr('class', 'node-label');

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

      scope.arrowheadRule = function() {
        return false;
      };

      scope.update = function () {
        // UPDATE INSTANCE COUNT
        angular.forEach(scope.model.instances, function (value, key) {
          d3.select('#instance-' + key)
            .text('x' + value);
        });

        // UPDATE METRICS COUNT
        angular.forEach(scope.model.metrics, function (value, key) {
          d3.select('#metrics-' + key)
            .text(value);
        });
      };

      scope.createTooltips = function () {
        var labels = d3.selectAll('.node-label')[0];
        var labelTooltips = {};

        angular.forEach(labels, function (label) {
          labelTooltips[label.id] = $tooltip(angular.element(label), {
            title: label.id,
            trigger: 'manual',
            delay: { show: 300, hide: 0 },
            target: angular.element(label),
            container: '.diagram-container'
          });
        });

        d3.selectAll('.node-label')
          .on('mouseover', function (node) {
            labelTooltips[node].enter();
          })
          .on('mouseout', function (node) {
            labelTooltips[node].leave();
          });
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

      FlowFactories.prepareGraph(scope);

    }
  }, baseDirective);
});
