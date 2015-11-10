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

angular.module(PKG.name+'.commons')
  .factory('d3', function ($window) {
    return $window.d3;
  })

  .factory('dagreD3', function ($window) {
    return $window.dagreD3;
  })

  .factory('FlowFactories', function () {
    function genericRender(scope, filter, location, tip, isWorkflow) {
      var nodes = scope.model.nodes;
      var edges = scope.model.edges;
      if (tip) {
        tip.destroy();
      }

      var renderer = new dagreD3.render();
      var g = new dagreD3.graphlib.Graph();

      g.setGraph({
        nodesep: 90,
        ranksep: 100,
        rankdir: 'LR',
        marginx: 30,
        marginy: 50
      })
        .setDefaultEdgeLabel(function () { return {}; });

      // First set nodes and edges.
      angular.forEach(nodes, function (node) {
        var nodeLabel = '';

        if (isWorkflow) {
          if (node.label && node.label.length) {
            nodeLabel = node.label.length > 24? node.label.substr(0,22) + '...': node.label;
          } else {
            nodeLabel = node.name.length > 24? node.name.substr(0,22) + '...': node.name;
          }
        } else {
          if (node.label && node.label.length) {
            nodeLabel = node.label.length > 8? node.label.substr(0,5) + '...': node.label;
          } else {
            nodeLabel = node.name.length > 8? node.name.substr(0,5) + '...': node.name;
          }
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
        .on('dblclick', scope.handleNodeClick);

      if (isWorkflow) {
        svg
          .selectAll('text.token-label')
          .on('click', scope.toggleToken);
      }

      svg
        .selectAll('g.node text')
        .on('mouseover', scope.handleShowTip)
        .on('mouseout', scope.handleHideTip);

      scope.$on('$destroy', scope.handleHideTip);

      scope.centerImage = function() {
        // Center svg.
        var initialScale = 1;
        var svgWidth = svg.node().getBoundingClientRect().width;
        var svgHeight = svg.node().getBoundingClientRect().height;
        if (svgWidth - g.graph().width <= 0) {
          scope.currentScale = svgWidth / g.graph().width;
          scope.translateX = 0;
          scope.translateY = ((svgHeight - g.graph().height) * scope.currentScale)/2;
        } else {
          scope.translateX = (svgWidth - g.graph().width * initialScale) / 2 + 10;
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


    return {
      genericRender: genericRender
    };
  });
