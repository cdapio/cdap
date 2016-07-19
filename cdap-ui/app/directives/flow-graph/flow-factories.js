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

  .factory('dagreD3', function ($window) {
    return $window.dagreD3;
  })

  .factory('FlowFactories', function () {
    function prepareGraph (scope) {
      scope.translateX = 0;
      scope.translateY = 0;
      scope.currentScale = 1.1;
      scope.renderer = new dagreD3.render();
      scope.g = new dagreD3.graphlib.Graph();

      scope.g.setGraph({
        nodesep: 90,
        ranksep: 120,
        rankdir: 'LR',
        marginx: 30,
        marginy: 50
      })
        .setDefaultEdgeLabel(function () { return {}; });

      angular.extend(scope.renderer.shapes(), scope.getShapes());

      scope.selector = '';
      // Making the query to be more specific instead of doing
      // it under the entire DOM. This allows us to draw the diagram
      // in multiple places.
      if (scope.parentSelector) {
        scope.selector += scope.parentSelector;
      }
      scope.selector += ' svg';


      scope.svg = d3.select(scope.selector).attr('fill', 'white');
      scope.svgGroup = d3.select(scope.selector + ' g');

      scope.drag = d3.behavior.drag();
      scope.drag.on('drag', function () {
        d3.event.sourceEvent.stopPropagation();
        scope.translateX = scope.translateX + d3.event.dx;
        scope.translateY = scope.translateY + d3.event.dy;

        var boundingClient = scope.svg.node().getBoundingClientRect(),
            gGraph = scope.g.graph();

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

        scope.svgGroup.attr('transform', 'translate(' + arr + ')' + ' scale(' + scope.currentScale + ')');
      });


      scope.zoomIn = function() {
        scope.currentScale += 0.1;

        if (scope.currentScale > 2.5) {
          scope.currentScale = 2.5;
        }

        scope.translateX = scope.translateX - (scope.translateX * 0.1);
        scope.translateY = scope.translateY - (scope.translateY * 0.1);

        var arr = [scope.translateX, scope.translateY];
        scope.svgGroup.attr('transform', 'translate(' + arr + ')' + ' scale(' + scope.currentScale + ')');
      };

      scope.zoomOut = function() {
        scope.currentScale -= 0.1;

        if (scope.currentScale < 0.1) {
          scope.currentScale = 0.1;
        }

        var arr = [scope.translateX, scope.translateY];
        scope.svgGroup.attr('transform', 'translate(' + arr + ')' + ' scale(' + scope.currentScale + ')');
      };


      // only being used to center and fit diagram
      var zoom = d3.behavior.zoom();
      zoom.on('zoom', function() {
        scope.svgGroup.attr('transform', 'translate(' + d3.event.translate + ')' + ' scale(' + scope.currentScale + ')');
      });

      scope.centerImage = function() {
        // Center svg.
        var initialScale = 1;
        scope.svg.attr('height', scope.g.graph().height * initialScale + 40);

        var svgWidth = scope.svg.node().getBoundingClientRect().width;
        var svgHeight = scope.svg.node().getBoundingClientRect().height;
        if (svgWidth - scope.g.graph().width <= 0) {
          scope.currentScale = svgWidth / scope.g.graph().width;
          scope.translateX = 0;
          scope.translateY = ((svgHeight - scope.g.graph().height) * scope.currentScale)/2;
        } else {
          scope.translateX = (svgWidth - scope.g.graph().width * initialScale) / 2 + 10;
          scope.translateY = 20;
          scope.currentScale = initialScale;
        }
        zoom
          .translate([scope.translateX, scope.translateY])
          .scale(scope.currentScale)
          .event(scope.svg);

      };


    }

    function genericRender(scope, filter, location, isWorkflow) {
      var nodes = scope.model.nodes;
      var edges = scope.model.edges;

      scope.svg = d3.select(scope.selector);
      scope.svgGroup = d3.select(scope.selector + ' g');
      scope.svg.call(scope.drag);

      // First set nodes and edges.
      angular.forEach(nodes, function (node) {
        var nodeLabel = '';

        if (isWorkflow) {
          if (node.label && node.label.length) {
            nodeLabel = node.label.length > 24? node.label.substr(0,20) + '...': node.label;
          } else {
            nodeLabel = node.name.length > 24? node.name.substr(0,20) + '...': node.name;
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
        scope.g.setNode(node.name, {
          shape: scope.getShape(node.type),
          label: nodeLabel,
          labelId: node.name
        });
      });

      angular.forEach(edges, function (edge) {
        if (scope.arrowheadRule(edge)) {
          scope.g.setEdge(edge.sourceName, edge.targetName);
        } else {
          scope.g.setEdge(edge.sourceName, edge.targetName, { arrowhead: 'undirected' });
        }
      });

      // Run the renderer. This is what draws the final graph.
      scope.renderer(d3.select(scope.selector + ' g'), scope.g);

      /**
       * We need to specify the full URL for the arrowhead.
       * http://stackoverflow.com/questions/19742805/angular-and-svg-filters
       */
      var paths = scope.svgGroup.selectAll('g.edgePath > path.path');
      angular.forEach(paths[0], function(p) {
        p.attributes['marker-end'].nodeValue = 'url(' + location.absUrl() + p.attributes['marker-end'].nodeValue.substr(4);
      });


      // Set up onclick after rendering.
      scope.svg
        .selectAll('g.node')
        .on('dblclick', scope.handleNodeClick);

      if (isWorkflow) {
        scope.svg
          .selectAll('g.token')
          .on('click', scope.toggleToken);
      }

      scope.createTooltips();
    }


    return {
      genericRender: genericRender,
      prepareGraph: prepareGraph
    };
  });
