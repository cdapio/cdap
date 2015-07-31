angular.module(PKG.name + '.commons')
  .factory('MyPlumbFactory', function() {
    var defaultSettings = {
      Connector : [ 'Flowchart', {gap: 7} ],
      ConnectionsDetachable: true
    };
    var connectorStyle = {
      strokeStyle: "#666e82",
      fillStyle: '#666e82',
      radius: 7,
      lineWidth: 2
    };

    var commonSettings = {
      endpoint:'Dot',
      maxConnections: 1,
      paintStyle: {
        strokeStyle: 'white',
        fillStyle: '#666e82',
        radius: 7,
        lineWidth: 3
      },
      connectorOverlays: [ [ 'Arrow', { location: 1, length: 12, width: 12, height: 10, foldback: 1 } ] ],
      anchors: [ 'Perimeter', {shape: 'Circle'}]
    };
    var sourceSettings = angular.extend({
      isSource: true,
      anchor: 'Right',
      connectorStyle: connectorStyle
    }, commonSettings);
    var sinkSettings = angular.extend({
      isTarget: true,
      anchor: 'Left',
      connectorStyle: connectorStyle
    }, commonSettings);

    function getSettings() {
      return {
        default: defaultSettings,
        common: commonSettings,
        source: sourceSettings,
        sink: sinkSettings
      };
    }

    function getIcon(plugin) {
      var iconMap = {
        'script': 'fa-code',
        'scriptfilter': 'fa-code',
        'twitter': 'fa-twitter',
        'cube': 'fa-cubes',
        'data': 'fa-database',
        'database': 'fa-database',
        'table': 'fa-table',
        'kafka': 'icon-kafka',
        'stream': 'icon-plugin-stream',
        'tpfsavro': 'icon-avro',
        'jms': 'icon-jms',
        'projection': 'icon-projection'
      };

      var pluginName = plugin.toLowerCase();
      var icon = iconMap[pluginName] ? iconMap[pluginName]: 'fa-plug';
      return icon;
    }

    function generateStyles(name, nodes, xmargin, ymargin) {
      var styles = {};
      var nodeStylesFromDagre = nodes.filter(function(node) {
        return node.label === name;
      });
      if (nodeStylesFromDagre.length) {
        styles = {
          'top': (nodeStylesFromDagre[0].x + xmargin) + 'px',
          'left': (nodeStylesFromDagre[0].y + ymargin) + 'px'
        };
      }
      return styles;
    }

    // Using Dagre here to generate x and y co-ordinates for each node.
    // When we fork and branch and have complex connections this will be useful for us.
    // Right now this returns a pretty simple straight linear graph.
     function getGraph(plugins) {
      var graph = new dagre.graphlib.Graph();
      graph.setGraph({
        nodesep: 60,
        ranksep: 100,
        rankdir: 'LR',
        marginx: 30,
        marginy: 30
      });
      plugins.forEach(function(plugin) {
        graph.setNode(plugin.id, {label: plugin.id, width: 100, height: 100});
      });
      dagre.layout(graph);
      return graph;
    };

    return {
      getSettings: getSettings,
      getIcon: getIcon,
      generateStyles: generateStyles,
      getGraph: getGraph
    }

  });
