/*
 * JSplumb helper.
 */

define([], function () {

  /**
   * Default color.
   */
  var DEFAULT_COLOR = '#AAA';

  /**
   * Set up plumber constructor. This creates and configures an instance of jsPlumb.
   */
  var Plumber = function() {

    this.sourcespec = {
      endpoint: 'Dot',
      paintStyle : { radius:1, fillStyle:"#89b086" },
      maxConnections:-1,
      isSource:true
    };

    this.destspec = {
      endpoint: 'Dot',
      paintStyle : { radius:1, fillStyle:"#89b086" },
      maxConnections:-1,
      isTarget:true
    };

    this.plumber = jsPlumb.getInstance();
  };

  /**
   * Connector.
   */
  Plumber.prototype.connector = ["Straight", {gap: 0, curviness: 70}];

  /**
   * Gets end point.
   * @param {string} elId element id.
   * @param {string} location {source|target}
   * @return jsPlumb endpoint.
   */
  Plumber.prototype.getEndPoint = function(elId, location) {

    if (location === 'source') {
      return this.plumber.addEndpoint(elId, this.sourcespec, {
        anchor: 'RightMiddle'
      });
    } else if (location === 'target') {
      return this.plumber.addEndpoint(elId, this.destspec, {
        anchor: 'LeftMiddle'
      });
    }
  };

  /**
   * Connects 2 points using a connector.
   * @param {jsPlumb endpoint} from starting point.
   * @param {jsPlumb endpoint} to ending point.
   * @param {string} opt_color optional color of color.
   */
  Plumber.prototype.connect = function(from, to, opt_color) {
    
    var self = this;
    var color = opt_color || DEFAULT_COLOR;

    var sourceEndPoint = this.getEndPoint(from, 'source');
    var targetEndPoint = this.getEndPoint(to, 'target');

    this.plumber.connect({
      source: sourceEndPoint,
      target: targetEndPoint,
      paintStyle: {strokeStyle: color, lineWidth: 2, dashstyle:"1"},
      connector: this.connector
    })
  };

  return new Plumber();

});
