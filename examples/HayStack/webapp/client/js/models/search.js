/*
 * Search Model
 */

define([], function () {

  var Model = Em.Object.extend({
  /*
    timestamp: function () {
      return this.get('timestamp');
    }.property('timestamp'),   */
    date: function () {
      return new Date(this.get('timestamp')).toLocaleString();
    }.property('date'),
    component: function () {
      return this.get('dimensions')['component'];
    }.property('component'),
    hostname: function () {
      return this.get('dimensions')['hostname'];
    }.property('hostname')
  });

  Model.reopenClass({
    type: 'Search',
    kind: 'Model'
  });

  return Model;

});