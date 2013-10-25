/*
 * Alerts Model
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
      return this.get('type');
    }.property('type'),
    hostname: function () {
      return this.get('message');
    }.property('message')
  });

  Model.reopenClass({
    type: 'Alerts',
    kind: 'Model'
  });

  return Model;

});