/*
 * Log Model
 */

define([], function () {

  var Model = Em.Object.extend({
    className: function () {
      return (this.get('thumb') ? 'story thumbnailed' : 'story') + ' clearfix';
    }.property('thumb'),
    timeago: function () {
      return $.timeago(new Date(this.get('pubDate') || new Date().getTime()));
    }.property('date')
  });

  Model.reopenClass({
    type: 'Log',
    kind: 'Model'
  });

  return Model;

});