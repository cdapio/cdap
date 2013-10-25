/**
 * Js for main page.
 */

var HOST = 'http://127.0.0.1:20000';
var Homepage = function () {
  this.init(arguments);
};

Homepage.prototype.init  = function () {
  this.enableIntervals();
};

Homepage.prototype.enableIntervals = function () {
  var self = this;
  this.interval = setInterval(function() {
    $.ajax({
      url: HOST + '/v2/apps/sentiment/procedures/sentiment-query/methods/aggregates',
      type: 'POST',
      data: JSON.stringify({data: {}}),
      contentType:"application/json; charset=utf-8",
      dataType: 'json',
      success: function(data) {
        console.log(data)
      }
    });

  }, 1000);
};


$(document).ready(function() {
  new Homepage();
});

