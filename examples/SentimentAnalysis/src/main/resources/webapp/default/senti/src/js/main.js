/**
 * Js for main page.
 */

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
      url: '/v2/apps/sentiment/procedures/sentiment-query/methods/aggregates',
      type: 'GET',
      contentType:"text/plain; charset=utf-8",
      dataType: 'jsonp',
      cache: false,
      success: function(data) {
        $("#positive-sentences-processed").text(data.positive);
        $("#neutral-sentences-processed").text(data.neutral);
        $("#negative-sentences-processed").text(data.negative);
        $("#all-sentences-processed").text(parseInt(data.negative) + parseInt(data.positive) + parseInt(data.neutral));
      }
    });

    $.ajax({
      url: '/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments',
      type: 'GET',
      data: {sentiment: 'positive'},
      contentType:"text/plain; charset=utf-8",
      dataType: 'jsonp',
      cache: false,
      success: function(data) {
        var list = [];
        for (item in data) {
          if(data.hasOwnProperty(item)) {
            list.push('<tr><td>' + new Date(data[item]) + '</td><td>' + item + '</td></tr>');
          }
        }
        $('#positive-sentences-table tbody').html(list.join(''));
      }
    });

    $.ajax({
      url: '/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments',
      type: 'GET',
      data: {sentiment: 'neutral'},
      contentType:"text/plain; charset=utf-8",
      dataType: 'jsonp',
      cache: false,
      success: function(data) {
        var list = [];
        for (item in data) {
          if(data.hasOwnProperty(item)) {
            list.push('<tr><td>' + new Date(data[item]) + '</td><td>' + item + '</td></tr>');
          }
        }
        $('#neutral-sentences-table tbody').html(list.join(''));
      }
    });

    $.ajax({
      url: '/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments',
      type: 'GET',
      data: {sentiment: 'negative'},
      contentType:"text/plain; charset=utf-8",
      dataType: 'jsonp',
      cache: false,
      success: function(data) {
        var list = [];
        for (item in data) {
          if(data.hasOwnProperty(item)) {
            list.push('<tr><td>' + new Date(data[item]) + '</td><td>' + item + '</td></tr>');
          }
        }
        $('#negative-sentences-table tbody').html(list.join(''));
      }
    });

  }, 1000);
};


$(document).ready(function() {
  new Homepage();
});

