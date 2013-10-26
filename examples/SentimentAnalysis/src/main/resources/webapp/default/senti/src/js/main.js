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
      url: 'http://127.0.0.1:10000/v2/apps/sentiment/procedures/sentiment-query/methods/aggregates',
      type: 'GET',
      contentType: "application/json",
      dataType: 'jsonp',
      jsonp: 'jsonp',
      cache: false,
      success: function(data) {
        $("#positive-sentences-processed").text(data.positive);
        $("#neutral-sentences-processed").text(data.neutral);
        $("#negative-sentences-processed").text(data.negative);
        $("#all-sentences-processed").text(parseInt(data.negative) + parseInt(data.positive) + parseInt(data.neutral));
      }
    });

    $.ajax({
      url: 'http://127.0.0.1:10000/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=positive',
      type: 'GET',
      contentType: "application/json",
      dataType: 'jsonp',
      jsonp: 'jsonp',
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
      url: 'http://127.0.0.1:10000/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=neutral',
      type: 'GET',
      contentType: "application/json",
      dataType: 'jsonp',
      jsonp: 'jsonp',
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
      url: 'http://127.0.0.1:10000/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=negative',
      type: 'GET',
      contentType: "application/json",
      dataType: 'jsonp',
      jsonp: 'jsonp',
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

