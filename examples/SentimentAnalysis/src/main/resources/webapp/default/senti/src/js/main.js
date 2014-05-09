/**
 * Js for main page.
 */

var Homepage = function () {
  this.init(arguments);
};

Homepage.prototype.init  = function () {
  var self = this;
  this.enableIntervals();

  $("#text-inject-form").submit(function(e) {
    e.preventDefault();
    self.injectIntoStream();
  });



};

Homepage.prototype.injectIntoStream = function() {
  var injectText = $("#stream-inject-textarea").val();
  $.ajax({
    url: '/v2/streams/sentence',
    type: 'POST',
    dataType: 'json',
    contentType: 'application/json',
    data: JSON.stringify(injectText)
  });
  $("#stream-inject-textarea").val('');
};

Homepage.prototype.enableIntervals = function () {
  var self = this;
  this.interval = setInterval(function() {
    $.ajax({
      url: '/v2/apps/sentiment/procedures/sentiment-query/methods/aggregates',
      type: 'GET',
      contentType: "application/json",
      cache: false,
      dataType: 'json',
      success: function(data) {
        if (!data.positive) {
            data.positive = 0;
        }
        if (!data.negative) {
            data.negative= 0;
        }
        if (!data.neutral) {
            data.neutral= 0;
        }
        $("#positive-sentences-processed").text(data.positive);
        $("#neutral-sentences-processed").text(data.neutral);
        $("#negative-sentences-processed").text(data.negative);
        $("#all-sentences-processed").text(parseInt(data.negative) + parseInt(data.positive) + parseInt(data.neutral));
      }
    });

    $.ajax({
      url: '/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=positive',
      type: 'GET',
      contentType: "application/json",
      dataType: 'json',
      cache: false,
      success: function(data) {
        var list = [];
        for (item in data) {
          if(data.hasOwnProperty(item)) {
            list.push('<tr><td>' + item + '</td></tr>');
          }
        }
        $('#positive-sentences-table tbody').html(list.join(''));
      }
    });

    $.ajax({
      url: '/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=neutral',
      type: 'GET',
      contentType: "application/json",
      dataType: 'json',
      cache: false,
      success: function(data) {
        var list = [];
        for (item in data) {
          if(data.hasOwnProperty(item)) {
            list.push('<tr><td>' + item + '</td></tr>');
          }
        }
        $('#neutral-sentences-table tbody').html(list.join(''));
      }
    });

    $.ajax({
      url: '/v2/apps/sentiment/procedures/sentiment-query/methods/sentiments?sentiment=negative',
      type: 'GET',
      contentType: "application/json",
      dataType: 'json',
      cache: false,
      success: function(data) {
        var list = [];
        for (item in data) {
          if(data.hasOwnProperty(item)) {
            list.push('<tr><td>' + item + '</td></tr>');
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

