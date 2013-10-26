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
    url: 'injectToStream',
    type: 'POST',
    dataType: 'json',
    contentType: 'application/json',
    data: JSON.stringify({data: injectText})
  });
  $("#stream-inject-textarea").val('');
};

Homepage.prototype.enableIntervals = function () {
  var self = this;
  this.interval = setInterval(function() {
    $.ajax({
      url: 'getSentimentAggregates',
      type: 'GET',
      contentType: "application/json",
      cache: false,
      dataType: 'json',
      success: function(data) {
        $("#positive-sentences-processed").text(data.positive);
        $("#neutral-sentences-processed").text(data.neutral);
        $("#negative-sentences-processed").text(data.negative);
        $("#all-sentences-processed").text(parseInt(data.negative) + parseInt(data.positive) + parseInt(data.neutral));
      }
    });

    $.ajax({
      url: 'getSentimentsPositive',
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
      url: 'getSentimentsNeutral',
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
      url: 'getSentimentsNegative',
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

