// jQuery(document).ready(function() {

function changeExampleTab(example) {
  return function(e) {
    e.preventDefault();
    var scrollOffset = $(this).offset().top - $(document).scrollTop();
    $(".tab-pane").removeClass("active");
    $(".tab-pane-" + example).addClass("active");
    $(".example-tab").removeClass("active");
    $(".example-tab-" + example).addClass("active");
    $(document).scrollTop($(this).offset().top - scrollOffset);
    localStorage.setItem("cdap-documentation-tab", example);
  }
}

$(function() {
  var examples = ["Linux", "Windows"];
  for (var i = 0; i < examples.length; i++) {
    var example = examples[i];
    $(".example-tab-" + example).click(changeExampleTab(example));
  }
});

$(document).ready(function() {
  var example = localStorage.getItem("cdap-documentation-tab");
  var tabs = $(".example-tab-" + example);
  if (example && tabs) {
    try {
      $(".example-tab-" + example)[0].click(changeExampleTab(example));
    } catch (e) {
      console.log("Unable to set using Cookie: " + example);
    }
  } else {
    console.log("Unable to set using Cookie: " + example);
  }
});

// });
