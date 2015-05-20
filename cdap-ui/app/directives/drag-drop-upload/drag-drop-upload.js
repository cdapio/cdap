angular.module(PKG.name + '.commons')
  .directive('myDragDropUpload', function($state, myAppUploader) {
    return {
      restrict: 'A',
      link: function(scope, element) {

        function drag (event) {
          event.preventDefault();
          element.addClass('drag-target');
        }

        function  dragLeave (event) {
          event.preventDefault();
          element.removeClass('drag-target');
        }

        element.bind('dragover', drag);
        // element.bind('dragenter', drag);
        element.bind('dragleave', dragLeave);
        element.bind('drop', function(event) {
          event.stopPropagation();
          event.preventDefault();
          element.removeClass('drag-target');

          var namespace = $state.params.namespace || $state.params.nsadmin;
          myAppUploader.upload(event.dataTransfer.files, namespace);
        });

      }
    };
  });
