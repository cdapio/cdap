angular.module(PKG.name + '.commons')
  .directive('draggable', function() {
    return function(scope, element, attrs) {
      var el = element[0];
      var dropZone = attrs.dropZone,
          // The actual data being transferred.
          data = attrs.model;
       el.draggable = true;

       el.addEventListener(
           'dragstart',
           function(e) {
               e.dataTransfer.effectAllowed = 'copy';
               e.dataTransfer.setData('Id', this.id);
               // Used when a drop has happend. To check if the draggable
               // is valid in the current drop zone.
               e.dataTransfer.setData('DropZone', dropZone);
               e.dataTransfer.setData('Data', data);
               this.classList.add('drag');
               return false;
           },
           false
       );

       el.addEventListener(
           'dragend',
           function() {
            this.classList.remove('drag');
            return false;
           },
           false
       );
    };
  })

  .directive('droppable', function() {
    return {
        scope: {
            drop: '&' // parent
        },
        link: function(scope, element) {
          var el = element[0];
          var counter = 0;
          el.addEventListener(
            'dragover',
            function(e) {
              e.dataTransfer.dropEffect = 'copy';
              this.classList.add('over');
              if (e.preventDefault) {
                e.preventDefault();
              }
              return false;
            },
            false
          );

          el.addEventListener(
            'dragenter',
            function() {
              counter += 1;
              this.classList.add('over');
              return false;
            },
            false
          );

          el.addEventListener(
            'dragleave',
            function() {
              counter -=1;
              if (counter === 0) {
                this.classList.remove('over');
              }
              return false;
            },
            false
          );

            el.addEventListener(
                'drop',
                function(e) {
                    // Stops some browsers from redirecting.
                    if (e.stopPropagation) {
                      e.stopPropagation();
                    }
                    counter = 0;
                    this.classList.remove('over');
                    var id = e.dataTransfer.getData('Id'),
                        dropZone = e.dataTransfer.getData('DropZone'),
                        data = e.dataTransfer.getData('Data');

                    // var item = document.getElementById(e.dataTransfer.getData('Text'));
                    // this.appendChild(item);

                    // call the drop passed drop function
                    scope.$apply(function(scope) {
                        var fn = scope.drop();
                        if ('undefined' !== typeof fn) {
                          fn(id, dropZone, data);
                        }
                    });
                    return false;
                },
                false
            );
        }
    };
  });
