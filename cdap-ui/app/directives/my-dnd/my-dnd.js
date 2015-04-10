angular.module(PKG.name + '.commons')
  .directive('draggable', function() {
    return function(scope, element, attrs) {
      var el = element[0];
      var dropZone = attrs.dropZone,
          // The actual data being transferred.
          data = attrs.model,
          dragType = attrs.dragType;
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
               // Used in dragover event to check if the drag is
               // droppable in a drop target
               e.dataTransfer.setData(dragType, true);
               this.classList.add('drag');
               return false;
           },
           false
       );

       el.addEventListener(
           'dragend',
           function(e) {
               this.classList.remove('drag');
               return false;
           },
           false
       );
    }
  })

  .directive('droppable', function() {
    return {
        scope: {
            drop: '&' // parent
        },
        link: function(scope, element) {
          var el = element[0];

          el.addEventListener(
            'dragover',
            function(e) {
                e.dataTransfer.dropEffect = 'copy';
                // Visual indicator to show if the current draggable is valid
                // in the dragover drop zone.
                // If I drag a 'source' in an etl template and dragover
                // a container that is supposed to hold transforms then it
                // should show visually that this is not a valid drop place for a source.
                var dropType = e.target.getAttribute('data-drop-type') ||
                               e.target.parentElement.getAttribute('data-drop-type');

                if (e.dataTransfer.types.indexOf(dropType) > -1) {
                  this.classList.add('over');
                  this.classList.add('green');
                } else {
                  this.classList.add('over');
                  this.classList.add('red');
                }

                if (e.preventDefault) e.preventDefault();
                return false;
            },
            false
          );

          el.addEventListener(
            'dragenter',
            function(e) {
                this.classList.add('over');
                return false;
            },
            false
          );

          el.addEventListener(
            'dragleave',
            function(e) {
                this.classList.remove('over');
                this.classList.remove('green');
                this.classList.remove('red');
                return false;
            },
            false
          );

            el.addEventListener(
                'drop',
                function(e) {
                    // Stops some browsers from redirecting.
                    if (e.stopPropagation) e.stopPropagation();

                    this.classList.remove('over');
                    this.classList.remove('green');
                    this.classList.remove('red');
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
    }
  });
