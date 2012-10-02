
define([], function () {
	return {
		clear: function () {
			$('#informer').html('');
		},
		queue: [],
		show: function (message, style, persist) {

			var wrapper = $('#informer');
			var children = wrapper.children();
			for (var i = 0; i < children.length; i ++) {
				$(children[i]).remove();
			}

			var div = $('<div class="alert-wrapper" />').append(
				$('<div></div>')
				.addClass('alert').addClass(style).html(message));

			$('#informer').append(div);
			div.fadeIn();
			
			if (!persist) {

				this.queue.push(div);

				setTimeout(function () {

					var el = C.Vw.Informer.queue.shift();

					el.animate({
						opacity: 0,
						height: 0
					}, function () {
						el.remove();
					});
					
				}, 4000);
			}
		}
	};
});