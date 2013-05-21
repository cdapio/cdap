
define(['core/socket'], function (Socket) {

	Em.debug('Loading API');

	var Api = Ember.Object.extend({

		Socket: Socket,

		connect: function () {

			this.Socket.connect();

		},

		RPC: function (method, arguments, callback) {

			callback(null);

		},

		getElement: function (type, id, callback) {

			callback(null);

		},

		__methodNames: {
			'App': 'getApplications',
			'Flow': 'getFlows',
			'Stream': 'getStreams',
			'Procedure': 'getQueries',
			'Dataset': 'getDatasets'
		},

		getElements: function (type, callback, appId, arg) {

			var self = this;

			C.get('metadata', {
				method: this.__methodNames[type] + (appId ? 'ByApplication' : ''),
				params: appId ? [appId] : []
			}, function (error, response, params) {

				if (error) {
					if (typeof callback === 'function') {
						callback([], arg);
					} else {
						C.interstitial.label(error);
					}
				} else {
					var objects = response.params;
					var i = objects.length, type = params[0];

					while (i--) {
						objects[i] = C[type].create(objects[i]);
					}

					if (typeof params[1] === 'function') {
						callback(objects, arg);
					}
				}
			}, [type, callback]);

		},

		getMetrics: function () {

		},

		Upload: Em.Object.create({

			processing: false,
			resource_identifier: null,
			fileQueue: [],
			entityType: null,

			__sendFile: function () {

				var file = this.fileQueue.shift();
				if (file === undefined) {
					window.location.reload();
					return;
				}

				var xhr = new XMLHttpRequest();
				var uploadProg = xhr.upload || xhr;

				uploadProg.addEventListener('progress', function (e) {

					if (e.type === 'progress') {
						var pct = Math.round((e.loaded / e.total) * 100);
						$('#far-upload-status').html(pct + '% Uploaded...');
					}

				});

				xhr.open('POST', '/upload/' + file.name, true);
				xhr.setRequestHeader("Content-type", "application/octet-stream");
				xhr.send(file);

			},

			sendFiles: function (files, type) {

				this.set('entityType', type);

				this.fileQueue = [];
				for (var i = 0; i < files.length; i ++) {
					this.fileQueue.push(files[i]);
				}

				if (files.length > 0) {
					this.__sendFile();
				}
			},

			update: function (response) {

				if (response.error) {
					C.Modal.show("Deployment Error", response.error);
					this.processing = false;

				} else {

					switch (response.step) {
						case 0:
						break;
						case 1:
						case 2:
						case 3:
							this.set('message', response.message);
							break;
						case undefined:
							if (response.status === 'initialized') {
								this.resource_identifier = response.resource_identifier;
							}
							this.set('message', response.status);
						break;
						case 5:
							this.set('message', 'Drop a JAR to Deploy');
							this.processing = false;
							this.__sendFile();
						break;
						default:
							this.set('message', 'Drop a JAR to Deploy');
							this.processing = false;
							this.set('warningMessage', response.message);

							$('.modal').modal('hide');

							C.Modal.show("Deployment Error", response.message);

					}
				}
			}
		})
	});

	Api = Api.create();

	Api.Socket.addEventHandler('upload', function (status) {
		Api.Upload.update(status);
	});

	return Api;

});