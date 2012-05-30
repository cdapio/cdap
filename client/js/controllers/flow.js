//
// Flow Controller. Content is an array of flowlets.
//

define([], function () {

	return Em.ArrayProxy.create({
		content: [],

		current: null,
		history: Em.ArrayProxy.create({content: []}),

		statusButtonAction: function () {
			return 'No Action';
		}.property(),
		statusButtonClass: function () {
			return 'btn btn-warning';
		}.property(),

		init: function () {
			var connectorPaintStyle = {
				lineWidth:3,
				strokeStyle:"#555",
				joinstyle:"round"
			};

			this.sourcespec = {
				endpoint:"Dot",
				paintStyle:{ fillStyle:"#51A351", radius:5 },
				isSource:true,
				connector:[ "Flowchart", { stub:40 } ],
				connectorStyle:connectorPaintStyle
			};

			this.destspec = {
				endpoint:"Dot",
				paintStyle:{ fillStyle:"#51A351", radius:5 },
				maxConnections:-1,
				dropOptions:{ hoverClass:"hover", activeClass:"active" },
				isTarget:true,
				overlays:[
					[ "Label", { location:[0.5, -0.5], label:"", cssClass:"endpointTargetLabel" } ]
				]
			};

			this.plumber = jsPlumb.getInstance();

			this.plumber.importDefaults({
				EndpointStyles:[
					{ fillStyle:'#225588' },
					{ fillStyle:'#558822' }
				],
				Endpoints:[
					[ "Dot", { radius:5 } ],
					[ "Dot", { radius:9 } ]
				],
				ConnectionOverlays:[
					[ "Arrow", { location:1, width: 8, length: 10 } ]
				]
			});
		},

		unload: function () {

			clearInterval(this.interval);

			this.set('content', []);
			this.history.set('content', []);
			this.clear();
			this.history.clear();

			this.set('current', null);

		},

		load: function (id) {

			var self = this;

			App.socket.request('rest', {
				method: 'history',
				params: id
			}, function (response) {
				var hist = response.params;
				for (var i = 0; i < hist.length; i ++) {
					hist[i] = Ember.Object.create(hist[i]);
					self.history.pushObject(hist[i]);
				}
			});

			App.socket.request('rest', {
				method: 'flows',
				params: id
			}, function (response) {

				self.set('current', App.Models.Flow.create(response.params));

				var flowlets = response.params.flowlets;
				for (var i = 0; i < flowlets.length; i ++) {
					self.pushObject(App.Models.Flowlet.create(flowlets[i]));
				}

				self.drawNodes();

				clearInterval(self.interval);
				self.interval = setInterval(function () {
					if (self.current.status === 'running') {
						self.updateStats();
					}
				}, 1000);
				self.updateStats();

			});
		},

		updateStats: function () {
			var self = this;
			App.socket.request('rest', {
				method: 'status',
				params: this.get('current').get('id')
			}, function (response) {
				var flowlets = response.params.flowlets;
				for (var i = 0; i < flowlets.length; i ++) {
					var start = parseInt($('#stat' + flowlets[i].id).html(), 10);
					var finish = flowlets[i].tuples.processed;

					if (!isNaN(start)) {
						self.spins($('#stat' + flowlets[i].id), start, finish, 1000);
					} else {
						$('#stat' + flowlets[i].id).html(finish);
						self.updateStats();
					}
				}
			});
		},

		intervals: {},
		stop_spin: function () {
			for (var i in this.intervals) {
				clearInterval(this.intervals[i]);
				delete this.intervals[i];
			}
		},
		spinner: function (element, start, finish, incr) {
			var id = element.attr('id');
			var res = 10;
			incr *= res;

			var interval = this.intervals[id] = setInterval(function () {
				element.html(Math.ceil(start));
				start += incr;
				if (start >= finish) {
					clearInterval(interval);
					element.html(finish);
				}
			}, res);
		},
		spins: function (element, start, finish, time) {
			var self = this;
			if (start === finish) {
				element.html(finish);
				return;
			}
			var incr = (finish - start) / time;
			this.spinner(element, start, finish, incr);
		},

		drawNodes: function () {

			$('#flowviz').html('');

			var flowlets = App.Controllers.Flow.content;
			var parent = $('#flowviz');
			var self = this;

			function get_flowlet(id) {
				id = id + "";
				for (var k = 0; k < flowlets.length; k++) {
					if (flowlets[k].id === id) {
						return flowlets[k];
					}
				}
			}

			var fade_delay = 0;
			function append (id, column, connectTo) {
				
				var flowlet = get_flowlet(id);
				var elId;

				var el = $('<div id="flowlet' + id +
					'" class="window"><strong>' + flowlet.name +
					'</strong><div id="stat' + id + '"></div></div>');
				parent.append(el);
				if (column === 0) {
					el.addClass('source');
					el.css({marginLeft: 0});
				}

				for (var i = 0; i < cx[id].length; i ++) {
					elId = "flowlet" + cx[id][i];
					self.plumber.addEndpoint(elId, self.destspec, {
						anchor: "RightMiddle",
						uuid: elId + "RightMiddle"
					});
				}
				if (cx[id].length > 0) {
					elId = "flowlet" + id;
					self.plumber.addEndpoint(elId, self.sourcespec, {
						anchor: "LeftMiddle",
						uuid: elId + "LeftMiddle"
					});
				}

				columns[column] = columns[column] !== undefined ? columns[column] + 1 : 0;
				
			}

			function connect(from, to) {
				self.plumber.connect({uuids:['flowlet' + from + 'RightMiddle',
					'flowlet' + to + 'LeftMiddle']});
			}

			var column = 1, columns = {};

			var cx = App.Controllers.Flow.current.connections;
			function connects_to(id) {
				var i;
				
				if (!id) { // Append the first node
					for (i in cx) {
						if (cx[i].length === 0) {
							append(i, 0);
							connects_to(i);
							break;
						}
					}
				} else {
					var atleastOne = false;
					for (i in cx) {
						for (var k = 0; k < cx[i].length; k ++) {
							if (cx[i][k] === id) {
								if (!atleastOne) {
									column ++;
									atleastOne = true;
								}
								append(i, column);
								connect(id, i);
								connects_to(i);
							}
						}
					}
				}
			}
			connects_to(null);

		}
	});
});