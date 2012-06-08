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

			this.sourcespec = {
				endpoint:"Dot",
				paintStyle:{ fillStyle:"#51A351", radius:5 },
				isSource:true
			};

			this.destspec = {
				endpoint:"Dot",
				paintStyle:{ fillStyle:"#51A351", radius:5 },
				maxConnections:-1,
				dropOptions:{ hoverClass:"hover", activeClass:"active" },
				isTarget:true
			};

			this.plumber = jsPlumb.getInstance();

			this.plumber.importDefaults({
				ConnectionOverlays:[
					[ "Arrow", { location:1, width: 20, length: 20 } ]
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

		load: function (app, id, run) {

			var self = this;

			if (!run) {
				$('#run-info').hide();
			}
			this.set('run', run);

			App.socket.request('monitor', {
				method: 'getFlowHistory',
				params: ['demo', app, id]
			}, function (response) {
				var hist = response.params;
				for (var i = 0; i < hist.length; i ++) {
					hist[i] = App.Models.Run.create(hist[i]);
					self.history.pushObject(hist[i]);

					if (run && run === hist[i].runId) {
						$('#run-info').html('The following diagram represents a run from ' + hist[i].startTime + ' to ' + hist[i].endTime + '. It finished with a ' + hist[i].endStatus + ' status.').show();
					}

				}
			});

			App.interstitial.loading();

			App.socket.request('monitor', {
				method: 'getFlowDefinition',
				params: ['demo', app, id, '']
			}, function (response) {

				if (!response.params) {
					App.interstitial.label('Flow not found.', {
						action: 'All Flows',
						click: function () {
							App.router.set('location', '');
						}
					});
					return;
				}

				self.set('current', App.Models.Definition.create(response.params));

				var flowlets = response.params.flowlets;
				for (var i = 0; i < flowlets.length; i ++) {
					self.pushObject(App.Models.Flowlet.create(flowlets[i]));
				}

				self.drawNodes();

				/*
				clearInterval(self.interval);
				self.interval = setInterval(function () {
					if (self.current.status === 'running') {
						self.updateStats();
					}
				}, 1000);
				self.updateStats();
				*/
				
				App.interstitial.hide();

			});
		},

		updateStats: function () {
			var self = this;
			App.socket.request('monitor', {
				method: 'getFlowMetric',
				params: this.get('current').get('id')
			}, function (response) {

				console.log(response);

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
			var self = this;

			function get_flowlet(id) {
				id = id + "";
				for (var k = 0; k < flowlets.length; k++) {
					if (flowlets[k].name === id) {
						return flowlets[k];
					}
				}
			}

			var cx = App.Controllers.Flow.current.connections;
			var conns = {};
			for (var i = 0; i < cx.length; i ++) {
				if (!conns[cx[i].from.flowlet]) {
					conns[cx[i].from.flowlet] = [];
				}
				conns[cx[i].from.flowlet].push(cx[i].to.flowlet);
			}
			for (var j = 0; j < flowlets.length; j++) {
				if (!conns[flowlets[j].name]) {
					conns[flowlets[j].name] = [];
				}
			}

			var columns = {}, rows = {}, numColumns = 0;
			var column_map = {};

			function append (id, column, connectTo) {
				
				var flowlet = get_flowlet(id);
				var elId;

				var el = $('<div id="flowlet' + id +
					'" class="window' + (column === 0 ? ' source' : '') + '"><strong>' + flowlet.name +
					'</strong><div id="stat' + id + '"></div></div>');
				
				if (columns[column] === undefined) {
					// Create a new column element.
					columns[column] = $('<div class="column clearfix"></div>').appendTo($('#flowviz'));
					rows[column] = 0;
					// Stretch the canvas to fit additional columns.
					$('#flowviz').css({width: (++numColumns * 184) + 'px'});
				}
				// Append the flowlet to the column.
				columns[column].append(el);

				// Associate the column with this flowlet.
				column_map[id] = {
					column: column,
					row: rows[column] ++
				};

				for (var i = 0; i < conns[id].length; i ++) {
					elId = "flowlet" + conns[id][i];
					self.plumber.addEndpoint(elId, self.destspec, {
						anchor: "RightMiddle",
						uuid: elId + "RightMiddle"
					});
				}
				if (conns[id].length > 0) {
					elId = "flowlet" + id;
					self.plumber.addEndpoint(elId, self.sourcespec, {
						anchor: "LeftMiddle",
						uuid: elId + "LeftMiddle"
					});
				}
	
			}

			function connect(from, to) {

				var connector = [ "Bezier", { gap: 0, curviness: 75 } ];
				if (column_map[from].row === column_map[to].row) {
					connector = [ "Flowchart", { gap: 0, stub: 75 } ];
				}

				self.plumber.connect({uuids:['flowlet' + from + 'RightMiddle',
					'flowlet' + to + 'LeftMiddle'],
					connector: connector});
			}

			function connects_to(id) {
				var i;
				
				if (!id) { // Append the first node
					for (i in conns) {
						if (!conns[i] || conns[i].length === 0) {
							append(i, 0);
							connects_to(i);
							break;
						}
					}
				} else {
					for (i in conns) {
						for (var k = 0; k < conns[i].length; k ++) {
							if (conns[i][k] === id) {
								append(i, column_map[id].column + 1);
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