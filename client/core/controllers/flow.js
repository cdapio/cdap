//
// Flow Controller. Content is an array of flowlets.
//

define([], function () {

	var COLUMN_WIDTH = 198;

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

		get_flowlet: function (id) {
			id = id + "";
			for (var k = 0; k < this.content.length; k++) {
				if (this.content[k].name === id) {
					return this.content[k];
				}
			}
		},

		init: function () {

			this.sourcespec = {
				endpoint: 'Dot',
				paintStyle : { radius:1, fillStyle:"#89b086" },
				isSource:true
			};

			this.destspec = {
				endpoint: 'Dot',
				paintStyle : { radius:1, fillStyle:"#89b086" },
				maxConnections:-1,
				isTarget:true
			};

			this.plumber = jsPlumb.getInstance();

			this.plumber.importDefaults({
				
			});
		},

		unload: function () {

			clearTimeout(this.updateTimeout);

			this.set('content', []);
			this.history.set('content', []);
			this.clear();
			this.history.clear();

			this.set('current', null);

			clearInterval(this.interval);

		},

		load: function (app, id) {

			var self = this;

			C.get('manager', {
				method: 'getFlowHistory',
				params: [app, id]
			}, function (error, response) {
				var hist = response.params;

				if (!hist) {
					return;
				}

				// This reverses and limits the history. API will handle this in the future.
				var length = (hist.length < 10 ? 0 : hist.length - 10);

				for (var i = hist.length - 1; i >= length; i --) {

					hist[i] = C.Mdl.Run.create(hist[i]);

					if (hist[i].endStatus !== 'STOPPED') {
						continue;
					}

					self.history.pushObject(hist[i]);

				}
			});

			C.interstitial.loading();

			//
			// Request Flow Definition
			//
			C.get('manager', {
				method: 'getFlowDefinition',
				params: [app, id]
			}, function (error, response) {

				if (error) {
					C.interstitial.label(error);
					return;
				}

				if (!response.params) {
					C.interstitial.label('Flow not found.', {
						action: 'All Flows',
						click: function () {
							C.router.set('location', '');
						}
					});
					return;
				}

				response.params.currentState = 'UNKNOWN';
				response.params.version = -1;
				response.params.type = 'Flow';
				
				self.set('current', C.Mdl.Flow.create(response.params));
	
				var flowlets = response.params.flowlets;
				for (var i = 0; i < flowlets.length; i ++) {
					self.pushObject(C.Mdl.Flowlet.create(flowlets[i]));
				}

				self.drawNodes();

				//
				// Request Flow Status
				//
				C.get('manager', {
					method: 'status',
					params: [app, id, -1]
				}, function (error, response) {

					if (response.params) {

						self.set('currentRun', response.params.runId.id);

						self.get('current').set('currentState', response.params.status);
						self.get('current').set('version', response.params.version);

						self.updateStats(self.get('run'));

						C.interstitial.hide();
					
					} else {
						C.interstitial.label('Unable to get Flow Status.');
					}

					self.interval = setInterval(function () {
						self.refresh();
					}, 1000);

				});

			});

		},
		refresh: function () {

			var self = this;
			var app = this.get('current').get('meta').app;
			var id = this.get('current').get('meta').name;

			if (C.Ctl.Flows.pending) {
				return;
			}

			C.get('manager', {
				method: 'status',
				params: [app, id, -1]
			}, function (error, response) {

				if (response.params && self.get('current')) {
					self.get('current').set('currentState', response.params.status);
					if (response.params.status === 'RUNNING') {
						C.interstitial.hide();
					}
				}
			});
		},
		
		startStats: function () {
			var self = this;
			clearTimeout(this.updateTimeout);
			this.updateTimeout = setTimeout(function () {
				self.updateStats(self.get('run'));
			}, 1000);
		},

		updateStats: function (for_run_id) {
			var self = this;

			if (!this.get('current')) {
				self.startStats();
				return;
			}

			var app = this.get('current').get('meta').app;
			var id = this.get('current').get('meta').name;
			var run = for_run_id || this.get('currentRun');

			if (!for_run_id && self.get('current').get('currentState') !== 'RUNNING') {
				self.startStats();
				return;
			}

			var end = Math.round(new Date().getTime() / 1000),
				start = end - 30;
			
			C.get.apply(C, this.get('current').getUpdateRequest());

			C.get('monitor', {
				method: 'getCounters',
				params: [app, id, run]
			}, function (error, response) {
			
				if (C.router.currentState.name !== "flow") {
					return;
				}

				if (!response.params) {
					return;
				}

				var metrics = response.params;
				for (var i = 0; i < metrics.length; i ++) {

					if (metrics[i].name !== 'processed.count') {
						continue;
					}

					var finish = metrics[i].value;
					var flowlet = self.get_flowlet(metrics[i].qualifier);

					var fs = flowlet.streams;
					for (var j = 0; j < fs.length; j ++) {
						fs[j].set('metrics', finish);
					}

					finish = C.util.number(finish);
					flowlet.set('processed', finish);

				}

				if (!for_run_id) {
					self.startStats();
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

			var flowlets = C.Ctl.Flow.content;
			var self = this;

			var flowSource;
			if (this.current.flowStreams.length) {
				flowSource = this.current.flowStreams[0];
				flowSource.isSource = true;
			}

			// Adapt connection format

			var hasSource = false;

			var cx = C.Ctl.Flow.current.connections;
			var conns = {};
			for (var i = 0; i < cx.length; i ++) {
				if (!cx[i].from.flowlet) {
					hasSource = true;
				}
				if (!conns[cx[i].to.flowlet]) {
					conns[cx[i].to.flowlet] = [];
				}
				conns[cx[i].to.flowlet].push(cx[i].from.flowlet || flowSource.name);
			}
			for (var j = 0; j < flowlets.length; j++) {
				if (!conns[flowlets[j].name]) {
					conns[flowlets[j].name] = [];
				}
			}

			// Adapt flowstream format

			var fs = C.Ctl.Flow.current.flowletStreams;
			
			for (i in fs) {

				var flowlet, streams = [];
				for (var k = 0; k < C.Ctl.Flow.content.length; k ++) {
					if (C.Ctl.Flow.content[k].name === i) {
						flowlet = C.Ctl.Flow.content[k];
						break;
					}
				}
				for (j in fs[i]) {
					streams.push(C.Mdl.Stream.create({
						id: j,
						type: fs[i][j].second,
						url: fs[i][j].first
					}));
				}

				flowlet.streams = streams;
				
			}

			var columns = {}, rows = {}, numColumns = 0;
			var column_map = {};

			function append (id, column, connectTo) {

				var flowlet = self.get_flowlet(id);

				if (columns[column] === undefined) {
					// Create a new column view.
					columns[column] = Em.ContainerView.create({
						classNames: ['column']
					});

					// Attach it to and expand the visualizer.
					var viz = C.router.applicationController.view.get('visualizer');
					viz.get('childViews').pushObject(columns[column]);
					$(viz.get('element')).css({width: (++numColumns * COLUMN_WIDTH) + 'px'});

					rows[column] = 0;
				}

				columns[column].get('childViews').pushObject(C.Vw.DagNode.create({
					current: flowlet
				}));

				// Associate the column with this flowlet.
				column_map[id] = {
					column: column,
					row: rows[column] ++
				};

				if (!conns[id]) {
					return;
				}

				// Use the run loop to ensure nodes have already been appended (previous append)
				Ember.run.next(this, function () {

					var elId;
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

				});
	
			}

			function connect(from, to) {

				Ember.run.next(this, function () {

					var connector = [ "Bezier", { gap: -5, curviness: 75 } ];

					// If drawing a straight line from LTR, use flowchart renderer
					if (column_map[from].row === column_map[to].row) {

						var col1 = column_map[from].column;
						var col2 = column_map[to].column;

						if (columns[col1]._childViews.length === columns[col2]._childViews.length) {
							connector = [ "Flowchart", { gap: 0, stub: 1 } ];
						}
					}
					

					var color = '#CCC';
					self.plumber.connect({
						paintStyle: { strokeStyle:color, lineWidth:6 },
						uuids:['flowlet' + from + 'RightMiddle',
							'flowlet' + to + 'LeftMiddle'],
						
						connector: connector
					});

				});

			}

			function bind_to(id) {
				var i;
				
				if (!id) { // Append the first node
					for (i in conns) {
						if (!conns[i] || conns[i].length === 0) {
							append(i, 0);
							bind_to(i);
							break;
						}
					}
				} else {
					for (i in conns) {
						for (var k = 0; k < conns[i].length; k ++) {
							if (conns[i][k] === id) {
								append(i, column_map[id].column + 1, id);
								//connect(id, i);
								bind_to(i);
							}
						}
					}
				}
			}

			if (hasSource) {

				this.pushObject(flowSource);
				append(flowSource.name, 0); // Attach the Input Stream

			}

			bind_to(hasSource ? flowSource.name : null);

			// Find max nodes in any column
			var maxHeight = 0;
			for (var i in columns) {
				if (columns[i]._childViews.length > maxHeight) {
					maxHeight = columns[i]._childViews.length;
				}
			}

			// Vertically center nodes
			for (var i in columns) {
				var num = columns[i]._childViews.length;
				var diff = maxHeight - num;
				var el = columns[i]._childViews[0].set('classNames', ['ember-view', 'window', 'window-' + diff]);
			}

			// Draw connections
			for (var i in conns) {
				for (var k = 0; k < conns[i].length; k ++) {
					connect(conns[i][k], i);
				}
			}

		}
	});
});