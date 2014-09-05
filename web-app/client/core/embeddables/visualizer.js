/*
 * Flow DAG Visualizer Embeddable
 */

define([], function () {

	var COLUMN_WIDTH = 226;

	//** Begin Hax
	function ___fixConnections () {
		// Adapt connection format
		var hasSource = false,
			cx = this.get('controller').get('model').connections,
			conns = {};

		for (var i = 0; i < cx.length; i ++) {
			if (!cx[i].from.flowlet) {
				hasSource = true;
			}
			if (!conns[cx[i].to.flowlet]) {
				conns[cx[i].to.flowlet] = [];
			}
			conns[cx[i].to.flowlet].push(cx[i].from.flowlet || cx[i].from.stream);

		}

		var flowlets = this.get('controller').elements.Flowlet;
		for (var j = 0; j < flowlets.length; j++) {
			if (!conns[flowlets[j].name]) {
				conns[flowlets[j].name] = [];
			}
		}

		return conns;
	}
	function ___fixStreams () {

		var flowlets = this.get('controller').elements.Flowlet;
		var fs = this.get('controller').get('model').flowletStreams;

		for (var i in fs) {
			if (Object.prototype.toString.call(fs[i]) === '[object Object]') {

				var flowlet = this.get('controller').get_flowlet(fs[i].name), streams = [];

				for (var j in fs[i]) {
					streams.push(C.Stream.create({
						id: j,
						type: fs[i][j].second,
						url: fs[i][j].first
					}));
				}

				flowlet.streams = streams;
			}
		}
	}
	//** End Hax

	var Embeddable = Em.ContainerView.extend({

		classNames: ['clearfix'],
		elementId: 'flowviz',

		init: function () {
			this._super();

			this.sourcespec = {
				endpoint: 'Dot',
				paintStyle : { radius:1, fillStyle:"#89b086" },
				maxConnections:-1,
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

			this.set('__cxn', {});
			this.set('__columns', {});
			this.set('__rowCounter', {});
			this.set('__location', {});
			this.set('__inserted', {});
			this.set('__numColumns', 0);
			this.set('__positioningWatch', []);

		},

		didInsertElement: function () {
			var flowSources = [];

			// Insert input stream node
			if (this.get('controller').get('model').flowStreams.length) {
				var source;
				var fs = this.get('controller').get('model').flowStreams;
				for (var i = 0; i < fs.length; i ++) {
					source = this.get('controller').get_flowlet(fs[i].name);
					flowSources.push(source);
					this.__append(source, 0);
				}

			}

			//** Begin Hax
			this.__cxn = ___fixConnections.call(this);
			___fixStreams.call(this);
			//** End Hax

			/*
			 * Pretty rendering fix.
			 * Determine whether there are any 'input-less' flowlets, and determine later where they should be rendered.
			 */
			this.get('controller').elements.Flowlet.content.forEach(function (item, index) {
				if (!this.__cxn[item.id] || this.__cxn[item.id].length === 0) {
					this.__positioningWatch.push(item.id);
					this.__location[item.id] = {
						col: 0,
						row: this.__rowCounter[0]
					};
					return false;
				}
			}, this);

			// Kickoff node insertions. Needs to be done for each source.
			for (var i = 0; i < flowSources.length; i ++) {
				this.__insert(flowSources[i].id);
			}

			if (this.__positioningWatch.length) {
				this.__positioningWatch = [];
				this.__insert(null);
			}

			// Vertically center nodes
			var maxHeight = 0, childViews, num, diff, el,
				id, k; //, columns = this.get('childViews');

			var i = this.get('length');
			while (i--) {
				if (this.objectAt(i).get('length') > maxHeight) {
					maxHeight = this.objectAt(i).get('length');
				}
			}

			var i = this.get('length');
			while (i--) {
				childViews = this.objectAt(i);
				num = childViews.get('length');
				diff = maxHeight - num;

				if (childViews.objectAt(0)) {
					childViews.objectAt(0).set('classNames', ['ember-view', 'window', 'window-' + diff]);
				}
			}

			// Draw connections
			for (id in this.__cxn) {
				for (k = 0; k < this.__cxn[id].length; k ++) {
					this.__connect(this.__cxn[id][k], id);
				}
			}
		},

		__positioningWatch: [],

		__insert: function (id) {

			var id2, j, k;

			if (!id) { // Append the first node
				this.get('controller').elements.Flowlet.content.forEach(function (item, index) {

					if (!this.__cxn[item.id] || this.__cxn[item.id].length === 0) {
						this.__append(this.get('controller').get_flowlet(item.id), 0);
						this.__insert(item.id);
						return false;
					}
				}, this);
			} else {

				/*
				 * Pretty rendering fix.
				 * Check whether this connects to an 'input-less' flowlet. If so, render the 'input-less'
				 * flowlet just behind it.
				 */
				for (k = this.__positioningWatch.length - 1; k >= 0; k --) {
					if (this.__cxn[id]) {
						for (j = 0; j < this.__cxn[id].length; j ++) {
							if (this.__cxn[id][j] === this.__positioningWatch[k]) {
								this.__append(this.get('controller').get_flowlet(
									this.__positioningWatch[k]), this.__location[id].col - 1);
								this.__positioningWatch.splice(k, 1);
							}
						}
					}
				}

				/*
				 * Normal rendering.
				 */
				for (id2 in this.__cxn) {
					for (k = 0; k < this.__cxn[id2].length; k ++) {
						if (this.__cxn[id2][k] === id) {
							if (!this.__inserted[id2]) {
								this.__append(this.get('controller').get_flowlet(id2), this.__location[id].col + 1);
								this.__inserted[id2] = 1;

								this.__insert(id2);
							}
						}
					}
				}

			}

		},

		__append: function (model, col) {

			var id, flowletView;

			if (model) {
				id = model.id;
				flowletView = C.Embed.DagNode.create({
					model: model
				});
			} else {
				id = 'dummy';
				model = C.Flowlet.create({
					id: 'dummy',
					name: 'dummy'
				});
				flowletView = C.Embed.EmptyDagNode.create({
					model: model
				});
			}


			if (!this.objectAt(col)) {
				var colView = Em.ContainerView.create({
					classNames: ['column']
				});
				this.pushObject(colView);
				$(this.get('element')).css({width: (++this.__numColumns * COLUMN_WIDTH) + 'px'});

				this.__rowCounter[col] = 0;
			}

			this.__location[id] = {
				col: col,
				row: this.__rowCounter[col] ++
			};

			var colView = this.objectAt(col);
			colView.pushObject(flowletView);

			if (!this.__cxn[id]) {
				return;
			}

			// Use the run loop to ensure nodes have already been appended (previous append)
			Ember.run.next(this, function () {

				var elId;
				for (var i = 0; i < this.__cxn[id].length; i ++) {
					elId = "flowlet" + this.__cxn[id][i];
					this.plumber.addEndpoint(elId, this.destspec, {
						anchor: "RightMiddle",
						uuid: elId + "RightMiddle"
					});
				}
				if (this.__cxn[id].length > 0) {
					elId = "flowlet" + id;
					this.plumber.addEndpoint(elId, this.sourcespec, {
						anchor: "LeftMiddle",
						uuid: elId + "LeftMiddle"
					});
				}

			});

		},

		__connect: function (from, to) {

			var self = this;

			Ember.run.next(this, function () {

				var connector = [ "Bezier", { gap: 0, curviness: 70 } ];

				// If drawing a straight line from LTR, use flowchart renderer
				if (this.__location[from].row === this.__location[to].row) {

					var col1 = this.__location[from].col;
					var col2 = this.__location[to].col;

					if (this.get('childViews')[col1].get('childViews').length ===
						this.get('childViews')[col2].get('childViews').length) {
						connector = [ "Flowchart", { gap: 0, stub: 1 } ];
					}
				}

				// Hax for 3 to 1
				if (this.__location[from].row === 1 && this.__location[to].row === 0) {
					var col1 = this.__location[from].col;
					var col2 = this.__location[to].col;
					if (this.get('childViews')[col1].get('childViews').length === 3 &&
						this.get('childViews')[col2].get('childViews').length === 1) {
						connector = [ "Flowchart", { gap: 0, stub: 1 } ];
					}
				}

				// Hax for 1 to 3
				if (this.__location[from].row === 0 && this.__location[to].row === 1) {
					var col1 = this.__location[from].col;
					var col2 = this.__location[to].col;
					if (this.get('childViews')[col1].get('childViews').length === 1 &&
						this.get('childViews')[col2].get('childViews').length === 3) {
						connector = [ "Flowchart", { gap: 0, stub: 1 } ];
					}
				}


				var color = '#CCC';
				self.plumber.connect({
					paintStyle: { strokeStyle:color, lineWidth:4 },
					uuids:['flowlet' + from + 'RightMiddle',
						'flowlet' + to + 'LeftMiddle'],

					connector: connector
				});

			});
		}
	});

	Embeddable.reopenClass({

		type: 'Visualizer',
		kind: 'Embeddable'

	});
	return Embeddable;

});
