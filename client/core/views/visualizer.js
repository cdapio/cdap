
define([], function () {

	var COLUMN_WIDTH = 198;

	//** Begin Hax
	function ___fixConnections (flowSource) {
		// Adapt connection format
		var hasSource = false,
			cx = this.get('controller').current.connections,
			conns = {};
		for (var i = 0; i < cx.length; i ++) {
			if (!cx[i].from.flowlet) {
				hasSource = true;
			}
			if (!conns[cx[i].to.flowlet]) {
				conns[cx[i].to.flowlet] = [];
			}
			conns[cx[i].to.flowlet].push(cx[i].from.flowlet || flowSource.name);
		}

		var flowlets = this.get('controller').types.Flowlet;
		for (var j = 0; j < flowlets.length; j++) {
			if (!conns[flowlets[j].name]) {
				conns[flowlets[j].name] = [];
			}
		}
		return conns;
	}
	function ___fixStreams () {

		var flowlets = this.get('controller').types.Flowlet;
		var i, k, j, fs = this.get('controller').current.flowletStreams;
		for (i in fs) {

			var flowlet = this.get('controller').get_flowlet(i), streams = [];

			for (j in fs[i]) {
				streams.push(C.Mdl.Stream.create({
					id: j,
					type: fs[i][j].second,
					url: fs[i][j].first
				}));
			}
			flowlet.streams = streams;
		}
	}
	//** End Hax

	return Em.ContainerView.extend({
		
		classNames: ['clearfix'],
		elementId: 'flowviz',

		init: function () {

			this._super();

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

		drawGraph: function () {

			var flowSource = null;

			// Insert input stream node
			if (this.get('controller').current.flowStreams.length) {
				flowSource = C.Mdl.Stream.create(this.get('controller').current.flowStreams[0]);
				this.get('controller').types.Stream.pushObject(flowSource);
				this.__append(flowSource, 0);
			}

			//** Begin Hax
			this.__cxn = ___fixConnections.call(this, flowSource);
			___fixStreams.call(this);
			//** End Hax

			// Kickoff node insertions
			this.__insert(flowSource ? flowSource.id : null);

			// Vertically center nodes
			var maxHeight = 0, childViews, num, diff, el,
				id, k, columns = this.get('childViews');

			var i = columns.length;
			while (i--) {
				if (columns[i].get('childViews').length > maxHeight) {
					maxHeight = columns[i].get('childViews').length;
				}
			}
			var i = columns.length;
			while (i--) {
				childViews = columns[i].get('childViews');
				num = childViews.length;
				diff = maxHeight - num;
				if (childViews[0]) {
					childViews[0].set('classNames', ['ember-view', 'window', 'window-' + diff]);
				}
			}

			// Draw connections
			for (id in this.__cxn) {
				for (k = 0; k < this.__cxn[id].length; k ++) {
					this.__connect(this.__cxn[id][k], id);
				}
			}
		},

		__cxn: {},
		__columns: {},
		__rowCounter: {},
		__location: {},
		__numColumns: 0,

		__insert: function (id) {
			
			var id2, k;
			if (!id) { // Append the first node
				this.get('controller').types.Flowlet.content.forEach(function (item, index) {
					if (!this.__cxn[item.id] || this.__cxn[item.id].length === 0) {
						this.__append(this.get('controller').get_flowlet(item.id), 0);
						this.__insert(item.id);
						return false;
					}
				}, this);
			} else {
				for (id2 in this.__cxn) {
					for (k = 0; k < this.__cxn[id2].length; k ++) {
						if (this.__cxn[id2][k] === id) {
							this.__append(this.get('controller').get_flowlet(id2), this.__location[id].col + 1);
							this.__insert(id2);
						}
					}
				}
			}
		},

		__append: function (entity, col) {

			var id = entity.id,
				flowletView = C.Vw.DagNode.create({
					current: entity
				});

			if (!this.get('childViews')[col]) {
				var colView = Em.ContainerView.create({
					classNames: ['column']
				});
				this.get('childViews').pushObject(colView);
				$(this.get('element')).css({width: (++this.__numColumns * COLUMN_WIDTH) + 'px'});

				this.__rowCounter[col] = 0;
			}

			this.__location[id] = {
				col: col,
				row: this.__rowCounter[col] ++
			};

			var colView = this.get('childViews')[col];
			colView.get('childViews').pushObject(flowletView);

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

				var connector = [ "Bezier", { gap: -5, curviness: 75 } ];

				// If drawing a straight line from LTR, use flowchart renderer
				if (this.__location[from].row === this.__location[to].row) {

					var col1 = this.__location[from].col;
					var col2 = this.__location[to].col;

					if (this.get('childViews')[col1].get('childViews').length ===
						this.get('childViews')[col2].get('childViews').length) {
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
	});
});
