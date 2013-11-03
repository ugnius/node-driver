
var Connection = require('./Connection');
var Message = require('./Message');
var Metadata = require('./Metadata');
var util = require('./util');
var noop = function() { return undefined; };


var Cluster = function Cluster() {
	this.port = 0;
	this.connections = [];
	this.connectionIndex = 0;
	this.notReadyConnections = 0;
};


Cluster.prototype.getMetadata = function() {
	return new Metadata(this);
};


Cluster.prototype.execute = function(query, callback) {
	var connection = this._nextConnection();

	if ( connection === null ) {
		return callback(new Error('No connections available'));
	}

	connection.query(query, function(error, result) {
		if ( error ) {
			return callback(error);
		}
		callback(null, result);
	});
};


Cluster.prototype._nextConnection = function() {
	if ( this.connectionIndex >= this.connections.length ) {
		this.connectionIndex = 0;
	}

	var connection = this.connections[this.connectionIndex++];

	if ( !connection.ready ) {
		if ( this.notReadyConnections < this.connections.length ) {
			this.notReadyConnections++;
			return this._nextConnection();
		}
		connection = null;
	}

	this.notReadyConnections = 0;
	return connection;
};


Cluster.prototype.shutdown = function() {
	var i;
	for (i = 0; i < this.connections.length; i++ ) {
		this.connections[i].close();
	}
};


Cluster.prototype._handleEvent = function(event) {
	var self = this;
	var connection;

	if ( event.event === 'TOPOLOGY_CHANGE') {
		if ( event.change === 'NEW_NODE' ) {
			connection = null;

			self.connections.forEach(function(c) {
				if (c.address === event.address) {
					connection = c;
				}
			});

			if ( connection === null ) {
				connection = new Connection(event.address, self.port, true, self._handleEvent.bind(self));
				connection.connect(noop);
				self.connections.push(connection);
			}

		}
		if ( event.change === 'REMOVED_NODE' ) {
			connection = null;

			self.connections.forEach(function(c) {
				if (c.address === event.address) {
					connection = c;
				}
			});

			if ( connection !== null ) {
				self.connections.splice( self.connections.indexOf(connection), 1 );
				connection.close();
			}
		}
	}
};


var ClusterBuilder = null;


Cluster.builder = function() {
	return new ClusterBuilder();
};


ClusterBuilder = function ClusterBuilder() {
	this.contactPoints = [];
	this.port = 9042;
};


ClusterBuilder.prototype.addContactPoint = function(node) {
	this.contactPoints.push(node);
	return this;
};


ClusterBuilder.prototype.addContactPoints = function(nodes) {
	this.contactPoints = this.contactPoints.concat(nodes);
	return this;
};


ClusterBuilder.prototype.withPort = function(port) {
	this.port = port;
	return this;
};


ClusterBuilder.prototype.build = function(callback) {
	this._tryContactPoints(this.contactPoints, callback);
};


ClusterBuilder.prototype._tryContactPoints = function(nodes, callback) {
	if ( nodes.length === 0 ) {
		callback(new Error('No live nodes to connect to'), null);
	}

	var self = this;

	var cluster = new Cluster();
	cluster.port = self.port;

	var node = nodes[0];
	var connection = new Connection(node, this.port, false, cluster._handleEvent.bind(cluster));

	connection.connect(function(error) {
		if ( error ) {
			return self._tryContactPoints( nodes.slice(1), callback );
		}

		connection.retry = true;

		connection.query('SELECT * FROM system.peers', function(error, result) {
			if ( error ) {
				return callback(error, null);
			}

			var peers = result.map(function(i) { return i.rpc_address; });

			cluster.connections.push(connection);

			peers.forEach(function(address) {
				connection = new Connection(address, self.port, true, cluster._handleEvent.bind(cluster));
				connection.connect(noop);
				cluster.connections.push(connection);
			});

			callback(null, cluster);
		});

	}, false);

};


module.exports = Cluster;