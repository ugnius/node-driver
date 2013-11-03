
var Connection = require('./Connection');
var Message = require('./Message');
var Metadata = require('./Metadata');
var util = require('./util');

var Cluster = function Cluster() {

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
	};

	var connection = this.connections[this.connectionIndex++];

	if ( !connection.ready ) {
		if ( this.notReadyConnections < this.connections.length ) {
			this.notReadyConnections++;
			return this._nextConnection();
		}
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

Cluster.builder = function() {
	return new ClusterBuilder();
};


var ClusterBuilder = function ClusterBuilder() {
	this.contactPoints = [];
	this.port = 9042;
};

ClusterBuilder.prototype.addContactPoint = function(node) {
	// console.log('node ' + node);
	this.contactPoints.push(node);
	return this;
};

ClusterBuilder.prototype.addContactPoints = function(nodes) {
	this.contactPoints = this.contactPoints.concat(nodes);
	return this;
};

ClusterBuilder.prototype.withCompression = function(compression) {
	return this;
};

ClusterBuilder.prototype.withCredentials = function(username, password) {
	return this;
};

ClusterBuilder.prototype.withPort = function(port) {
	this.port = port;
	return this;
};

ClusterBuilder.prototype.withReconnectionPolicy = function(policy) {
	return this;
};

ClusterBuilder.prototype.withRetryPolicy = function(policy) {
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
	var node = nodes[0];

	var connection = new Connection(node, this.port);

	connection.connect(function(error) {
		if ( error ) {
			return self._tryContactPoints( nodes.slice(1), callback );
		}

		connection.query('SELECT * FROM system.peers', function(error, result) {
			if ( error ) {
				return callback(error, null);
			}

			var peers = result.map(function(i) { return i.rpc_address; });

			var cluster = new Cluster();
			cluster.connections.push(connection);

			peers.forEach(function(address) {
				var connection = new Connection(address, self.port);
				connection.connect(function() {});
				cluster.connections.push(connection);
			});

			callback(null, cluster);
		});

	}, false);



};

module.exports = Cluster;