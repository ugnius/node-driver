
var Metadata = function Metadata(cluster) {
	this._cluster = cluster;
};

Metadata.prototype.exportSchemaAsString = function () {

};

Metadata.prototype.getAllHosts = function (callback) {

	this._cluster.connections[0].query('SELECT * FROM system.peers', function(error, result) {
		if ( error ) {
			return callback(error);
		}

		callback(null, result);

	});

};

Metadata.prototype.getClusterName = function (callback) {

	this._cluster.connections[0].query('SELECT * FROM system.local', function(error, result) {
		if ( error ) {
			return callback(error);
		}

		callback(null, result[0].cluster_name);
	});

};


module.exports = Metadata;