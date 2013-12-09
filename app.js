
var Cluster = require('../node-driver').Cluster;

var cluster = null;

new Cluster.builder()
	.addContactPoints(['192.168.1.177'])
	.withPort(9042)
	.build(function(error, _cluster) {
		if (error) {
			console.log(error.message);
			process.exit(1);
		}

		cluster = _cluster;
		var metadata = cluster.getMetadata();

		metadata.getClusterName(function(error, clusterName) {
			console.log(error, clusterName);

			create_keyspace_test(function(error) {
				test_table_simple_types(function(error) {
				});
			});

			loopFunction(5 * 1000, test_query);

		});
	});

function create_keyspace_test(callback) {
	var query = 'CREATE KEYSPACE IF NOT EXISTS ' +
		'test WITH replication = {\'class\':\'SimpleStrategy\', \'replication_factor\':2}';

	cluster.execute(query, function(error, result) {
		console.log(error, result);
		callback(error);
	});
}

function test_table_simple_types(callback) {
	var query = 'CREATE TABLE IF NOT EXISTS test.test_simple_types ( ' +
		'ascii ascii, ' + 
		'bigint bigint, ' + 
		'blob blob, ' + 
		'boolean boolean, ' + 
		'decimal decimal, ' + 
		'double double, ' + 
		'float float, ' + 
		'inet inet, ' + 
		'int int, ' + 
		'text text, ' + 
		'timestamp timestamp, ' + 
		'timeuuid timeuuid, ' + 
		'uuid uuid, ' + 
		'varchar varchar, ' + 
		'varint varint, ' + 
		'PRIMARY KEY (ascii) )';

	cluster.execute(query, function(error, result) {
		console.log(error, result);

		var query = 'INSERT INTO test.test_simple_types ' +
			'(ascii, bigint, blob, boolean, decimal, double, float, inet, int, text, timestamp, timeuuid, uuid, varchar, varint) ' +
			'VALUES (\'test\', 4294967297, 0x0123456789ABCDEF, true, 42949.67296, 42949.672961, 42949.67296, ' +
			'\'2001:db8::ff00:42:8329\', -2147483648, \'a@ąве́ди神\', \'2011-02-03T04:05:01+0000\', ' +
			'maxTimeuuid(\'2011-02-03T04:05:01+0000\'), c593fb8f-2f4a-11e0-7f7f-7f7f7f7f7f7f, \'varchar\', 4294967297 )';
		cluster.execute(query, function(error, result) {
			console.log(error, result);

			var query = 'SELECT * FROM test.test_simple_types';
			cluster.execute(query, function(error, result) {
				console.log(error, result);

				callback(error);
			});
		});
	});
}

function test_table_simple_types(callback) {
	var query = 'CREATE TABLE IF NOT EXISTS test.test_counters ( ' +
		'id text PRIMARY KEY, counter counter )';

	cluster.execute(query, function(error, result) {
		console.log(error, result);

		var query = 'UPDATE test.test_counters SET counter = counter + 4294967297 WHERE id = \'id\'';
		cluster.execute(query, function(error, result) {
			console.log(error, result);

			var query = 'SELECT * FROM test.test_counters';
			cluster.execute(query, function(error, result) {
				console.log(error, result);

				callback(error);
			});
		});
	});
}

function test_query(callback) {
	var query = 'SELECT * FROM system.local';

	cluster.execute(query, function(error, result) {
		console.log(error, result && result.length);
		callback(error, result);
	});
}

function loopFunction(timer, func) {
	func(function() {

		setTimeout(function() {
			loopFunction(timer, func);
		}, timer);
	});
}
