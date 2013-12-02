
var assert = require('assert');
var nodeDriver = require('../index.js');


suite('index', function() {

	suite('objects', function() {
		
		test('should have Cluster object', function() {
			
			assert.equal(true, nodeDriver.hasOwnProperty('Cluster'));

		});
	});
});