
var rewire = require('rewire');
var assert = require('assert');


suite('Message', function() {
	var Message;

	setup(function() {
		Message = rewire('../lib/Message.js');
	});

	suite('_deserializeValue()', function() {
		var _deserializeValue;
		
		setup(function() {
			_deserializeValue = Message._deserializeValue;

			Message.__set__({
				handle: {1: function(v, f, s) {return 2;}}
			});
		});
		
		test('should return value from handle function', function() {
			var value = new Buffer('');
			assert.equal(_deserializeValue(value, 1, 2, 3), 2);
		});

	});

	suite('handle[eColumnType.ASCII]', function() {
		var handle;
		
		setup(function() {
			handle = Message.__get__('handle')[Message.__get__('eColumnType').ASCII];
		});
		
		test('should return decoded ascii', function() {
			var value;
			value = new Buffer('202122232425262728292a2b2c2d2e2f', 'hex');
			assert.equal(handle(value), ' !"#$%&\'()*+,-./');
			value = new Buffer('f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff', 'hex');
			assert.equal(handle(value), '����������������');
		});
	});


	suite('handle[eColumnType.BIGINT]', function() {
		var handle;
		
		setup(function() {
			handle = Message.__get__('handle')[Message.__get__('eColumnType').BIGINT];
		});
		
		test('should return decoded BIGINT', function() {
			var value;
			value = new Buffer('0000000000000000', 'hex');
			assert.equal(handle(value), 0);
			value = new Buffer('0000000000000001', 'hex');
			assert.equal(handle(value), 1);
			value = new Buffer('0000000100000001', 'hex');
			assert.equal(handle(value), 4294967297);
			value = new Buffer('00000001ffffffff', 'hex');
			assert.equal(handle(value), 8589934591);
			value = new Buffer('001FFFFFFFFFFFFF', 'hex');
			assert.equal(handle(value), 9007199254740991);
			value = new Buffer('FFFFFFFFFFFFFFFF', 'hex');
			assert.equal(handle(value), -1);
			value = new Buffer('FFFFFFFFFFFFDCD7', 'hex');
			assert.equal(handle(value), -9001);
		});
	});

	suite('handle[eColumnType.BOOLEAN]', function() {
		var handle;
		
		setup(function() {
			handle = Message.__get__('handle')[Message.__get__('eColumnType').BOOLEAN];
		});
		
		test('should return decoded BOOLEAN', function() {
			var value;
			value = new Buffer('00', 'hex');
			assert.equal(handle(value), false);
			value = new Buffer('01', 'hex');
			assert.equal(handle(value), true);
			value = new Buffer('FF', 'hex');
			assert.equal(handle(value), true);
		});
	});

	suite('handle[eColumnType.COUNTER]', function() {
		var handle;
		
		setup(function() {
			handle = Message.__get__('handle')[Message.__get__('eColumnType').COUNTER];
		});
		
		test('should return decoded COUNTER', function() {
			var value;
			value = new Buffer('0000000000000000', 'hex');
			assert.equal(handle(value), 0);
			value = new Buffer('0000000000000001', 'hex');
			assert.equal(handle(value), 1);
			value = new Buffer('0000000100000001', 'hex');
			assert.equal(handle(value), 4294967297);
			value = new Buffer('00000001ffffffff', 'hex');
			assert.equal(handle(value), 8589934591);
			value = new Buffer('001FFFFFFFFFFFFF', 'hex');
			assert.equal(handle(value), 9007199254740991);
			value = new Buffer('FFFFFFFFFFFFFFFF', 'hex');
			assert.equal(handle(value), -1);
			value = new Buffer('FFFFFFFFFFFFDCD7', 'hex');
			assert.equal(handle(value), -9001);
		});
	});


	suite('handle[eColumnType.INET]', function() {
		var handle;
		
		setup(function() {
			handle = Message.__get__('handle')[Message.__get__('eColumnType').INET];
		});
		
		test('should return decoded INET', function() {
			var value;
			value = new Buffer('00000000', 'hex');
			assert.equal(handle(value), '0.0.0.0');
			value = new Buffer('ffffffff', 'hex');
			assert.equal(handle(value), '255.255.255.255');
			value = new Buffer('00000000000000000000000000000000', 'hex');
			assert.equal(handle(value), '0000:0000:0000:0000:0000:0000:0000:0000');
			value = new Buffer('ffffffffffffffffffffffffffffffff', 'hex');
			assert.equal(handle(value), 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff');
		});
	});


	suite('handle[eColumnType.TIMESTAMP]', function() {
		var handle;
		
		setup(function() {
			handle = Message.__get__('handle')[Message.__get__('eColumnType').TIMESTAMP];
		});
		
		test('should return decoded TIMESTAMP as big int', function() {
			var value, date;
			value = new Buffer('0000000000000000', 'hex');
			assert.equal(handle(value), '0');
			value = new Buffer('00000001ffffffff', 'hex');
			assert.equal(handle(value), 8589934591);
		});
	});




});