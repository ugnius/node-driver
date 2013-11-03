
var net = require('net');
var Message = require('./Message');
var eConsistencyLevel = require('./eConsistencyLevel');

var Connection = function Connection(address, port) {
	this.address = address;
	this.port = port;
	this.client = null;
	this.ready = false;
	this.availableStreams = [];
	for ( i = 0; i < 128; i++ ) {
		this.availableStreams.push(i);
	}
	this.liveStreams = {};
	this.reconnection = 0;
};

Connection.prototype.connect = function (callback, retry) {
	var self = this;

	if ( retry === undefined ) {
		retry = true;
	}

	console.log('connect ' + this.address);

	this.client = net.connect({host: this.address, port: this.port});

	this.client.on('connect', function() {
		console.log('connected ' + self.address);

		var message = new Message(Message.eOpCode.STARTUP, util.buildStringMap({ 'CQL_VERSION': '3.0.0' }));

		self.write( message, function(error, message) {
			if ( error ) {
				return callback(error);
			}
			if ( message.opCode !== Message.eOpCode.READY ) {
				return callback(new Error('GOT NOT READY TO STARTUP'));
			}

			console.log('ReADY ' + self.address + ' ' + self.port);
			self.ready = true;
			self.reconnection = 0;

			callback(null);
		});

	});

	this.client.on('data', function (data) {
		self.read(data);
	});

	this.client.on('error', function(error) {
		self._disconnected();
		if ( retry ) {
			self._reconnect(callback);
		}
		else {
			callback(error);
		}
	});

	this.client.on('end', function() {
		console.log(self.address + ' disconnected');
		self.ready = false;

		self._disconnected();
		self._reconnect(function() {});
	});

};


Connection.prototype._disconnected = function() {
	var stream;

	for ( streamId in this.liveStreams ) 
	{
		streamId = parseInt(streamId, 10);
		var stream = this.liveStreams[streamId];

		console.log(typeof streamId, streamId, stream);

		if ( stream === null ) {
			continue;
		}

		this.availableStreams.unshift(stream.streamId);
		stream.callback(new Error('Client disconnected'), null);
	}

	this.liveStreams = {};

	// TODO, stream QUEUE
};


Connection.prototype._reconnect = function(callback) {
	var self = this;

	self.reconnection++;
	var time = Math.min( Math.pow(2, self.reconnection - 1) * 1000, 1 * 60 * 1000 );
	// console.log(self.reconnection, time);

	setTimeout(function() {
		self.connect(callback, true);
	}, time);
};

Connection.prototype.query = function(query, callback, consistencyLevel)
{
	var self = this;

	// TODO: daufault consistency level
	var message = Message.makeQuery(query, {
			consistency: eConsistencyLevel.QUORUM
		});

	self.write(message, function(error, message) {
		if ( error ) {
			return callback(error);
		}

		if ( message.opCode === Message.eOpCode.ERROR ) {
			return callback(message.content, null);
		}

		callback(null, message.content);
	});

};

Connection.prototype.write = function (message, callback) {

	var self = this;

	if ( !self.ready && message.opCode !== Message.eOpCode.STARTUP ) {

		console.log(self.ready, message.opCode);
		return callback(new Error('Connection not ready'));
	}

	var streamId = this.availableStreams.shift();

	// TODO, stream QUEUE

	// console.log('write', this.address, this.ready, streamId);

	this.liveStreams[streamId] = { streamId: streamId, callback: callback };

	// console.log('W ' + this.address + ' ' + stream);

	message.stream = streamId;

	this.client.write(message.serializeHeader());
	this.client.write(message.body);

};


var buffer = null;

Connection.prototype.read = function (data) {

	if ( buffer !== null ) {
		// TODO, add to array
		data = Buffer.concat([buffer, data]);
		buffer = null;
	}

	var estimatedLength = data.readUInt32BE(4);

	if ( estimatedLength + 8 > data.length ) {
		buffer = data;
		return;
	}

	var message = Message.deserialize(data);
	var stream = message.stream;

	// console.log('R ' + this.address + ' ' + stream);

	var liveStream = this.liveStreams[stream];

	if ( !liveStream ) {
		console.log('no live stream found with stream id: ' + stream);
		return;
	}

	this.liveStreams[stream] = null;
	this.availableStreams.unshift(stream);

	liveStream.callback(null, message);

	if (message.length + 8 < data.length) {
		this.read(data.slice(message.length + 8));
	}
}

Connection.prototype.close = function() {
	console.log('close', this.address, this.port);
	this.client.end();
};

module.exports = Connection;