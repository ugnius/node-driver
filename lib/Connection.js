
var net = require('net');
var Message = require('./Message');
var eConsistencyLevel = require('./eConsistencyLevel');
var noop = function() { return undefined; };


var Connection = function Connection(address, port, reconnect, eventsCallback) {
	var i;

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
	this.eventsCallback = eventsCallback || null;
	this.reconnect = reconnect === undefined ? true : reconnect;
	this._reconnectTimeout = null;

	this.buffers = [];
	this.buffersLength = 0;
};


Connection.prototype.connect = function (callback) {
	var self = this;

	console.log('connect ' + this.address);

	this.client = net.connect({host: this.address, port: this.port});

	this.client.on('connect', function() {
		console.log('connected ' + self.address);

		var message = Message.makeStartup({ 'CQL_VERSION': '3.0.0' });

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

			if ( self.eventsCallback ) {
				self.registerForEvents(
					['TOPOLOGY_CHANGE', 'STATUS_CHANGE', 'SCHEMA_CHANGE'], 
					noop,
					self.eventsCallback);
			}
		});

	});

	this.client.on('data', function (data) {
		self.read(data);
	});

	this.client.on('error', function(error) {
		self._disconnected();
		if ( self.reconnect ) {
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

		if ( self.reconnect ) {
			self._reconnect(noop);
		}
	});

};


Connection.prototype._disconnected = function() {
	var stream;
	var streamId;

	for ( streamId in this.liveStreams ) 
	{
		streamId = parseInt(streamId, 10);
		stream = this.liveStreams[streamId];
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
	var time = Math.min( Math.pow(2, self.reconnection - 1) * 1000, 5 * 60 * 1000 );

	self._reconnectTimeout = setTimeout(function() {
		self.connect(callback);
	}, time);
};

Connection.prototype.query = function(query, callback, consistencyLevel)
{
	var self = this;

	var message = Message.makeQuery(query, {
			consistency: consistencyLevel !== undefined ? consistencyLevel : eConsistencyLevel.QUORUM
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
		return callback(new Error('Connection not ready'));
	}

	var streamId = this.availableStreams.shift();

	// TODO, stream QUEUE

	this.liveStreams[streamId] = { streamId: streamId, callback: callback };
	message.stream = streamId;

	this.client.write(message.serializeHeader());
	this.client.write(message.body);
};


Connection.prototype.read = function (data) {
	var self = this;

	var estimatedLength = self.buffers.length === 0
		? data.readUInt32BE(4)
		: self.buffers[0].readUInt32BE(4);
	estimatedLength += 8;

	var currentLength = self.buffersLength + data.length;

	if ( estimatedLength > currentLength ) {
		self.buffers.push(data);
		self.buffersLength += data.length;
		return;
	}

	if ( self.buffersLength !== 0 ) {
		self.buffers.push(data);
		self.buffersLength += data.length;
		data = Buffer.concat(self.buffers, self.buffersLength);

		self.buffers = []
		self.buffersLength = 0;
	}

	var message = Message.deserialize(data);
	var stream = message.stream;

	if ( stream === -1 ) {
		if ( self.eventsCallback !== null ) {
			self.eventsCallback(message.content);
		}
	}
	else {
		var liveStream = this.liveStreams[stream];

		if ( !liveStream ) {
			console.log('no live stream found with stream id: ' + stream);
			return;
		}

		this.liveStreams[stream] = null;
		this.availableStreams.unshift(stream);

		liveStream.callback(null, message);
	}

	if (message.length + 8 < data.length) {
		this.read(data.slice(message.length + 8));
	}
};


Connection.prototype.close = function() {
	console.log('close', this.address, this.port);
	this.reconnect = false;
	clearTimeout(this._reconnectTimeout);
	this.client.end();
};


Connection.prototype.registerForEvents = function(events, callback, eventsCallback) {
	var self = this;

	var message = Message.makeRegister(events);

	self.write(message, function(error, message) {
		if ( error ) {
			return callback(error);
		}

		if ( message.opCode === Message.eOpCode.ERROR ) {
			return callback(message.content, null);
		}

		callback(null);
		self.eventsCallback = eventsCallback;

	});
};


module.exports = Connection;