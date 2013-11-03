
var util = require('./util');
var eConsistencyLevel = require('./eConsistencyLevel');

var eKind = {
	VOID: 0x0001,
	ROWS: 0x0002,
	SET_KEYSPACE: 0x0003,
	PREPARED: 0x0004,
	SCHEMA_CHANGE: 0x0005
};


var eColumnType = {
	CUSTOM: 0x0000,
	ASCII: 0x0001,
	BIGINT: 0x0002,
	BLOB: 0x0003,
	BOOLEAN: 0x0004,
	COUNTER: 0x0005,
	DECIMAL: 0x0006,
	DOUBLE: 0x0007,
	FLOAT: 0x0008,
	INT: 0x0009,
	TIMESTAMP: 0x000B,
	UUID: 0x000C,
	VARCHAR: 0x000D,
	VARINT: 0x000E,
	TIMEUUID: 0x000F,
	INET: 0x0010,
	LIST: 0x0020,
	MAP: 0x0021,
	SET: 0x0022
};


var eOpCode = {
	ERROR: 0x00,
	STARTUP: 0x01,
	READY: 0x02,
	AUTHENTICATE: 0x03,
	OPTIONS: 0x05,
	SUPPORTED: 0x06,
	QUERY: 0x07,
	RESULT: 0x08,
	PREPARE: 0x09,
	EXECUTE: 0x0A,
	REGISTER: 0x0B,
	EVENT: 0x0C,
	BATCH: 0x0D,
	AUTH_CHALLENGE: 0x0E,
	AUTH_RESPONSE: 0x0F,
	AUTH_SUCCESS: 0x10
};


var key;
var eColumnTypeName = {};
for (key in eColumnType) {
	eColumnTypeName[eColumnType[key]] = key;
}


var eOpName = {};
for (key in eOpCode) {
	eOpName[eOpCode[key]] = key;
}


var Message = function Message(opCode, body) {
	this.version = 2;
	this.compression = false;
	this.tracing = false;
	this.stream = 0;
	this.opCode = opCode;
	this.opName = eOpName[opCode] || ('UNKNOWN:' + opCode);
	this.body = body;
	this.content = null;
};


Message.eOpCode = eOpCode;


Message.prototype.serializeHeader = function() {
	var header = new Buffer(8);

	header[0] = 0x0F & this.version; // assuming searializing only requests
	header[1] = (this.compression ? 0x01 : 0x00) | (this.tracing ? 0x02 : 0x00);
	header.writeInt8(this.stream, 2);
	header.writeInt8(this.opCode, 3);
	header.writeUInt32BE(this.body.length, 4);

	return header;
};


Message.deserialize = function (data) {

	var message = new Message(data[3], data.slice(8));
	message.version = data[0] & 0x0F;
	message.compression = data[1] & 0x01;
	message.tracing = data[1] & 0x02;
	message.stream = data.readInt8(2);
	message.length = data.readUInt32BE(4);
	message.body._o = 0;

	var body;
	var event;
	var change;

	switch (message.opCode) {
		case eOpCode.ERROR:
			message.content = {};
			message.content.errorCode = util.readInt(message.body);
			message.content.errorMessage = util.readString(message.body);
			break;

		case eOpCode.READY:
			break;

		case eOpCode.RESULT:
			var kind = util.readInt(message.body);

			switch(kind) {
				case eKind.VOID:
					break;

				case eKind.ROWS:
					body = message.body.slice(message.body._o);
					body._o = 0;
					message.content = Message._deserializeResultRows(message.body);
					break;

				case eKind.SCHEMA_CHANGE:
					body = message.body.slice(message.body._o);
					body._o = 0;
					change = util.readString(body);
					var keyspace = util.readString(body);
					var table = util.readString(body);
					message.content = change + 
						(table ? ' table ' + keyspace + ' ' + table : ' keyspace ' + keyspace ); 
					break;

				default: 
					console.error('Unknown result kind: ' + kind);
				break;
			}

			break;

		case eOpCode.EVENT:
			event = util.readString(message.body);
			change = util.readString(message.body);
			message.content = {event: event, change: change};
			if ( event === 'TOPOLOGY_CHANGE' || event === 'STATUS_CHANGE' ) {
				message.content.address = util.readInet(message.body).address;
			}
			else {
				message.content.keyspace = util.readString(message.body);
				message.content.table = util.readString(message.body);
			}
			break;

		default:
			console.error('Unknown opCode: ' + message.opCode + ' ' + message.opName);
			break;
	}

	return message;
};


Message._deserializeResultRows = function(body) {
	var flags = util.readInt(body);
	var global_tables_spec = !!(flags & 0x0001);

	var columns_count = util.readInt(body);

	if ( global_tables_spec ) {
		util.readString(body);
		util.readString(body);
	}

	var col_specs = [];
	var i, j;

	var name, typeId, firstTypeId, secondTypeId;

	for ( i = 0; i < columns_count; i++ ) {

		if ( !global_tables_spec ) {
			util.readString(body);
			util.readString(body);
		}
		name = util.readString(body);
		typeId = util.readShort(body);
		firstTypeId = null;
		secondTypeId = null;

		if ( typeId === eColumnType.CUSTOM ) {
			throw new Error('NOT IMPLEMENTED custom row type');
		}

		if ( typeId === eColumnType.LIST || typeId === eColumnType.SET ) {
			firstTypeId = util.readShort(body);
		}

		if ( typeId === eColumnType.MAP ) {
			firstTypeId = util.readShort(body);
			secondTypeId = util.readShort(body);
		}

		col_specs[i] = {
			name: name,
			typeId: typeId,
			firstTypeId: firstTypeId,
			secondTypeId: secondTypeId
		};
	}

	var rows_count = util.readInt(body);
	var result = [];
	var row, valueSize, value;

	for ( i = 0; i < rows_count; i++ ) {
		row = {};

		for ( j = 0; j < columns_count; j++ ) {
			valueSize = util.readInt(body);
			value = null;

			if ( valueSize >= 0 ) {
				value = body.slice(body._o, body._o + valueSize);
				value._o = 0;
				body._o += valueSize;
				value = Message._deserializeValue(col_specs[j].typeId, value, firstTypeId, secondTypeId);
			}

			row[col_specs[j].name] = value;
		}

		result.push(row);
	}

	return result;
};


var handle = {};

handle[eColumnType.ASCII] = function(v) {
	return v.toString('ascii');
};

handle[eColumnType.BIGINT] = function(v) {
	var msb = v.readInt32BE(0);
	var lsb = v.readUInt32BE(4);
	return msb * 4294967296 + lsb;
};

handle[eColumnType.BOOLEAN] = function(v) {
	return !!v[0];
};


handle[eColumnType.COUNTER] = function(v) {
	var msb = v.readInt32BE(0);
	var lsb = v.readUInt32BE(4);
	return msb * 4294967296 + lsb;
};

handle[eColumnType.DECIMAL] = function(v) {
	return v;
};

handle[eColumnType.DOUBLE] = function(v) {
	return v.readDoubleBE(0);
};

handle[eColumnType.FLOAT] = function(v) {
	return v.readFloatBE(0);
};

handle[eColumnType.INET] = function(v) {
	if (v.length === 16) {
		return [
			v.toString('hex', 0, 2),
			v.toString('hex', 2, 4),
			v.toString('hex', 4, 6),
			v.toString('hex', 6, 8),
			v.toString('hex', 8, 10), 
			v.toString('hex', 10, 12), 
			v.toString('hex', 12, 14), 
			v.toString('hex', 14, 16)].join(':');
	}
	return v[0] + '.' + v[1] + '.' + v[2] + '.' + v[3];
};

handle[eColumnType.INT] = function(v) {
	return v.readInt32BE(0);
};

handle[eColumnType.BLOB] = function(v) {
	return v;
};

handle[eColumnType.TIMESTAMP] = function(v) {
	var msb = v.readInt32BE(0);
	var lsb = v.readUInt32BE(4);
	var miliseconds = msb * 4294967296 + lsb;
	return new Date(miliseconds);
};

handle[eColumnType.TIMEUUID] = function(v) {
	return v.toString('hex', 0, 4) +
		'-' + v.toString('hex', 4, 6) +
		'-' + v.toString('hex', 6, 8) +
		'-' + v.toString('hex', 8, 10) +
		'-' + v.toString('hex', 10, 16);
};

handle[eColumnType.UUID] = function(v) {
	return v.toString('hex', 0, 4) +
		'-' + v.toString('hex', 4, 6) +
		'-' + v.toString('hex', 6, 8) +
		'-' + v.toString('hex', 8, 10) +
		'-' + v.toString('hex', 10, 16);
};

handle[eColumnType.VARCHAR] = function(v) {
	return v.toString('utf8');
};

handle[eColumnType.VARINT] = function(v) {
	return v;
};

handle[eColumnType.SET] = function(v, f) {
	var result = [];
	var count = util.readShort(v);
	var i;
	var size, value;

	for ( i = 0; i < count; i++) {
		size = util.readShort(v);
		value = v.slice(v._o, v._o + size);
		v._o += size;
		result.push( Message._deserializeValue( f, value, null, null ) );
	}

	return result;
};

handle[eColumnType.MAP] = function(v, f, s) {
	var result = {};
	var count = util.readShort(v);
	var i;
	var size, value, key;

	for (i = 0; i < count; i++) {
		size = util.readShort(v);
		value = v.slice(v._o, v._o + size);
		v._o += size;
		key = Message._deserializeValue( f, value, null, null );

		size = util.readShort(v);
		value = v.slice(v._o, v._o + size);
		v._o += size;

		result[key] = Message._deserializeValue( s, value, null, null );
	}

	return result;
};


Message._deserializeValue = function(rowTypeId, value, firstTypeId, secondTypeId) {

	if ( !handle[rowTypeId] ) {
		console.log('Unknown typeId ' + rowTypeId + ' ' + eColumnTypeName[rowTypeId] + ' to serialze');
	} else {
		value = handle[rowTypeId](value, firstTypeId, secondTypeId);
	}

	return value;
};


Message.makeStartup = function(options) {
	return new Message(Message.eOpCode.STARTUP, util.buildStringMap(options));
};


Message.makeQuery = function(query, parameters) {

	var consistency = parameters.consistency;
	var flags = 0x00;

	var bodySize = 4 + Buffer.byteLength(query, 'utf-8') + 2 + 1;
	var body = new Buffer(bodySize);
	body._o = 0;

	util.writeLongString(body, query);
	util.writeShort(body, consistency);
	util.writeByte(body, flags);

	return new Message(Message.eOpCode.QUERY, body);
};


Message.makeRegister = function(events) {
	var bodySize = 2;
	events.forEach(function(event) {
		bodySize += 2 + Buffer.byteLength(event, 'utf-8');
	});

	var body = new Buffer(bodySize);
	body._o = 0;

	util.writeShort(body, events.length);
	events.forEach(function(event) {
		util.writeString(body, event);
	});

	return new Message(Message.eOpCode.REGISTER, body);
};


module.exports = Message;
