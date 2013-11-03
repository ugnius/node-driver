
var util = require('./util');
var eConsistencyLevel = require('./eConsistencyLevel');

var Message = function Message(opCode, body) {
	this.version = 2;
	this.compression = false;
	this.tracing = false;
	this.stream = 0;
	this.opCode = opCode;
	this.opName = Message.eOpName[opCode] || ('UNKNOWN:' + opcode);
	this.body = body;
	this.content = null;
};

var eKind = {
	VOID: 0x0001,
	ROWS: 0x0002,
	SET_KEYSPACE: 0x0003,
	PREPARED: 0x0004,
	SCHEMA_CHANGE: 0x0005
};

var eRowType = {
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

var eRowTypeName = {};
for (var key in eRowType) {
	eRowTypeName[eRowType[key]] = key;
}

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

Message.eOpCode = eOpCode;

Message.eOpName = {};
for (var key in Message.eOpCode) {
	Message.eOpName[Message.eOpCode[key]] = key;
}

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
	message.stream = data[2];
	message.length = data.readUInt32BE(4);
	message.body._o = 0;

	// console.log( message.length, data.length );

	switch (message.opCode) {
		case Message.eOpCode.ERROR:
			message.content = {};
			message.content.errorCode = util.readInt(message.body);
			message.content.errorMessage = util.readString(message.body);
			break;
		case Message.eOpCode.READY:
			break;
		case Message.eOpCode.RESULT:
			var kind = util.readInt(message.body);

			switch(kind) {
				case eKind.VOID:
					break;

				case eKind.ROWS:
					var body = message.body.slice(message.body._o);
					body._o = 0;
					message.content = Message._deserializeResultRows(body);
					break;

				case eKind.SCHEMA_CHANGE:
					var body = message.body.slice(message.body._o);
					body._o = 0;
					var change = util.readString(body);
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
		default:
			console.error('Unknown opCode: ' + message.opCode + ' ' + message.opName);
			break;
	};

	return message;
};

Message._deserializeResultRows = function(body) {
	var flags = util.readInt(body);
	var global_tables_spec = !!(flags & 0x0001);
	var has_more_pages = !!(flags & 0x0002);
	var no_metadata = !!(flags & 0x0004)

	var columns_count = util.readInt(body);

	var keyspace = '';
	var table = '';

	if ( global_tables_spec ) {
		keyspace = util.readString(body);
		table = util.readString(body);
	}

	var col_specs = [];

	for ( var i = 0; i < columns_count; i++ ) {

		var ksname = '';
		var tablename = '';
		if ( !global_tables_spec ) {
			keyspace = util.readString(body);
			table = util.readString(body);
		}
		var name = util.readString(body);
		var typeId = util.readShort(body);
		var firstTypeId = null;
		var secondTypeId = null;

		if ( typeId === eRowType.CUSTOM ) {
			throw new Error('NOT IMPLEMENTED custom row type');
		}

		if ( typeId === eRowType.LIST || typeId === eRowType.SET ) {
			firstTypeId = util.readShort(body);
		}

		if ( typeId === eRowType.MAP ) {
			firstTypeId = util.readShort(body);
			secondTypeId = util.readShort(body);
		}

		col_specs[i] = {
			name: name,
			typeId: typeId,
			firstTypeId: firstTypeId,
			secondTypeId: secondTypeId
		}
	}

	var rows_count = util.readInt(body);
	var result = [];

	for ( var i = 0; i < rows_count; i++ ) {
		var row = {};

		for ( var j = 0; j < columns_count; j++ ) {
			var valueSize = util.readInt(body);
			var value = null;

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

handle[eRowType.ASCII] = function(v) {
	return v.toString('ascii');
};

handle[eRowType.BIGINT] = function(v) {
	// TODO: using this will loose precision
	var msb = v.readInt32BE(0);
	var lsb = v.readUInt32BE(4);
	return msb * 4294967296 + lsb;
};

handle[eRowType.BOOLEAN] = function(v) {
	return !!v[0];
};


handle[eRowType.COUNTER] = function(v) {
	// TODO: using this will loose precision
	var msb = v.readInt32BE(0);
	var lsb = v.readUInt32BE(4);
	return msb * 4294967296 + lsb;
};

handle[eRowType.DECIMAL] = function(v) {
	// TODO: decimal are stupid
	return v;
};

handle[eRowType.DOUBLE] = function(v) {
	return v.readDoubleBE(0);
};

handle[eRowType.FLOAT] = function(v) {
	return v.readFloatBE(0);;
};

handle[eRowType.INET] = function(v) {
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

handle[eRowType.INT] = function(v) {
	return v.readInt32BE(0);
};

handle[eRowType.BLOB] = function(v) {
	return v;
};

handle[eRowType.TIMESTAMP] = function(v) {
	var msb = v.readInt32BE(0);
	var lsb = v.readUInt32BE(4);
	var miliseconds = msb * 4294967296 + lsb;
	return new Date(miliseconds);
};

handle[eRowType.TIMEUUID] = function(v) {
	return v.toString('hex', 0, 4) +
		'-' + v.toString('hex', 4, 6) +
		'-' + v.toString('hex', 6, 8) +
		'-' + v.toString('hex', 8, 10) +
		'-' + v.toString('hex', 10, 16);
};

handle[eRowType.UUID] = function(v) {
	return v.toString('hex', 0, 4) +
		'-' + v.toString('hex', 4, 6) +
		'-' + v.toString('hex', 6, 8) +
		'-' + v.toString('hex', 8, 10) +
		'-' + v.toString('hex', 10, 16);
};

handle[eRowType.VARCHAR] = function(v) {
	return v.toString('utf8');
};

handle[eRowType.VARINT] = function(v) {
	// TODO: varints are also stupid
	return v
};

handle[eRowType.SET] = function(v, f) {
	var result = [];
	var count = util.readShort(v);

	for ( var i = 0; i < count; i++) {
		var size = util.readShort(v);
		var value = v.slice(v._o, v._o + size);
		v._o += size;
		result.push( Message._deserializeValue( f, value, null, null ) );
	}

	return result;
};

handle[eRowType.MAP] = function(v, f, s) {
	var result = {};
	var count = util.readShort(v);

	for ( var i = 0; i < count; i++) {
		var size = util.readShort(v);
		var value = v.slice(v._o, v._o + size);
		v._o += size;
		var key = Message._deserializeValue( f, value, null, null );

		size = util.readShort(v);
		value = v.slice(v._o, v._o + size);
		v._o += size;

		result[key] = Message._deserializeValue( s, value, null, null );
	}

	return result;
};


Message._deserializeValue = function(rowTypeId, value, firstTypeId, secondTypeId) {

	if ( !handle[rowTypeId] ) {
		console.log('Unknown typeId ' + rowTypeId + ' ' + eRowTypeName[rowTypeId] + ' to serialze');
	} else {
		value = handle[rowTypeId](value, firstTypeId, secondTypeId);
	}

	return value;
};


Message.makeQuery = function(query, parameters) {

	// TODO Values, Skip_metadata, Page_size, With_paging_state, With_serial_consistency

	var consistency = parameters.consistency;
	var flags = 0x00;

	var bodySize = 4 + Buffer.byteLength(query, 'utf-8') + 2 + 1;
	var body = new Buffer(bodySize);
	body._o = 0;

	util.writeLongString(body, query);
	util.writeShort(body, consistency);
	util.writeByte(body, flags);

	return new Message(Message.eOpCode.QUERY, body);
}




module.exports = Message;
