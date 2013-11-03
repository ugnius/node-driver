
util = {};

util.writeByte = function(buf, byte) {
	buf.writeUInt8(byte, buf._o);
	buf._o += 1;
};

util.writeShort = function(buf, short) {
	buf.writeUInt16BE(short, buf._o);
	buf._o += 2;
};

util.writeInt = function(buf, int) {
	buf.writeInt32BE(int, buf._o);
	buf._o += 4;
};

util.writeString = function(buf, string) {
	var l = Buffer.byteLength(string);
	util.writeShort(buf, l);
	buf.write(string, buf._o);
	buf._o += l;
};

util.writeLongString = function(buf, string) {
	var l = Buffer.byteLength(string);
	util.writeInt(buf, l);
	buf.write(string, buf._o);
	buf._o += l;
};

util.buildStringMap = function(map) {
	var length = 2;
	var n = 0;;

	for (var k in map) {
		var v = map[k];
		length += 2 + Buffer.byteLength(k) + 2 + Buffer.byteLength(v);
		n++;
	}

	var buf = new Buffer(length);
	buf._o = 0;

	util.writeShort(buf, n);
	for (var k in map) {
		var v = map[k];
		util.writeString(buf, k);
		util.writeString(buf, v);
	}

	return buf;
};

util.readShort = function(buf) {
	var s = buf.readUInt16BE(buf._o);
	buf._o += 2;
	return s;
};

util.readInt = function(buf) {
	var i = buf.readInt32BE(buf._o);
	buf._o += 4;
	return i;
};

util.readString = function(buf) {
	var l = util.readShort(buf);
	var s = buf.toString('utf-8', buf._o, buf._o + l);
	buf._o += l;
	return s;
};

util.readStringList = function(buf) {
	var l = [];
	var n = util.readShort(buf);
	var i;
	for (i = n; i > 0; i--) {
		l.push(util.readString(buf));
	}
	return l;
};

util.readMultimap = function(buf) {
	var map = {};
	var n = util.readShort(buf);
	var i, k;

	for (i = n; i > 0; i--) {
		k = util.readString(buf);
		map[k] = util.readStringList(buf);
	}
	return map;
};

module.exports = util;