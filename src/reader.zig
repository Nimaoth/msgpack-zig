const std = @import("std");
const time = @import("time");

pub const ValueTree = struct {
    arena: std.heap.ArenaAllocator,
    root: Value,

    pub fn deinit(self: *ValueTree) void {
        self.arena.deinit();
    }
};

pub const Map = std.StringArrayHashMap(Value);
pub const Array = std.ArrayList(Value);

pub const Ext = struct {
    type: i8,
    data: []const u8,
};

pub const Timestamp = struct {
    sec: i64,
    nsec: u32,
};

pub const Value = union(enum) {
    Nil,
    Bool: bool,
    Int: i64,
    UInt: u64,
    Float: f64,
    Binary: []const u8,
    String: []const u8,
    Array: Array,
    Map: Map,
    Ext: Ext,
    Timestamp: Timestamp,

    pub fn stringify(
        value: @This(),
        options: std.json.StringifyOptions,
        out_stream: anytype,
    ) @TypeOf(out_stream).Error!void {
        switch (value) {
            .Nil => try std.json.stringify(null, options, out_stream),
            .Bool => |inner| try std.json.stringify(inner, options, out_stream),
            .Int => |inner| try std.json.stringify(inner, options, out_stream),
            .UInt => |inner| try std.json.stringify(inner, options, out_stream),
            .Float => |inner| try std.json.stringify(inner, options, out_stream),
            .String => |inner| try std.json.stringify(inner, options, out_stream),

            .Timestamp => |inner| {
                try out_stream.writeAll("#timestamp \"");
                const t = time.unix(inner.sec, inner.nsec, &time.Location.utc_local);
                try std.fmt.format(out_stream, "{}", .{t});
                try out_stream.writeAll("\"");
            },

            .Binary => |inner| {
                try out_stream.writeByte('[');
                var field_output = false;
                var child_options = options;
                if (child_options.whitespace) |*child_whitespace| {
                    child_whitespace.indent_level += 1;
                }

                for (inner) |item| {
                    if (!field_output) {
                        field_output = true;
                    } else {
                        try out_stream.writeByte(',');
                    }

                    if (child_options.whitespace) |child_whitespace| {
                        try out_stream.writeByte('\n');
                        try child_whitespace.outputIndent(out_stream);
                    }

                    try std.fmt.format(out_stream, "0x{x}", .{item});
                    if (child_options.whitespace) |child_whitespace| {
                        if (child_whitespace.separator) {
                            try out_stream.writeByte(' ');
                        }
                    }
                }
                if (field_output) {
                    if (options.whitespace) |whitespace| {
                        try out_stream.writeByte('\n');
                        try whitespace.outputIndent(out_stream);
                    }
                }

                try out_stream.writeByte(']');
            },

            .Ext => |inner| {
                try out_stream.writeAll("#ext ");
                try std.json.stringify(inner.type, options, out_stream);
                try out_stream.writeByte(':');
                if (options.whitespace) |child_whitespace| {
                    try out_stream.writeByte(' ');
                }

                try out_stream.writeByte('[');
                var field_output = false;
                var child_options = options;
                if (child_options.whitespace) |*child_whitespace| {
                    child_whitespace.indent_level += 1;
                }

                for (inner.data) |item| {
                    if (!field_output) {
                        field_output = true;
                    } else {
                        try out_stream.writeByte(',');
                    }

                    if (child_options.whitespace) |child_whitespace| {
                        try out_stream.writeByte('\n');
                        try child_whitespace.outputIndent(out_stream);
                    }

                    try std.fmt.format(out_stream, "0x{x}", .{item});
                    if (child_options.whitespace) |child_whitespace| {
                        if (child_whitespace.separator) {
                            try out_stream.writeByte(' ');
                        }
                    }
                }
                if (field_output) {
                    if (options.whitespace) |whitespace| {
                        try out_stream.writeByte('\n');
                        try whitespace.outputIndent(out_stream);
                    }
                }

                try out_stream.writeByte(']');
            },

            .Array => |inner| {
                try out_stream.writeByte('[');
                var field_output = false;
                var child_options = options;
                if (child_options.whitespace) |*child_whitespace| {
                    child_whitespace.indent_level += 1;
                }

                for (inner.items) |item| {
                    if (!field_output) {
                        field_output = true;
                    } else {
                        try out_stream.writeByte(',');
                    }

                    if (child_options.whitespace) |child_whitespace| {
                        try out_stream.writeByte('\n');
                        try child_whitespace.outputIndent(out_stream);
                    }

                    try item.stringify(child_options, out_stream);
                    if (child_options.whitespace) |child_whitespace| {
                        if (child_whitespace.separator) {
                            try out_stream.writeByte(' ');
                        }
                    }
                }
                if (field_output) {
                    if (options.whitespace) |whitespace| {
                        try out_stream.writeByte('\n');
                        try whitespace.outputIndent(out_stream);
                    }
                }

                try out_stream.writeByte(']');
            },

            .Map => |inner| {
                try out_stream.writeByte('{');
                var field_output = false;
                var child_options = options;
                if (child_options.whitespace) |*child_whitespace| {
                    child_whitespace.indent_level += 1;
                }
                var it = inner.iterator();
                while (it.next()) |entry| {
                    if (!field_output) {
                        field_output = true;
                    } else {
                        try out_stream.writeByte(',');
                    }
                    if (child_options.whitespace) |child_whitespace| {
                        try out_stream.writeByte('\n');
                        try child_whitespace.outputIndent(out_stream);
                    }

                    try std.json.stringify(entry.key_ptr.*, options, out_stream);
                    try out_stream.writeByte(':');
                    if (child_options.whitespace) |child_whitespace| {
                        if (child_whitespace.separator) {
                            try out_stream.writeByte(' ');
                        }
                    }
                    try entry.value_ptr.stringify(child_options, out_stream);
                    //try std.json.stringify(entry.value_ptr.*, child_options, out_stream);
                }
                if (field_output) {
                    if (options.whitespace) |whitespace| {
                        try out_stream.writeByte('\n');
                        try whitespace.outputIndent(out_stream);
                    }
                }
                try out_stream.writeByte('}');
            },
        }
    }
};

pub fn MsgPackReader(comptime ReaderType: anytype) type {
    return struct {
        const Self = @This();

        reader: ReaderType,

        pub fn init(reader: ReaderType) Self {
            return Self{
                .reader = reader,
            };
        }

        fn readFromSliceEndianCorrected(self: *Self, comptime T: type, data: []const u8) !T {
            if (@sizeOf(T) > data.len) {
                return error.Eof;
            }
            var result: T = undefined;
            std.mem.copy(u8, std.mem.asBytes(&result), data[0..@sizeOf(T)]);
            if (std.Target.current.cpu.arch.endian() == .Little) {
                std.mem.reverse(u8, std.mem.asBytes(&result));
            }
            return result;
        }

        fn readAnyEndianCorrected(self: *Self, comptime T: type) !T {
            var result: T = undefined;
            const bytesRead = try self.reader.readAll(std.mem.asBytes(&result));
            if (bytesRead != @sizeOf(T)) {
                return error.Eof;
            }
            if (std.Target.current.cpu.arch.endian() == .Little) {
                std.mem.reverse(u8, std.mem.asBytes(&result));
            }
            return result;
        }

        fn readBytes(self: *Self, len: usize) !void {
            var i: usize = 0;
            var buffer: [64]u8 = undefined;
            while (i < len) {
                const bytesRead = try self.reader.readAll(buffer[0..std.math.min(buffer.len, len - i)]);
                if (bytesRead == 0) {
                    return error.Eof;
                }
                for (buffer[0..bytesRead]) |b| {
                    std.debug.print("{x:0>2} ", .{b});
                }
                i += bytesRead;
            }
        }

        fn readString(self: *Self, len: usize) !void {
            var i: usize = 0;
            var buffer: [64]u8 = undefined;
            while (i < len) {
                const bytesRead = try self.reader.readAll(buffer[0..std.math.min(buffer.len, len - i)]);
                if (bytesRead == 0) {
                    return error.Eof;
                }
                std.debug.print("{s}", .{buffer[0..bytesRead]});
                i += bytesRead;
            }
        }

        fn printIndent(self: *Self, indent: usize) void {
            var k: usize = 0;
            while (k < indent) : (k += 1) {
                std.debug.print("  ", .{});
            }
        }

        fn readInt(self: *Self, comptime T: type) !T {
            const typeInfo = @typeInfo(T);
            if (typeInfo != .Int) {
                @compileError("readInt expects an int type as parameter, got " ++ @typeName(T));
            }

            var result: T = 0;
            const tag = try self.reader.readByte();

            if (typeInfo.Int.signedness == .unsigned) {
                switch (tag) {
                    // u8 - u64
                    0xcc => return try self.readAnyEndianCorrected(u8),
                    0xcd => std.debug.print("u16: {}\n", .{try self.readAnyEndianCorrected(u16)}),
                    0xce => std.debug.print("u32: {}\n", .{try self.readAnyEndianCorrected(u32)}),
                    0xcf => std.debug.print("u64: {}\n", .{try self.readAnyEndianCorrected(u64)}),

                    else => {
                        if (tag & 0b1000_0000 == 0) {
                            // positive fixint
                            const value = tag;
                            std.debug.print("pos fixint: {}\n", .{value});
                        } else if (tag & 0b1110_0000 == 0b1110_0000) {
                            // negative fixint
                            var value = @bitCast(i8, tag & 0b1111_1111);
                            std.debug.print("neg fixint: {}\n", .{value});
                        }
                    },
                }
            } else {
                switch (tag) {
                    // u8 - u64
                    0xcc => return try self.readAnyEndianCorrected(u8),
                    0xcd => std.debug.print("u16: {}\n", .{try self.readAnyEndianCorrected(u16)}),
                    0xce => std.debug.print("u32: {}\n", .{try self.readAnyEndianCorrected(u32)}),
                    0xcf => std.debug.print("u64: {}\n", .{try self.readAnyEndianCorrected(u64)}),

                    // i8 - i64
                    0xd0 => std.debug.print("i8 : {}\n", .{try self.readAnyEndianCorrected(i8)}),
                    0xd1 => std.debug.print("i16: {}\n", .{try self.readAnyEndianCorrected(i16)}),
                    0xd2 => std.debug.print("i32: {}\n", .{try self.readAnyEndianCorrected(i32)}),
                    0xd3 => std.debug.print("i64: {}\n", .{try self.readAnyEndianCorrected(i64)}),

                    else => {
                        if (tag & 0b1000_0000 == 0) {
                            // positive fixint
                            const value = tag;
                            std.debug.print("pos fixint: {}\n", .{value});
                        }
                    },
                }
            }
        }

        pub fn readValue(self: *Self, allocator: *std.mem.Allocator) anyerror!ValueTree {
            var arena = std.heap.ArenaAllocator.init(allocator);
            errdefer arena.deinit();

            const value = try self.readValueInternal(&arena.allocator);

            return ValueTree{
                .arena = arena,
                .root = value,
            };
        }

        fn readValueInternal(self: *Self, allocator: *std.mem.Allocator) anyerror!Value {
            const tag = try self.reader.readByte();
            switch (tag) {
                0xc0 => return .Nil,

                // bool
                0xc2 => return Value{ .Bool = false },
                0xc3 => return Value{ .Bool = true },

                // u8 - u64
                0xcc => return Value{ .UInt = @intCast(u64, try self.readAnyEndianCorrected(u8)) },
                0xcd => return Value{ .UInt = @intCast(u64, try self.readAnyEndianCorrected(u16)) },
                0xce => return Value{ .UInt = @intCast(u64, try self.readAnyEndianCorrected(u32)) },
                0xcf => return Value{ .UInt = @intCast(u64, try self.readAnyEndianCorrected(u64)) },

                // i8 - i64
                0xd0 => return Value{ .Int = @intCast(i64, try self.readAnyEndianCorrected(i8)) },
                0xd1 => return Value{ .Int = @intCast(i64, try self.readAnyEndianCorrected(i16)) },
                0xd2 => return Value{ .Int = @intCast(i64, try self.readAnyEndianCorrected(i32)) },
                0xd3 => return Value{ .Int = @intCast(i64, try self.readAnyEndianCorrected(i64)) },

                // f32 - f64
                0xca => return Value{ .Float = @floatCast(f64, try self.readAnyEndianCorrected(f32)) },
                0xcb => return Value{ .Float = @floatCast(f64, try self.readAnyEndianCorrected(f64)) },

                // str 8 - str 32
                0xd9 => { // str 8
                    const len = try self.readAnyEndianCorrected(u8);
                    return try self.readValueString(allocator, len);
                },
                0xda => { // str 16
                    const len = try self.readAnyEndianCorrected(u16);
                    return try self.readValueString(allocator, len);
                },
                0xdb => { // str 32
                    const len = try self.readAnyEndianCorrected(u32);
                    return try self.readValueString(allocator, len);
                },

                // bin 8 - bin 32
                0xc4 => { // bin 8
                    const len = try self.readAnyEndianCorrected(u8);
                    return try self.readValueBytes(allocator, len);
                },
                0xc5 => { // bin 16
                    const len = try self.readAnyEndianCorrected(u16);
                    return try self.readValueBytes(allocator, len);
                },
                0xc6 => { // bin 32
                    const len = try self.readAnyEndianCorrected(u32);
                    return try self.readValueBytes(allocator, len);
                },

                // array 16 - array 32
                0xdc => { // array 16
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u16));
                    return try self.readValueArray(allocator, len);
                },
                0xdd => { // array 32
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u32));
                    return try self.readValueArray(allocator, len);
                },

                // map 16 - map 32
                0xde => { // map 16
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u16));
                    return try self.readValueObject(allocator, len);
                },
                0xdf => { // map 32
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u32));
                    return try self.readValueObject(allocator, len);
                },

                // ext
                0xd4 => { // fixext 1
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readValueExt(allocator, typ, 1);
                },
                0xd5 => { // fixext 2
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readValueExt(allocator, typ, 2);
                },
                0xd6 => { // fixext 4
                    const typ = @bitCast(i8, try self.reader.readByte());

                    if (typ == -1) {
                        // timestamp 32
                        var data: [4]u8 = undefined;
                        _ = try self.reader.readAll(data[0..]);
                        const sec = try self.readFromSliceEndianCorrected(u32, data[0..]);
                        return Value{ .Timestamp = .{ .sec = sec, .nsec = 0 } };
                    } else {
                        return try self.readValueExt(allocator, typ, 4);
                    }
                },
                0xd7 => { // fixext 8
                    const typ = @bitCast(i8, try self.reader.readByte());

                    if (typ == -1) {
                        // timestamp 64
                        var data: [8]u8 = undefined;
                        _ = try self.reader.readAll(data[0..]);
                        const data64 = try self.readFromSliceEndianCorrected(u64, data[0..]);
                        const nsec = @intCast(u32, data64 >> 34);
                        const sec = @bitCast(i64, data64 & 0x00000003ffffffff);
                        return Value{ .Timestamp = .{ .sec = sec, .nsec = nsec } };
                    } else {
                        return try self.readValueExt(allocator, typ, 8);
                    }
                },
                0xd8 => { // fixext 16
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readValueExt(allocator, typ, 16);
                },
                0xc7 => { // ext 8
                    const len = try self.readAnyEndianCorrected(u8);
                    const typ = @bitCast(i8, try self.reader.readByte());

                    if (typ == -1) {
                        // timestamp 96
                        if (len != 12) {
                            std.log.err("Timestamp has wrong length. Expected 12, got {}", .{len});
                            return error.InvalidTimestamp;
                        }

                        const nsec = try self.readAnyEndianCorrected(u32);
                        const sec = try self.readAnyEndianCorrected(i64);

                        return Value{ .Timestamp = .{ .sec = sec, .nsec = nsec } };
                    } else {
                        return try self.readValueExt(allocator, typ, len);
                    }
                },
                0xc8 => { // ext 16
                    const len = try self.readAnyEndianCorrected(u16);
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readValueExt(allocator, typ, len);
                },
                0xc9 => { // ext 32
                    const len = try self.readAnyEndianCorrected(u32);
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readValueExt(allocator, typ, len);
                },

                // other stuff
                else => {
                    if (tag & 0b1000_0000 == 0) {
                        // positive fixint
                        const value = tag;
                        return Value{ .Int = @intCast(i64, value) };
                    } else if (tag & 0b1110_0000 == 0b1110_0000) {
                        // negative fixint
                        var value = @bitCast(i8, tag & 0b1111_1111);
                        return Value{ .Int = @intCast(i64, value) };
                    } else if (tag & 0b1110_0000 == 0b1010_0000) {
                        // fixstr
                        const len = tag & 0b0001_1111;
                        return try self.readValueString(allocator, len);
                    } else if (tag & 0b1111_0000 == 0b1001_0000) {
                        // fixarray
                        const len = @intCast(usize, tag & 0b0000_1111);
                        return try self.readValueArray(allocator, len);
                    } else if (tag & 0b1111_0000 == 0b1000_0000) {
                        // fixmap
                        const len = @intCast(usize, tag & 0b0000_1111);
                        return try self.readValueObject(allocator, len);
                    } else {
                        std.debug.print("read other: {x:0>2}\n", .{tag});
                        return error.FailedToDecodeMsgPack;
                    }
                },
            }
        }

        pub fn readValueExt(self: *Self, allocator: *std.mem.Allocator, typ: i8, len: usize) anyerror!Value {
            const buffer = try allocator.alloc(u8, len);
            const bytesRead = try self.reader.readAll(buffer);
            if (bytesRead != buffer.len) {
                return error.Eof;
            }

            return Value{ .Ext = .{ .type = typ, .data = buffer } };
        }

        pub fn readValueString(self: *Self, allocator: *std.mem.Allocator, len: usize) anyerror!Value {
            const buffer = try allocator.alloc(u8, len);
            const bytesRead = try self.reader.readAll(buffer);
            if (bytesRead != buffer.len) {
                return error.Eof;
            }
            return Value{ .String = buffer };
        }

        pub fn readValueBytes(self: *Self, allocator: *std.mem.Allocator, len: usize) anyerror!Value {
            const buffer = try allocator.alloc(u8, len);
            const bytesRead = try self.reader.readAll(buffer);
            if (bytesRead != buffer.len) {
                return error.Eof;
            }
            return Value{ .Binary = buffer };
        }

        pub fn readValueArray(self: *Self, allocator: *std.mem.Allocator, len: usize) anyerror!Value {
            var array = try std.ArrayList(Value).initCapacity(allocator, len);

            var i: usize = 0;
            while (i < len) : (i += 1) {
                try array.append(try self.readValueInternal(allocator));
            }

            return Value{ .Array = array };
        }

        pub fn readValueObject(self: *Self, allocator: *std.mem.Allocator, len: usize) anyerror!Value {
            var object = std.StringArrayHashMap(Value).init(allocator);

            var i: usize = 0;
            while (i < len) : (i += 1) {
                const key = try self.readValueInternal(allocator);
                const value = try self.readValueInternal(allocator);

                if (key == .String) {
                    try object.put(key.String, value);
                } else {
                    return error.MapContainsNonStringKey;
                }
            }

            return Value{ .Map = object };
        }

        pub fn readJson(self: *Self, allocator: *std.mem.Allocator) anyerror!std.json.ValueTree {
            var arena = std.heap.ArenaAllocator.init(allocator);
            errdefer arena.deinit();

            const value = try self.readJsonInternal(&arena.allocator);

            return std.json.ValueTree{
                .arena = arena,
                .root = value,
            };
        }

        fn readJsonInternal(self: *Self, allocator: *std.mem.Allocator) anyerror!std.json.Value {
            const tag = try self.reader.readByte();
            switch (tag) {
                0xc0 => return .Null,

                // bool
                0xc2 => return std.json.Value{ .Bool = false },
                0xc3 => return std.json.Value{ .Bool = true },

                // u8 - u64
                0xcc => return std.json.Value{ .Integer = @intCast(i64, try self.readAnyEndianCorrected(u8)) },
                0xcd => return std.json.Value{ .Integer = @intCast(i64, try self.readAnyEndianCorrected(u16)) },
                0xce => return std.json.Value{ .Integer = @intCast(i64, try self.readAnyEndianCorrected(u32)) },
                0xcf => {
                    const value = try self.readAnyEndianCorrected(u64);
                    if (value <= @intCast(u64, std.math.maxInt(i64))) {
                        return std.json.Value{ .Integer = @intCast(i64, value) };
                    } else {
                        var buffer = std.ArrayList(u8).init(allocator);
                        try std.fmt.formatIntValue(value, "", .{}, buffer.writer());
                        return std.json.Value{ .NumberString = buffer.toOwnedSlice() };
                    }
                },

                // i8 - i64
                0xd0 => return std.json.Value{ .Integer = @intCast(i64, try self.readAnyEndianCorrected(i8)) },
                0xd1 => return std.json.Value{ .Integer = @intCast(i64, try self.readAnyEndianCorrected(i16)) },
                0xd2 => return std.json.Value{ .Integer = @intCast(i64, try self.readAnyEndianCorrected(i32)) },
                0xd3 => return std.json.Value{ .Integer = @intCast(i64, try self.readAnyEndianCorrected(i64)) },

                // f32 - f64
                0xca => return std.json.Value{ .Float = @floatCast(f64, try self.readAnyEndianCorrected(f32)) },
                0xcb => return std.json.Value{ .Float = @floatCast(f64, try self.readAnyEndianCorrected(f64)) },

                // str 8 - str 32
                0xd9 => { // str 8
                    const len = try self.readAnyEndianCorrected(u8);
                    return try self.readJsonString(allocator, len);
                },
                0xda => { // str 16
                    const len = try self.readAnyEndianCorrected(u16);
                    return try self.readJsonString(allocator, len);
                },
                0xdb => { // str 32
                    const len = try self.readAnyEndianCorrected(u32);
                    return try self.readJsonString(allocator, len);
                },

                // bin 8 - bin 32
                0xc4 => { // bin 8
                    const len = try self.readAnyEndianCorrected(u8);
                    return try self.readJsonBinary(allocator, len);
                },
                0xc5 => { // bin 16
                    const len = try self.readAnyEndianCorrected(u16);
                    return try self.readJsonBinary(allocator, len);
                },
                0xc6 => { // bin 32
                    const len = try self.readAnyEndianCorrected(u32);
                    return try self.readJsonBinary(allocator, len);
                },

                // array 16 - array 32
                0xdc => { // array 16
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u16));
                    return try self.readJsonArray(allocator, len);
                },
                0xdd => { // array 32
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u32));
                    return try self.readJsonArray(allocator, len);
                },

                // map 16 - map 32
                0xde => { // map 16
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u16));
                    return try self.readJsonObject(allocator, len);
                },
                0xdf => { // map 32
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u32));
                    return try self.readJsonObject(allocator, len);
                },

                // ext
                0xd4 => { // fixext 1
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readJsonExt(allocator, typ, 1);
                },
                0xd5 => { // fixext 2
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readJsonExt(allocator, typ, 2);
                },
                0xd6 => { // fixext 4
                    const typ = @bitCast(i8, try self.reader.readByte());

                    if (typ == -1) {
                        // timestamp 32
                        var data: [4]u8 = undefined;
                        _ = try self.reader.readAll(data[0..]);
                        const sec = try self.readFromSliceEndianCorrected(u32, data[0..]);

                        var ext = try std.ArrayList(std.json.Value).initCapacity(allocator, 2);
                        try ext.append(std.json.Value{ .Integer = sec });
                        try ext.append(std.json.Value{ .Integer = 0 });
                        return std.json.Value{ .Array = ext };
                    } else {
                        return try self.readJsonExt(allocator, typ, 4);
                    }
                },
                0xd7 => { // fixext 8
                    const typ = @bitCast(i8, try self.reader.readByte());

                    if (typ == -1) {
                        // timestamp 64
                        var data: [8]u8 = undefined;
                        _ = try self.reader.readAll(data[0..]);
                        const data64 = try self.readFromSliceEndianCorrected(u64, data[0..]);
                        const nsec = @intCast(u32, data64 >> 34);
                        const sec = @bitCast(i64, data64 & 0x00000003ffffffff);

                        var ext = try std.ArrayList(std.json.Value).initCapacity(allocator, 2);
                        try ext.append(std.json.Value{ .Integer = sec });
                        try ext.append(std.json.Value{ .Integer = nsec });
                        return std.json.Value{ .Array = ext };
                    } else {
                        return try self.readJsonExt(allocator, typ, 8);
                    }
                },
                0xd8 => { // fixext 16
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readJsonExt(allocator, typ, 16);
                },
                0xc7 => { // ext 8
                    const len = try self.readAnyEndianCorrected(u8);
                    const typ = @bitCast(i8, try self.reader.readByte());

                    if (typ == -1) {
                        // timestamp 96
                        if (len != 12) {
                            std.log.err("Timestamp has wrong length. Expected 12, got {}", .{len});
                            return error.InvalidTimestamp;
                        }

                        const nsec = try self.readAnyEndianCorrected(u32);
                        const sec = try self.readAnyEndianCorrected(i64);

                        var ext = try std.ArrayList(std.json.Value).initCapacity(allocator, 2);
                        try ext.append(std.json.Value{ .Integer = sec });
                        try ext.append(std.json.Value{ .Integer = nsec });
                        return std.json.Value{ .Array = ext };
                    } else {
                        return try self.readJsonExt(allocator, typ, len);
                    }
                },
                0xc8 => { // ext 16
                    const len = try self.readAnyEndianCorrected(u16);
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readJsonExt(allocator, typ, len);
                },
                0xc9 => { // ext 32
                    const len = try self.readAnyEndianCorrected(u32);
                    const typ = @bitCast(i8, try self.reader.readByte());
                    return try self.readJsonExt(allocator, typ, len);
                },

                // other stuff
                else => {
                    if (tag & 0b1000_0000 == 0) {
                        // positive fixint
                        const value = tag;
                        return std.json.Value{ .Integer = @intCast(i64, value) };
                    } else if (tag & 0b1110_0000 == 0b1110_0000) {
                        // negative fixint
                        var value = @bitCast(i8, tag & 0b1111_1111);
                        return std.json.Value{ .Integer = @intCast(i64, value) };
                    } else if (tag & 0b1110_0000 == 0b1010_0000) {
                        // fixstr
                        const len = tag & 0b0001_1111;
                        return try self.readJsonString(allocator, len);
                    } else if (tag & 0b1111_0000 == 0b1001_0000) {
                        // fixarray
                        const len = @intCast(usize, tag & 0b0000_1111);
                        return try self.readJsonArray(allocator, len);
                    } else if (tag & 0b1111_0000 == 0b1000_0000) {
                        // fixmap
                        const len = @intCast(usize, tag & 0b0000_1111);
                        return try self.readJsonObject(allocator, len);
                    } else {
                        std.debug.print("read other: {x:0>2}\n", .{tag});
                        return error.FailedToDecodeMsgPack;
                    }
                },
            }
        }

        pub fn readJsonExt(self: *Self, allocator: *std.mem.Allocator, typ: i8, len: usize) anyerror!std.json.Value {
            const strLen = @intCast(usize, len) * 2 + if (len > 1) @intCast(usize, len - 1) else 0;
            const buffer = try allocator.alloc(u8, strLen);
            std.mem.set(u8, buffer, 0);
            var stream = std.io.fixedBufferStream(buffer);
            var writer = stream.writer();

            var i: usize = 0;
            while (i < len) : (i += 1) {
                if (i > 0) try writer.writeByte('-');
                const byte = try self.reader.readByte();
                try std.fmt.formatIntValue(byte, "x", .{ .width = 2, .fill = '0' }, writer);
            }

            var ext = try std.ArrayList(std.json.Value).initCapacity(allocator, 2);
            try ext.append(std.json.Value{ .Integer = typ });
            try ext.append(std.json.Value{ .String = buffer });

            return std.json.Value{ .Array = ext };
        }

        pub fn readJsonString(self: *Self, allocator: *std.mem.Allocator, len: usize) anyerror!std.json.Value {
            const buffer = try allocator.alloc(u8, len);
            const bytesRead = try self.reader.readAll(buffer);
            if (bytesRead != buffer.len) {
                return error.Eof;
            }
            return std.json.Value{ .String = buffer };
        }

        pub fn readJsonBinary(self: *Self, allocator: *std.mem.Allocator, len: usize) anyerror!std.json.Value {
            const strLen = @intCast(usize, len) * 2 + if (len > 1) @intCast(usize, len - 1) else 0;
            const buffer = try allocator.alloc(u8, strLen);
            var stream = std.io.fixedBufferStream(buffer);
            var writer = stream.writer();

            var i: usize = 0;
            while (i < len) : (i += 1) {
                if (i > 0) try writer.writeByte('-');
                const byte = try self.reader.readByte();
                try std.fmt.formatIntValue(byte, "x", .{ .width = 2, .fill = '0' }, writer);
            }

            return std.json.Value{ .String = buffer };
        }

        pub fn readJsonArray(self: *Self, allocator: *std.mem.Allocator, len: usize) anyerror!std.json.Value {
            var array = try std.ArrayList(std.json.Value).initCapacity(allocator, len);

            var i: usize = 0;
            while (i < len) : (i += 1) {
                try array.append(try self.readJsonInternal(allocator));
            }

            return std.json.Value{ .Array = array };
        }

        pub fn readJsonObject(self: *Self, allocator: *std.mem.Allocator, len: usize) anyerror!std.json.Value {
            var object = std.StringArrayHashMap(std.json.Value).init(allocator);

            var i: usize = 0;
            while (i < len) : (i += 1) {
                const key = try self.readJsonInternal(allocator);
                const value = try self.readJsonInternal(allocator);

                if (key == .String) {
                    try object.put(key.String, value);
                } else {
                    return error.MapContainsNonStringKey;
                }
            }

            return std.json.Value{ .Object = object };
        }
    };
}

pub fn msgPackReader(reader: anytype) MsgPackReader(@TypeOf(reader)) {
    return .{ .reader = reader };
}
