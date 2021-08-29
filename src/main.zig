const std = @import("std");
const time = @import("time");
const yaml = @import("yaml");

const String = struct {
    value: []const u8,

    pub fn msgPackWrite(self: @This(), writer: anytype) !void {
        try writer.writeString(self.value);
    }

    pub fn msgPackRead(self: @This(), writer: anytype) !void {
        try writer.writeString(self.value);
    }
};

pub fn msgPackWrite(arr: anytype, writer: anytype) !void {}

fn MsgPackWriter(comptime WriterType: anytype, comptime Fast: bool) type {
    return struct {
        const Self = @This();

        const Options = struct {
            writeBytesSliceAsString: bool = true,
            writeEnumAsString: bool = false,
        };

        writer: WriterType,
        options: Options,

        pub fn init(writer: WriterType, options: Options) Self {
            return Self{
                .writer = writer,
                .options = options,
            };
        }

        fn writeAnyEndianCorrected(self: *Self, value: anytype) !void {
            try self.writeBytesEndianCorrected(std.mem.asBytes(&value));
        }

        fn writeBytesEndianCorrected(self: *Self, bytes: []const u8) !void {
            switch (std.Target.current.cpu.arch.endian()) {
                .Big => try self.writer.writeAll(bytes),
                .Little => {
                    var i: isize = @intCast(isize, bytes.len) - 1;
                    while (i >= 0) : (i -= 1) {
                        _ = try self.writer.write(&.{bytes[@intCast(usize, i)]});
                    }
                },
            }
        }

        pub fn writeString(self: *Self, string: []const u8) !void {
            std.log.info("writeString: {}", .{string.len});
            // length
            if (string.len <= 31) {
                std.debug.assert(@intCast(u8, string.len) & 0b00011111 == @intCast(u8, string.len));
                _ = try self.writer.writeAll(&.{@intCast(u8, string.len) | 0b10100000});
            } else if (string.len <= std.math.pow(usize, 2, 8) - 1) {
                _ = try self.writer.writeAll(&.{ 0xd9, @intCast(u8, string.len) });
            } else if (string.len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xda});
                _ = try self.writeAnyEndianCorrected(@intCast(u16, string.len));
            } else if (string.len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xdb});
                _ = try self.writeAnyEndianCorrected(@intCast(u32, string.len));
            } else {
                return error.StringTooLong;
            }

            // content
            _ = try self.writer.writeAll(string);
        }

        pub fn writeBytes(self: *Self, bytes: []const u8) !void {
            std.log.info("writeBytes: {}", .{bytes.len});
            // length
            if (bytes.len <= std.math.pow(usize, 2, 8) - 1) {
                _ = try self.writer.writeAll(&.{ 0xc4, @intCast(u8, bytes.len) });
            } else if (bytes.len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xc5});
                _ = try self.writeAnyEndianCorrected(@intCast(u16, bytes.len));
            } else if (bytes.len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xc6});
                _ = try self.writeAnyEndianCorrected(@intCast(u32, bytes.len));
            } else {
                return error.BytesTooLong;
            }

            // content
            _ = try self.writer.writeAll(bytes);
        }

        pub fn writeBool(self: *Self, value: bool) !void {
            _ = try self.writer.writeAll(&.{0xc2 + @intCast(u8, @boolToInt(value))});
        }

        pub fn writeInt(self: *Self, value: anytype) !void {
            std.log.info("writeInt: {}", .{value});
            const ValueType = @TypeOf(value);
            const typeInfo = @typeInfo(ValueType);

            // Special cases
            if (value >= 0 and value <= 127) {
                // positive fixint 0XXX XXXX
                _ = try self.writer.writeAll(&.{@intCast(u8, value) & 0b0111_1111});
                return;
            } else if (value >= -32 and value <= -1) {
                // negative fixint 111X XXXX
                _ = try self.writer.writeAll(&.{(@bitCast(u8, @intCast(i8, value)) & 0b0001_1111) | 0b1110_0000});
                return;
            }

            const bits = comptime std.mem.alignForward(@as(usize, typeInfo.Int.bits), 8);
            const tag = if (typeInfo.Int.signedness == .signed) switch (bits) {
                8 => 0xd0,
                16 => 0xd1,
                32 => 0xd2,
                64 => 0xd3,
                else => unreachable,
            } else switch (bits) {
                8 => 0xcc,
                16 => 0xcd,
                32 => 0xce,
                64 => 0xcf,
                else => unreachable,
            };

            _ = try self.writer.writeAll(&.{tag});
            _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&value));
        }

        pub fn writeFloat(self: *Self, value: anytype) !void {
            const ValueType = @TypeOf(value);
            const typeInfo = @typeInfo(ValueType);

            const bits = comptime std.mem.alignForward(@as(usize, typeInfo.Float.bits), 8);
            const tag = switch (bits) {
                32 => 0xca,
                64 => 0xcb,
                else => unreachable,
            };

            _ = try self.writer.writeAll(&.{tag});
            _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&value));
        }

        pub fn writeExt(self: *Self, typ: i8, data: []const u8) !void {
            if (data.len == 1) {
                _ = try self.writer.writeAll(&.{ 0xd4, @bitCast(u8, typ), data[0] });
            } else if (data.len == 2) {
                _ = try self.writer.writeAll(&.{ 0xd5, @bitCast(u8, typ), data[0], data[1] });
            } else if (data.len == 4) {
                _ = try self.writer.writeAll(&.{ 0xd6, @bitCast(u8, typ) });
                _ = try self.writer.writeAll(data);
            } else if (data.len == 8) {
                _ = try self.writer.writeAll(&.{ 0xd7, @bitCast(u8, typ) });
                _ = try self.writer.writeAll(data);
            } else if (data.len == 16) {
                _ = try self.writer.writeAll(&.{ 0xd8, @bitCast(u8, typ) });
                _ = try self.writer.writeAll(data);
            } else if (data.len <= std.math.pow(usize, 2, 8) - 1) {
                _ = try self.writer.writeAll(&.{ 0xc7, @intCast(u8, data.len), @bitCast(u8, typ) });
                _ = try self.writer.writeAll(data);
            } else if (data.len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xc8});
                _ = try self.writeAnyEndianCorrected(@intCast(u16, data.len));
                _ = try self.writer.writeAll(&.{@bitCast(u8, typ)});
                _ = try self.writer.writeAll(data);
            } else if (data.len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xc9});
                _ = try self.writeAnyEndianCorrected(@intCast(u32, data.len));
                _ = try self.writer.writeAll(&.{@bitCast(u8, typ)});
                _ = try self.writer.writeAll(data);
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn writeTimestamp(self: *Self, unixSeconds: i64, nanoseconds: u32) !void {
            if ((unixSeconds >> 34) == 0) {
                const data64 = @bitCast(u64, (@intCast(i64, nanoseconds) << 34) | unixSeconds);
                if ((data64 & 0xffffffff00000000) == 0) {
                    // timestamp 32
                    _ = try self.writer.writeAll(&.{ 0xd6, 0xff });
                    try self.writeAnyEndianCorrected(@intCast(u32, data64));
                } else {
                    // timestamp 64
                    _ = try self.writer.writeAll(&.{ 0xd7, 0xff });
                    try self.writeAnyEndianCorrected(data64);
                }
            } else {
                // timestamp 96
                _ = try self.writer.writeAll(&.{ 0xc7, 12, 0xff });
                try self.writeAnyEndianCorrected(nanoseconds);
                try self.writeAnyEndianCorrected(unixSeconds);
            }
        }

        pub fn beginArray(self: *Self, len: usize) !void {
            if (len <= 15) {
                _ = try self.writer.writeAll(&.{@intCast(u8, len) | 0b1001_0000});
            } else if (len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xdc});
                const len16 = @intCast(u16, len);
                _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&len16));
            } else if (len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xdd});
                const len32 = @intCast(u16, len);
                _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&len32));
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn beginMap(self: *Self, len: usize) !void {
            if (len <= 15) {
                _ = try self.writer.writeAll(&.{@intCast(u8, len) | 0b1000_0000});
            } else if (len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xde});
                const len16 = @intCast(u16, len);
                _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&len16));
            } else if (len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xdf});
                const len32 = @intCast(u16, len);
                _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&len32));
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn writeAny(self: *Self, value: anytype) !void {
            try self.writeAnyPtr(&value);
        }

        pub fn writeAnyPtr(self: *Self, value: anytype) !void {
            const ptrInfo = @typeInfo(@TypeOf(value));
            if (ptrInfo != .Pointer) {
                @compileError("Parameter 'value' has to be a pointer but is " ++ @typeName(@TypeOf(value)));
            }
            const ValueType = ptrInfo.Pointer.child;
            const typeInfo = @typeInfo(ValueType);

            // special case: string ([]const u8)
            if (ValueType == []const u8 or ValueType == []u8) {
                if (self.options.writeBytesSliceAsString) {
                    try self.writeString(value.*);
                } else {
                    try self.writeBytes(value.*);
                }
                return;
            }
            switch (typeInfo) {
                .Int => try self.writeInt(value.*),
                .Float => try self.writeFloat(value.*),
                .Bool => try self.writeBool(value.*),

                .Enum => {
                    if (self.options.writeEnumAsString) {
                        try self.writeString(@tagName(value.*));
                    } else {
                        try self.writeInt(@enumToInt(value.*));
                    }
                },

                .Struct => {
                    if (comptime std.meta.trait.hasFn("msgPackWrite")(ValueType)) {
                        return try value.msgPackWrite(self);
                    }

                    try self.beginMap(typeInfo.Struct.fields.len);
                    inline for (typeInfo.Struct.fields) |field| {
                        try self.writeString(field.name);
                        try self.writeAny(@field(value, field.name));
                    }
                },

                .Pointer => {
                    switch (typeInfo.Pointer.size) {
                        .Slice => {
                            try self.beginArray(value.*.len);
                            for (value.*) |*v| {
                                try self.writeAnyPtr(v);
                            }
                        },

                        else => {
                            std.log.err("Failed to write value of type {s} ({})", .{ @typeName(ValueType), typeInfo });
                            return error.MsgPackWriteError;
                        },
                    }
                },

                else => {
                    std.log.err("Failed to write value of type {s} ({})", .{ @typeName(ValueType), typeInfo });
                    return error.MsgPackWriteError;
                },
            }
        }
    };
}

fn msgPackWriter(writer: anytype, options: MsgPackWriter(@TypeOf(writer), false).Options) MsgPackWriter(@TypeOf(writer), false) {
    return MsgPackWriter(@TypeOf(writer), false).init(writer, options);
}

fn MsgPackReader(comptime ReaderType: anytype) type {
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

        fn read(self: *Self, indent: usize) anyerror!void {
            self.printIndent(indent);

            const tag = try self.reader.readByte();
            switch (tag) {
                // bool
                0xc2 => std.debug.print("bool: false\n", .{}),
                0xc3 => std.debug.print("bool: true\n", .{}),

                // u8 - u64
                0xcc => std.debug.print("u8 : {}\n", .{try self.readAnyEndianCorrected(u8)}),
                0xcd => std.debug.print("u16: {}\n", .{try self.readAnyEndianCorrected(u16)}),
                0xce => std.debug.print("u32: {}\n", .{try self.readAnyEndianCorrected(u32)}),
                0xcf => std.debug.print("u64: {}\n", .{try self.readAnyEndianCorrected(u64)}),

                // i8 - i64
                0xd0 => std.debug.print("i8 : {}\n", .{try self.readAnyEndianCorrected(i8)}),
                0xd1 => std.debug.print("i16: {}\n", .{try self.readAnyEndianCorrected(i16)}),
                0xd2 => std.debug.print("i32: {}\n", .{try self.readAnyEndianCorrected(i32)}),
                0xd3 => std.debug.print("i64: {}\n", .{try self.readAnyEndianCorrected(i64)}),

                // f32 - f64
                0xca => std.debug.print("f32: {}\n", .{try self.readAnyEndianCorrected(f32)}),
                0xcb => std.debug.print("f64: {}\n", .{try self.readAnyEndianCorrected(f64)}),

                // str 8 - str 32
                0xd9 => {
                    // str 8
                    const len = try self.readAnyEndianCorrected(u8);
                    std.debug.print("str  8 ({}): ", .{len});
                    try self.readString(len);
                    std.debug.print("\n", .{});
                },
                0xda => {
                    // str 16
                    const len = try self.readAnyEndianCorrected(u16);
                    std.debug.print("str 16 ({}): ", .{len});
                    try self.readString(len);
                    std.debug.print("\n", .{});
                },
                0xdb => {
                    // str 32
                    const len = try self.readAnyEndianCorrected(u32);
                    std.debug.print("str 32 ({}): ", .{len});
                    try self.readString(len);
                    std.debug.print("\n", .{});
                },

                // bin 8 - bin 32
                0xc4 => {
                    // bin 8
                    const len = try self.readAnyEndianCorrected(u8);
                    std.debug.print("bin  8 ({}): ", .{len});
                    try self.readBytes(len);
                    std.debug.print("\n", .{});
                },
                0xc5 => {
                    // bin 16
                    const len = try self.readAnyEndianCorrected(u16);
                    std.debug.print("bin 16 ({}): ", .{len});
                    try self.readBytes(len);
                    std.debug.print("\n", .{});
                },
                0xc6 => {
                    // bin 32
                    const len = try self.readAnyEndianCorrected(u32);
                    std.debug.print("bin 32 ({}): ", .{len});
                    try self.readBytes(len);
                    std.debug.print("\n", .{});
                },

                // array 16 - array 32
                0xdc => {
                    // array 16
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u16));
                    std.debug.print("array 16 ({}): ", .{len});
                    try self.readBytes(len);
                    std.debug.print("\n", .{});

                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        try self.read(indent + 1);
                    }
                },
                0xdd => {
                    // array 32
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u32));
                    std.debug.print("array 32 ({}): ", .{len});
                    try self.readBytes(len);
                    std.debug.print("\n", .{});

                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        try self.read(indent + 1);
                    }
                },

                // map 16 - map 32
                0xde => {
                    // map 16
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u16));
                    std.debug.print("map 16 ({}): ", .{len});
                    try self.readBytes(len);
                    std.debug.print("\n", .{});

                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        self.printIndent(indent + 1);
                        std.debug.print("Key:\n", .{});
                        try self.read(indent + 2);

                        self.printIndent(indent + 1);
                        std.debug.print("Value:\n", .{});
                        try self.read(indent + 2);
                    }
                },
                0xdf => {
                    // map 32
                    const len = @intCast(usize, try self.readAnyEndianCorrected(u32));
                    std.debug.print("map 32 ({}): ", .{len});
                    try self.readBytes(len);
                    std.debug.print("\n", .{});

                    var i: usize = 0;
                    while (i < len) : (i += 1) {
                        self.printIndent(indent + 1);
                        std.debug.print("Key:\n", .{});
                        try self.read(indent + 2);

                        self.printIndent(indent + 1);
                        std.debug.print("Value:\n", .{});
                        try self.read(indent + 2);
                    }
                },

                // ext
                0xd4 => {
                    // fixext 1
                    const typ = @bitCast(i8, try self.reader.readByte());
                    std.debug.print("fixext 1 ({}): {x:0>2}\n", .{ typ, try self.reader.readByte() });
                },
                0xd5 => {
                    // fixext 2
                    const typ = @bitCast(i8, try self.reader.readByte());
                    std.debug.print("fixext 2 ({}): {x:0>2} {x:0>2}\n", .{ typ, try self.reader.readByte(), try self.reader.readByte() });
                },
                0xd6 => {
                    // fixext 4
                    const typ = @bitCast(i8, try self.reader.readByte());
                    var data: [4]u8 = undefined;
                    _ = try self.reader.readAll(data[0..]);

                    if (typ == -1) {
                        // timestamp 32
                        const data32 = try self.readFromSliceEndianCorrected(u32, data[0..]);
                        const t = time.unix(@intCast(i64, data32), 0, &time.Location.utc_local);
                        std.debug.print("timestamp 32: {}, {}\n", .{ data32, t });
                    } else {
                        std.debug.print("fixext 4 ({}): {}\n", .{ typ, std.fmt.fmtSliceHexUpper(data[0..]) });
                    }
                },
                0xd7 => {
                    // fixext 8
                    const typ = @bitCast(i8, try self.reader.readByte());
                    var data: [8]u8 = undefined;
                    _ = try self.reader.readAll(data[0..]);

                    if (typ == -1) {
                        // timestamp 64
                        const data64 = try self.readFromSliceEndianCorrected(u64, data[0..]);
                        const nsec = @intCast(u32, data64 >> 34);
                        const sec = @bitCast(i64, data64 & 0x00000003ffffffff);
                        const t = time.unix(sec, @intCast(i32, nsec), &time.Location.utc_local);
                        std.debug.print("timestamp 64: {}, {}, {}\n", .{ sec, nsec, t });
                    } else {
                        std.debug.print("fixext 8 ({}): {}\n", .{ typ, std.fmt.fmtSliceHexUpper(data[0..]) });
                    }
                },
                0xd8 => {
                    // fixext 16
                    const typ = @bitCast(i8, try self.reader.readByte());
                    var data: [16]u8 = undefined;
                    _ = try self.reader.readAll(data[0..]);
                    std.debug.print("fixext 16 ({}): {}\n", .{ typ, std.fmt.fmtSliceHexUpper(data[0..]) });
                },
                0xc7 => {
                    // ext 8
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
                        const t = time.unix(sec, @intCast(i32, nsec), &time.Location.utc_local);
                        std.debug.print("timestamp 96: {}, {}, {}\n", .{ sec, nsec, t });
                    } else {
                        std.debug.print("ext 8 ({}, {}): ", .{ typ, len });
                        try self.readBytes(len);
                        std.debug.print("\n", .{});
                    }
                },
                0xc8 => {
                    // ext 16
                    const len = try self.readAnyEndianCorrected(u16);
                    const typ = @bitCast(i8, try self.reader.readByte());
                    std.debug.print("ext 16 ({}, {}): ", .{ typ, len });
                    try self.readBytes(len);
                    std.debug.print("\n", .{});
                },
                0xc9 => {
                    // ext 32
                    const len = try self.readAnyEndianCorrected(u32);
                    const typ = @bitCast(i8, try self.reader.readByte());
                    std.debug.print("ext 32 ({}, {}): ", .{ typ, len });
                    try self.readBytes(len);
                    std.debug.print("\n", .{});
                },

                // other stuff
                else => {
                    if (tag & 0b1000_0000 == 0) {
                        // positive fixint
                        const value = tag;
                        std.debug.print("pos fixint: {}\n", .{value});
                    } else if (tag & 0b1110_0000 == 0b1110_0000) {
                        // negative fixint
                        var value = @bitCast(i8, tag & 0b1111_1111);
                        std.debug.print("neg fixint: {}\n", .{value});
                    } else if (tag & 0b1110_0000 == 0b1010_0000) {
                        // fixstr
                        const len = tag & 0b0001_1111;
                        std.debug.print("fixstr ({}): ", .{len});
                        try self.readString(len);
                        std.debug.print("\n", .{});
                    } else if (tag & 0b1111_0000 == 0b1001_0000) {
                        // fixarray
                        const len = @intCast(usize, tag & 0b0000_1111);
                        std.debug.print("fixarray ({}):\n", .{len});

                        var i: usize = 0;
                        while (i < len) : (i += 1) {
                            try self.read(indent + 1);
                        }
                    } else if (tag & 0b1111_0000 == 0b1000_0000) {
                        // fixmap
                        const len = @intCast(usize, tag & 0b0000_1111);
                        std.debug.print("fixmap ({}):\n", .{len});

                        var i: usize = 0;
                        while (i < len) : (i += 1) {
                            self.printIndent(indent + 1);
                            std.debug.print("Key:\n", .{});
                            try self.read(indent + 2);

                            self.printIndent(indent + 1);
                            std.debug.print("Value:\n", .{});
                            try self.read(indent + 2);
                        }
                    } else {
                        std.debug.print("read other: {x:0>2}\n", .{tag});
                    }
                },
            }
        }
    };
}

fn msgPackReader(reader: anytype) MsgPackReader(@TypeOf(reader)) {
    return .{ .reader = reader };
}

const Foo = struct {
    a: i64,
    b: []const u8,
    c: []const bool,
    d: Bar,
    e: String,
};

const Baz = enum {
    Eins,
    Zwei,
    Drei,
};

const Bar = struct {
    lol: f32,
    uiae: bool,
    baz: Baz,
};

pub fn main() anyerror!void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var local = time.Location.getLocal();
    var now = time.now(&local);
    std.log.info("now : {}", .{now});
    std.log.info("date: {}", .{now.date()});
    std.log.info("sec : {}", .{now.unix()});
    std.log.info("nsec: {}", .{now.nanosecond()});

    var buffer = try gpa.allocator.alloc(u8, 1024 * 1024);
    defer gpa.allocator.free(buffer);
    std.mem.set(u8, buffer, 0);

    var stream = std.io.fixedBufferStream(buffer);

    var msgPack = msgPackWriter(stream.writer(), .{
        .writeBytesSliceAsString = true,
        .writeEnumAsString = false,
    });

    var foo = Foo{
        .a = 123,
        .b = "Hello",
        .c = &.{ true, false, false, true, true, false, true, true, false, false },
        .d = Bar{
            .lol = 123.456,
            .uiae = true,
            .baz = .Drei,
        },
        .e = String{ .value = "this is a String" },
    };

    try msgPack.writeTimestamp(now.unix(), @intCast(u32, now.nanosecond()));
    try msgPack.writeTimestamp(now.unix(), 0);

    const yaml_test =
        \\- nil: null
        \\  msgpack:
        \\    - "c0"
    ;
    //var uiae = try yaml.Yaml.load(&gpa.allocator, yaml_test);
    //defer uiae.deinit();

    try msgPack.writeAny(foo);
    try msgPack.writeExt(72, std.mem.asBytes(&foo));
    try msgPack.writeExt(1, &.{0x12});
    try msgPack.writeExt(2, &.{ 0x34, 0x56 });
    try msgPack.writeExt(-1, &.{ 0x78, 0x9a, 0xbc, 0xde });
    try msgPack.beginArray(5);

    try msgPack.beginMap(3);
    try msgPack.writeAny(@as(i64, 0));
    try msgPack.writeAny(@as(i64, 5));
    try msgPack.writeAny(@as(i64, 127));
    try msgPack.writeAny(@as(i64, -1));
    try msgPack.writeAny(@as(i64, -11));
    try msgPack.writeAny(@as(i64, -32));

    try msgPack.writeAny(@as(u8, 123));
    try msgPack.writeAny(@as(i8, -123));

    try msgPack.writeAny(@as(u16, 456));
    try msgPack.writeAny(@as(i16, -789));

    try msgPack.writeAny(@as(f32, 1.2345678987654321));
    try msgPack.writeAny(@as(f64, 1.2345678987654321));

    try msgPack.writeString("hello world");
    try msgPack.writeString("lol wassup? rtndui adtrn dutiarned trndutilrcdtugiaeduitarn");
    try msgPack.writeString(&.{ 0x12, 0x34, 0x56, 0x67, 0x89, 0xab, 0xcd, 0xef });

    try msgPack.writeBytes("hello world");
    try msgPack.writeBytes("lol wassup? rtndui adtrn dutiarned trndutilrcdtugiaeduitarn");
    try msgPack.writeBytes(&.{ 0x12, 0x34, 0x56, 0x67, 0x89, 0xab, 0xcd, 0xef });

    try msgPack.writeBool(true);
    try msgPack.writeBool(false);
    try msgPack.writeBool(true);
    try msgPack.writeBool(false);

    std.debug.print("\n", .{});
    const writtenBuffer = buffer[0..try stream.getPos()];
    var i: usize = 0;
    while (true) : (i += 16) {
        if (i >= writtenBuffer.len) break;

        const line = writtenBuffer[i..std.math.min(writtenBuffer.len, i + 16)];

        var k: usize = 0;
        while (k < 16) : (k += 1) {
            if (k > 0) {
                if (@mod(k, 8) == 0) {
                    std.debug.print("  ", .{});
                }
            }
            if (k < line.len) {
                std.debug.print("{x:0>2} ", .{line[k]});
            } else {
                std.debug.print("   ", .{});
            }
        }
        std.debug.print("\t", .{});

        k = 0;
        while (k < 16) : (k += 1) {
            if (k > 0) {
                if (@mod(k, 8) == 0) {
                    std.debug.print("  ", .{});
                }
            }
            if (k < line.len) {
                std.debug.print("{:3} ", .{line[k]});
            } else {
                std.debug.print("    ", .{});
            }
        }
        std.debug.print("\t", .{});

        k = 0;
        while (k < 16) : (k += 1) {
            if (k >= line.len or line[k] == 0 or line[k] == 0xa or line[k] == 0xc) {
                std.debug.print(".", .{});
            } else {
                std.debug.print("{c}", .{line[k]});
            }
        }
        if (i > 0 and @mod(i, 16 * 4) == 0) std.debug.print("\n", .{});
        std.debug.print("\n", .{});
    }
    std.debug.print("\n", .{});

    var reader = msgPackReader(std.io.fixedBufferStream(writtenBuffer).reader());
    while (true) {
        reader.read(0) catch break;
    }
}
