const std = @import("std");
const time = @import("time");

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

        pub fn read(self: *Self, indent: usize) anyerror!void {
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

pub fn msgPackReader(reader: anytype) MsgPackReader(@TypeOf(reader)) {
    return .{ .reader = reader };
}
