const std = @import("std");
const msgpack = @import("msgpack");

// Generic serialization method: convert any Zig struct to msgpack.Payload
pub fn marshal(allocator: std.mem.Allocator, value: anytype) !msgpack.Payload {
    const T = @TypeOf(value);
    const info = @typeInfo(T);

    switch (info) {
        .int => |int_info| {
            if (int_info.signedness == .signed) {
                return msgpack.Payload.intToPayload(@intCast(value));
            } else {
                return msgpack.Payload.uintToPayload(@intCast(value));
            }
        },
        .pointer => |ptr_info| {
            if (ptr_info.size == .slice and ptr_info.child == u8) {
                return msgpack.Payload.strToPayload(value, allocator);
            }
            return error.UnsupportedType;
        },
        .@"struct" => |struct_info| {
            var map = msgpack.Payload.mapPayload(allocator);
            errdefer map.free(allocator);

            inline for (struct_info.fields) |field| {
                const field_val = @field(value, field.name);
                const field_payload = try marshal(allocator, field_val);
                try map.mapPut(field.name, field_payload);
            }
            return map;
        },
        else => return error.UnsupportedType,
    }
}

// Generic deserialization method: map msgpack.Payload to target struct T
pub fn unmarshal(comptime T: type, payload: msgpack.Payload) !T {
    const info = @typeInfo(T);

    switch (info) {
        .int => {
            if (payload != .uint and payload != .int) return error.TypeMismatch;
            return if (payload == .uint) @intCast(payload.uint) else @intCast(payload.int);
        },
        .pointer => |ptr_info| {
            if (ptr_info.size == .slice and ptr_info.child == u8) {
                if (payload != .str) return error.TypeMismatch;
                return payload.str.value();
            }
            return error.UnsupportedType;
        },
        .@"struct" => |struct_info| {
            if (payload != .map) return error.TypeMismatch;
            var result: T = undefined;

            inline for (struct_info.fields) |field| {
                const field_payload = try payload.mapGet(field.name);
                if (field_payload == null) {
                    return error.MissingField;
                }
                const field_val = try unmarshal(field.type, field_payload.?);
                @field(result, field.name) = field_val;
            }
            return result;
        },
        else => return error.UnsupportedType,
    }
}
