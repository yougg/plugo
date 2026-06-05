const std = @import("std");
const builtin = @import("builtin");
const string = []const u8;

pub const ExtraResult = struct {
    node_id: string,
    status: string,
};

pub const TaskResult = struct {
    message: string,
    extra: ExtraResult,
};

pub const ResponsePayload = struct {
    code: i64,
    result: TaskResult,
};

pub const IPCRespMessage = struct {
    id: u32,
    command: string,
    payload: ResponsePayload,
};

pub const TaskInfo = struct {
    step: i64,
    options: string,
};

pub const RequestPayload = struct {
    task_name: string,
    task_detail: TaskInfo,
};

pub const IPCReqMessage = struct {
    id: u32,
    command: string,
    payload: RequestPayload,
};

pub const HandshakeRequest = struct {
    supported: []string,
};

pub var win_read_handle: std.os.windows.HANDLE = undefined;
pub var win_write_handle: std.os.windows.HANDLE = undefined;
pub var unix_fd: i32 = 3;

pub fn readExact(buf: []u8) !void {
    var bytes_read: usize = 0;
    while (bytes_read < buf.len) {
        if (builtin.os.tag == .windows) {
            var io_status: std.os.windows.IO_STATUS_BLOCK = undefined;
            const rc = std.os.windows.ntdll.NtReadFile(win_read_handle, null, null, null, &io_status, buf[bytes_read..].ptr, @as(u32, @intCast(buf.len - bytes_read)), null, null);
            if (rc != .SUCCESS) return error.ReadError;
            if (io_status.Information == 0) return error.EndOfStream;
            bytes_read += io_status.Information;
        } else {
            const n = try std.posix.read(unix_fd, buf[bytes_read..]);
            if (n == 0) return error.EndOfStream;
            bytes_read += n;
        }
    }
}

pub fn writeAll(buf: string) !void {
    var bytes_written: usize = 0;
    while (bytes_written < buf.len) {
        if (builtin.os.tag == .windows) {
            var io_status: std.os.windows.IO_STATUS_BLOCK = undefined;
            const rc = std.os.windows.ntdll.NtWriteFile(win_write_handle, null, null, null, &io_status, buf[bytes_written..].ptr, @as(u32, @intCast(buf.len - bytes_written)), null, null);
            if (rc != .SUCCESS) return error.WriteError;
            bytes_written += io_status.Information;
        } else {
            const rc = std.os.linux.write(unix_fd, buf.ptr + bytes_written, buf.len - bytes_written);
            const signed_rc: isize = @bitCast(rc);
            if (signed_rc <= 0) return error.WriteError;
            bytes_written += @intCast(signed_rc);
        }
    }
}

pub fn runPlugin(
    allocator: std.mem.Allocator,
    plugin_name: string,
    codec_name: string,
    node_id: string,
    process_prefix: string,
    comptime CodecImpl: type,
) !void {
    std.debug.print("[{s}] Reading handshake request from host...\n", .{plugin_name});

    var len_buf: [4]u8 = undefined;
    try readExact(&len_buf);
    const handshake_req_len = std.mem.readInt(u32, &len_buf, .big);

    const req_buf = try allocator.alloc(u8, handshake_req_len);
    defer allocator.free(req_buf);

    try readExact(req_buf);

    var parsed = try std.json.parseFromSlice(HandshakeRequest, allocator, req_buf, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var has_codec = false;
    for (parsed.value.supported) |supported_codec| {
        if (std.mem.eql(u8, supported_codec, codec_name)) {
            has_codec = true;
            break;
        }
    }

    if (!has_codec) {
        std.debug.print("[{s}] Host does not support {s} format, negotiation failed\n", .{ plugin_name, codec_name });
        return error.NoMatchingCodec;
    }

    const resp_json = try std.fmt.allocPrint(allocator, "{{\"selected\":\"{s}\"}}", .{codec_name});
    defer allocator.free(resp_json);

    var resp_header_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &resp_header_buf, @intCast(resp_json.len), .big);
    try writeAll(&resp_header_buf);
    try writeAll(resp_json);

    std.debug.print("[{s}] Negotiation successful, selected {s} format for communication!\n", .{ plugin_name, codec_name });

    while (true) {
        var loop_arena = std.heap.ArenaAllocator.init(allocator);
        defer loop_arena.deinit();
        const loop_alloc = loop_arena.allocator();

        var loop_header_buf: [12]u8 = undefined;
        readExact(&loop_header_buf) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };

        const frame_len = std.mem.readInt(u32, loop_header_buf[0..4], .big);
        const stream_id = std.mem.readInt(u32, loop_header_buf[4..8], .big);
        const flags = std.mem.readInt(u32, loop_header_buf[8..12], .big);

        if (stream_id != 0) {
            if (frame_len > 0) {
                const discard = try loop_alloc.alloc(u8, frame_len);
                try readExact(discard);
            }
            continue;
        }

        const payload_buf = try loop_alloc.alloc(u8, frame_len);
        try readExact(payload_buf);

        if (flags & 8 != 0) {
            return error.ErrorFrameReceived;
        }

        const req_msg = try CodecImpl.decode(loop_alloc, payload_buf);

        std.debug.print("[{s}] Received task - ID: {d}, Cmd: {s}, TaskName: {s}\n", .{ plugin_name, req_msg.id, req_msg.command, req_msg.payload.task_name });

        const processed_payload = try std.fmt.allocPrint(loop_alloc, "{s}{s}", .{ process_prefix, req_msg.payload.task_name });

        const resp_msg = IPCRespMessage{
            .id = req_msg.id,
            .command = req_msg.command,
            .payload = .{
                .code = 200,
                .result = .{
                    .message = processed_payload,
                    .extra = .{
                        .node_id = node_id,
                        .status = "SUCCESS",
                    },
                },
            },
        };

        const out_payload = try CodecImpl.encode(loop_alloc, resp_msg);

        const out_len: u32 = @intCast(out_payload.len);
        var out_header_buf: [12]u8 = undefined;
        std.mem.writeInt(u32, out_header_buf[0..4], out_len, .big);
        std.mem.writeInt(u32, out_header_buf[4..8], 0, .big);
        std.mem.writeInt(u32, out_header_buf[8..12], 0, .big);
        try writeAll(&out_header_buf);
        try writeAll(out_payload);
    }

    std.debug.print("[{s}] Host exited, connection closed.\n", .{plugin_name});
}

pub fn initPlugin(init: std.process.Init, plugin_name: string) !void {
    if (builtin.os.tag == .windows) {
        const map = init.environ_map;
        const read_env = map.get("PLUGO_PIPE_READ") orelse return error.MissingEnv;
        const write_env = map.get("PLUGO_PIPE_WRITE") orelse return error.MissingEnv;

        const read_handle_val = try std.fmt.parseInt(usize, read_env, 10);
        const write_handle_val = try std.fmt.parseInt(usize, write_env, 10);

        win_read_handle = @ptrFromInt(read_handle_val);
        win_write_handle = @ptrFromInt(write_handle_val);
        std.debug.print("[{s}] Plugin started successfully, taking over handles via environment...\n", .{plugin_name});
    } else {
        unix_fd = 3;
        std.debug.print("[{s}] Plugin started successfully, taking over FD 3 via descriptor inheritance...\n", .{plugin_name});
    }
}
