const std = @import("std");
const builtin = @import("builtin");
const msgpack = @import("msgpack");
const plugz = @import("plugz");
const codec = @import("codec.zig");

const zig_ver = "Zig " ++ builtin.zig_version_string;
export const zig_version: [zig_ver.len:0]u8 linksection(".comment") = zig_ver.*;

const MsgpackCodec = struct {
    pub fn decode(allocator: std.mem.Allocator, payload: []u8) !plugz.IPCReqMessage {
        var msgp_reader: std.Io.Reader = .fixed(payload);
        var dummy_write_buf: [0]u8 = undefined;
        var dummy_writer: std.Io.Writer = .fixed(&dummy_write_buf);
        var read_packer = msgpack.PackerIO.init(&msgp_reader, &dummy_writer);

        const req_payload = try read_packer.read(allocator);
        return try codec.unmarshal(plugz.IPCReqMessage, req_payload);
    }

    pub fn encode(allocator: std.mem.Allocator, resp: plugz.IPCRespMessage) ![]const u8 {
        var resp_writer_alloc = std.Io.Writer.Allocating.init(allocator);

        var dummy_read_buf: [0]u8 = undefined;
        var dummy_reader: std.Io.Reader = .fixed(&dummy_read_buf);
        var write_packer = msgpack.PackerIO.init(&dummy_reader, &resp_writer_alloc.writer);

        const resp_payload = try codec.marshal(allocator, resp);
        try write_packer.write(resp_payload);

        return resp_writer_alloc.written();
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    try plugz.initPlugin(init, "Zig Msgpack Plugin");
    try plugz.runPlugin(
        allocator,
        "Zig Msgpack Plugin",
        "msgp",
        "zig-msgp-node-1",
        "Processed by Zig Msgpack: ",
        MsgpackCodec,
    );
}

test "MsgpackCodec: encode and decode" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const req = plugz.IPCReqMessage{
        .id = 42,
        .command = "test_command",
        .payload = .{
            .task_name = "test_task",
            .task_detail = .{
                .step = 123,
                .options = "options_abc",
            },
        },
    };

    // Convert to Payload using codec.marshal, then serialize to bytes
    const req_payload = try codec.marshal(allocator, req);

    var req_writer = std.Io.Writer.Allocating.init(allocator);
    var dummy_reader_buf: [0]u8 = undefined;
    var dummy_reader: std.Io.Reader = .fixed(&dummy_reader_buf);
    var write_packer = msgpack.PackerIO.init(&dummy_reader, &req_writer.writer);
    try write_packer.write(req_payload);
    const req_bytes = req_writer.written();

    // Decode it using MsgpackCodec.decode
    const decoded = try MsgpackCodec.decode(allocator, @constCast(req_bytes));
    try std.testing.expectEqual(req.id, decoded.id);
    try std.testing.expectEqualStrings(req.command, decoded.command);
    try std.testing.expectEqualStrings(req.payload.task_name, decoded.payload.task_name);
    try std.testing.expectEqual(req.payload.task_detail.step, decoded.payload.task_detail.step);
    try std.testing.expectEqualStrings(req.payload.task_detail.options, decoded.payload.task_detail.options);

    // Test encode
    const resp = plugz.IPCRespMessage{
        .id = 42,
        .command = "test_command",
        .payload = .{
            .code = 200,
            .result = .{
                .message = "hello",
                .extra = .{
                    .node_id = "node-1",
                    .status = "SUCCESS",
                },
            },
        },
    };

    const resp_bytes = try MsgpackCodec.encode(allocator, resp);

    // Decode it back
    var resp_reader: std.Io.Reader = .fixed(resp_bytes);
    var dummy_write_buf: [0]u8 = undefined;
    var dummy_writer: std.Io.Writer = .fixed(&dummy_write_buf);
    var read_packer = msgpack.PackerIO.init(&resp_reader, &dummy_writer);
    const parsed_payload = try read_packer.read(allocator);

    const parsed_resp = try codec.unmarshal(plugz.IPCRespMessage, parsed_payload);

    try std.testing.expectEqual(resp.id, parsed_resp.id);
    try std.testing.expectEqualStrings(resp.command, parsed_resp.command);
    try std.testing.expectEqual(resp.payload.code, parsed_resp.payload.code);
    try std.testing.expectEqualStrings(resp.payload.result.message, parsed_resp.payload.result.message);
    try std.testing.expectEqualStrings(resp.payload.result.extra.node_id, parsed_resp.payload.result.extra.node_id);
    try std.testing.expectEqualStrings(resp.payload.result.extra.status, parsed_resp.payload.result.extra.status);
}


