const std = @import("std");
const builtin = @import("builtin");
const plugz = @import("plugz");

const zig_ver = "Zig " ++ builtin.zig_version_string;
export const zig_version: [zig_ver.len:0]u8 linksection(".comment") = zig_ver.*;

const JsonCodec = struct {
    pub fn decode(allocator: std.mem.Allocator, payload: []u8) !plugz.IPCReqMessage {
        const req_parsed = try std.json.parseFromSlice(plugz.IPCReqMessage, allocator, payload, .{ .ignore_unknown_fields = true });
        return req_parsed.value;
    }

    pub fn encode(allocator: std.mem.Allocator, resp: plugz.IPCRespMessage) ![]const u8 {
        var resp_writer_alloc = std.Io.Writer.Allocating.init(allocator);
        try std.json.Stringify.value(resp, .{}, &resp_writer_alloc.writer);
        return resp_writer_alloc.written();
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    try plugz.initPlugin(init, "Zig JSON Plugin");
    try plugz.runPlugin(
        allocator,
        "Zig JSON Plugin",
        "json",
        "zig-json-node-1",
        "Processed by Zig JSON: ",
        JsonCodec,
    );
}

test "JsonCodec: encode and decode" {
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

    // Serialize req to json string
    var req_writer = std.Io.Writer.Allocating.init(allocator);
    try std.json.Stringify.value(req, .{}, &req_writer.writer);
    const req_bytes = req_writer.written();

    // Decode it
    const decoded = try JsonCodec.decode(allocator, @constCast(req_bytes));
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

    const resp_bytes = try JsonCodec.encode(allocator, resp);

    // Parse it back using json for assertion
    const parsed_resp = try std.json.parseFromSlice(plugz.IPCRespMessage, allocator, resp_bytes, .{});

    try std.testing.expectEqual(resp.id, parsed_resp.value.id);
    try std.testing.expectEqualStrings(resp.command, parsed_resp.value.command);
    try std.testing.expectEqual(resp.payload.code, parsed_resp.value.payload.code);
    try std.testing.expectEqualStrings(resp.payload.result.message, parsed_resp.value.payload.result.message);
    try std.testing.expectEqualStrings(resp.payload.result.extra.node_id, parsed_resp.value.payload.result.extra.node_id);
    try std.testing.expectEqualStrings(resp.payload.result.extra.status, parsed_resp.value.payload.result.extra.status);
}


