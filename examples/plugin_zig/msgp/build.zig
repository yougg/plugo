const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{
        .default_target = .{
            .cpu_arch = .x86_64,
            .os_tag = .linux,
            .abi = .gnu,
        },
    });
    const optimize: std.builtin.OptimizeMode = if (b.option(
        std.builtin.OptimizeMode,
        "optimize",
        "Prioritize performance, safety, or binary size (-O flag)",
    )) |opt| opt else .ReleaseSmall;

    const exe = b.addExecutable(.{
        .name = "plugin_zig_msgp",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .linkage = .static,
    });

    const msgpack_dep = b.dependency("zig_msgpack", .{
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("msgpack", msgpack_dep.module("msgpack"));

    const plugz_mod = b.createModule(.{
        .root_source_file = b.path("../plugz.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("plugz", plugz_mod);

    exe.build_id = .sha1;
    // exe.lto = .full;
    // exe.pie = true;

    exe.forceUndefinedSymbol("zig_version"); // readelf -p .comment <file_path>

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // Unit tests
    const exe_unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe_unit_tests.root_module.addImport("msgpack", msgpack_dep.module("msgpack"));
    exe_unit_tests.root_module.addImport("plugz", plugz_mod);

    const run_exe_unit_tests = b.addRunArtifact(exe_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_exe_unit_tests.step);
}

