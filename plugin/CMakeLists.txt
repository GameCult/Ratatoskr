cmake_minimum_required(VERSION 3.22)

# The OBS plugin: an obs_source that owns no transport. Everything below the
# source callbacks is ratatoskr-core, linked as a static library through the
# hand-written header in include/. Point CMAKE_PREFIX_PATH at a libobs build
# tree or SDK prefix that exports libobsConfig.cmake.

project(ratatoskr LANGUAGES C)

find_package(libobs REQUIRED CONFIG)

set(RATATOSKR_CORE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../target/release" CACHE PATH
    "Directory holding ratatoskr_core.lib built with `cargo build --release -p ratatoskr-core`")

add_library(ratatoskr MODULE
    src/ratatoskr_source.c)

target_include_directories(ratatoskr PRIVATE include)
target_compile_definitions(ratatoskr PRIVATE WIN32_LEAN_AND_MEAN NOMINMAX)
target_link_libraries(ratatoskr PRIVATE
    OBS::libobs
    "${RATATOSKR_CORE_DIR}/ratatoskr_core.lib"
    # what the Rust standard library and cultnet-rs pull in on Windows
    ws2_32 userenv bcrypt ntdll advapi32)

set_target_properties(ratatoskr PROPERTIES PREFIX "" OUTPUT_NAME "ratatoskr")

# Lay out a plugin directory OBS loads from %APPDATA%/obs-studio/plugins/ratatoskr.
install(TARGETS ratatoskr RUNTIME DESTINATION bin/64bit LIBRARY DESTINATION bin/64bit)
install(DIRECTORY data/ DESTINATION data)

# Proves the C ABI is callable as the header claims, by loading the cdylib the
# way a host would. Built here so it uses the same compiler as the plugin.
add_executable(abi_smoke tests/abi_smoke.c)
target_include_directories(abi_smoke PRIVATE include)
include(CTest)
add_test(NAME abi_smoke COMMAND abi_smoke "${RATATOSKR_CORE_DIR}/ratatoskr_core.dll")
