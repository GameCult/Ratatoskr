/* Proves the C ABI is callable and behaves as its header claims.
 *
 * Loads the cdylib at runtime rather than linking it. That keeps the test
 * compiler-agnostic — the production plugin is built with MSVC, this runs
 * anywhere — and it exercises the library the way a host actually loads it.
 */
#include "ratatoskr.h"
#include <stdio.h>
#include <string.h>
#include <windows.h>

static int failures = 0;
static void check(int ok, const char *what)
{
    if (!ok) { printf("FAIL %s\n", what); failures++; }
    else { printf("ok   %s\n", what); }
}

typedef int (*open_fn)(const char *, const char *, unsigned int, RatatoskrHandle **);
typedef int (*poll_fn)(RatatoskrHandle *);
typedef int (*next_fn)(RatatoskrHandle *, uint8_t *, size_t, size_t *);
typedef uint16_t (*port_fn)(RatatoskrHandle *);
typedef void (*delivered_fn)(RatatoskrHandle *, uint64_t *, uint64_t *);
typedef size_t (*err_fn)(char *, size_t);
typedef void (*close_fn)(RatatoskrHandle *);

int main(int argc, char **argv)
{
    const char *dll = argc > 1 ? argv[1] : "ratatoskr_core.dll";
    HMODULE lib = LoadLibraryA(dll);
    if (!lib) { printf("FAIL could not load %s (error %lu)\n", dll, GetLastError()); return 1; }

    open_fn r_open = (open_fn)(void *)GetProcAddress(lib, "ratatoskr_receiver_open");
    poll_fn r_poll = (poll_fn)(void *)GetProcAddress(lib, "ratatoskr_receiver_poll");
    next_fn r_next = (next_fn)(void *)GetProcAddress(lib, "ratatoskr_receiver_next_payload");
    port_fn r_port = (port_fn)(void *)GetProcAddress(lib, "ratatoskr_receiver_local_port");
    delivered_fn r_delivered = (delivered_fn)(void *)GetProcAddress(lib, "ratatoskr_receiver_delivered");
    err_fn r_err = (err_fn)(void *)GetProcAddress(lib, "ratatoskr_last_error");
    close_fn r_close = (close_fn)(void *)GetProcAddress(lib, "ratatoskr_receiver_close");
    check(r_open && r_poll && r_next && r_port && r_delivered && r_err && r_close,
          "every documented symbol is exported");
    if (failures) { printf("ABI SMOKE FAILED\n"); return 1; }

    RatatoskrHandle *handle = NULL;
    check(r_open("127.0.0.1:0", "ratatoskr-abi-smoke", 0x0BE00001u, &handle) == RATATOSKR_OK,
          "open returns OK");
    check(handle != NULL, "open yields a handle");
    check(r_port(handle) != 0, "ephemeral bind resolves to a real port");
    check(r_poll(handle) == 0, "idle poll reports nothing waiting");

    size_t out_len = 12345;
    unsigned char buffer[64];
    check(r_next(handle, buffer, sizeof buffer, &out_len) == RATATOSKR_NONE,
          "empty queue reports NONE, not an error");
    check(out_len == 0, "NONE writes a zero length");

    uint64_t payloads = 7, bytes = 7;
    r_delivered(handle, &payloads, &bytes);
    check(payloads == 0 && bytes == 0, "delivered counts start at zero");
    r_close(handle);

    handle = NULL;
    check(r_open("not-an-address", "x", 1, &handle) == RATATOSKR_ERR_ARGUMENT,
          "a bad bind address is refused");
    check(handle == NULL, "a refused open leaves no handle");

    char message[256];
    check(r_err(message, sizeof message) > 0, "a failure leaves a message");
    check(strstr(message, "not-an-address") != NULL, "the message names the bad input");

    r_close(NULL);
    check(r_poll(NULL) == RATATOSKR_ERR_ARGUMENT, "null handle is refused");
    check(r_port(NULL) == 0, "null handle reports no port");

    printf("%s\n", failures ? "ABI SMOKE FAILED" : "ABI SMOKE PASSED");
    return failures ? 1 : 0;
}
