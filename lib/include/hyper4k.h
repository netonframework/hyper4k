/*
 * hyper4k — C ABI
 *
 * 借用切片 ABI：
 *   - 进方向（请求）：Rust 把请求字段以 (ptr,len) 借用给回调，回调期间有效，
 *     直到你调用 hyper4k_respond() 为止。Kotlin 侧若要异步处理，必须先把需要
 *     的字节拷走，再返回。
 *   - 出方向（响应）：你把响应字节以 (ptr,len) 传回，hyper4k_respond() 内部拷贝
 *     一次后即可释放你的缓冲。
 *
 * 线程模型（push 模型）：
 *   on_request 回调在 Tokio worker 线程上被调用。语义上它应当“尽快返回”，并在
 *   之后（可在另一线程）调用 hyper4k_respond() 完成该请求。每个请求对应一个独立的
 *   Hyper4kResponder。句柄失效后的迟到或重复响应会安全返回失败。
 *
 * panic 安全：crate 以 panic = "abort" 编译，保证 Rust panic 不会跨越 FFI 边界。
 */
#ifndef HYPER4K_H
#define HYPER4K_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* 不透明句柄 */
typedef struct Hyper4kServer Hyper4kServer;
typedef uint64_t Hyper4kResponder;

/* 借用的字节切片（ptr 可能为 NULL 当 len == 0） */
typedef struct Hyper4kSlice {
    const uint8_t *ptr;
    size_t len;
} Hyper4kSlice;

/*
 * 单次请求视图。所有切片在 on_request 调用期间有效，
 * 在你调用 hyper4k_respond(responder, ...) 之前保持有效。异步消费者应在回调返回前复制。
 * headers 为扁平文本块，每行 "Name: Value\n"。
 */
typedef struct Hyper4kRequest {
    Hyper4kSlice method;    /* GET / POST / ...                 */
    Hyper4kSlice path;      /* /foo/bar （不含 query）          */
    Hyper4kSlice query;     /* a=1&b=2 （不含 '?'）             */
    Hyper4kSlice headers;   /* "Name: Value\n" 串联             */
    Hyper4kSlice body;      /* 已聚合的请求体（v1 非流式）      */
    Hyper4kResponder responder;
} Hyper4kRequest;

/* 每请求回调。user_data 即 start 时传入的指针。 */
typedef void (*Hyper4kRequestCallback)(void *user_data, const Hyper4kRequest *req);

/*
 * 启动服务器。绑定失败（端口占用等）返回 NULL。
 * host 为 NUL 结尾 C 字符串，如 "0.0.0.0"。
 */
Hyper4kServer *hyper4k_server_start(const char *host,
                                   uint16_t port,
                                   Hyper4kRequestCallback on_request,
                                   void *user_data);

/*
 * 完成一个请求（拷贝版）。返回 1 表示已交付，0 表示 responder 已失效或已经完成。
 * headers 与 body 在本调用内被拷贝，返回后你的缓冲即可释放。
 * headers 编码同请求：每行 "Name: Value\n"（可为空 -> 传 NULL,0）。
 *
 * ABI v2 同步快路径：若在 on_request 回调内调用本函数（即 handler 同步完成），
 * 响应直接回填给连接处理协程，不经过异步通道。
 */
int32_t hyper4k_respond(Hyper4kResponder responder,
                       uint16_t status,
                       const uint8_t *headers_ptr, size_t headers_len,
                       const uint8_t *body_ptr, size_t body_len);

/* -------------------------------------------------------------------------
 * ABI v3: streaming responses
 *
 * These three calls are mutually exclusive with hyper4k_respond: a responder
 * either answers once or streams. Mixing them returns HYPER4K_ERR_WRONG_STATE
 * rather than invoking undefined behaviour.
 * ------------------------------------------------------------------------- */

#define HYPER4K_OK                 (1)   /* success                                       */
#define HYPER4K_FAILED             (0)   /* responder is stale or already completed        */
#define HYPER4K_ERR_WRONG_STATE   (-4)   /* the responder's state forbids this call        */
#define HYPER4K_ERR_CLIENT_GONE   (-5)   /* client is gone; stop writing and call finish   */
#define HYPER4K_ERR_WOULD_BLOCK   (-6)   /* would block on an engine thread; see write     */

/*
 * Starts a streaming response: the status line and headers go out immediately,
 * the body follows as chunks.
 *
 * The responder enters the streaming state and hyper4k_respond no longer applies
 * to it. It must be closed with hyper4k_response_finish.
 *
 * Headers use the v2 encoding: one "Name: Value\n" per line.
 * Do not set Content-Length yourself: the engine frames a streaming body as
 * chunked (HTTP/1.1) or DATA frames (HTTP/2).
 */
int32_t hyper4k_response_begin(Hyper4kResponder responder,
                              uint16_t status,
                              const uint8_t *headers_ptr, size_t headers_len);

/*
 * Writes one body chunk. The data is copied during the call, so your buffer can
 * be released on return.
 *
 * Backpressure: while the client reads slowly this call blocks the calling
 * thread until the downstream is writable, so it must run on a thread where
 * blocking is safe. Called on an engine thread (a Tokio worker) it returns
 * HYPER4K_ERR_WOULD_BLOCK instead of blocking; that code means streaming writes
 * were left on the engine thread and belong on a blocking-capable dispatcher.
 *
 * HYPER4K_ERR_CLIENT_GONE means the client disconnected: stop producing data and
 * call finish. It is a normal path, and it is what an SSE client closing its tab
 * looks like.
 */
int32_t hyper4k_response_write(Hyper4kResponder responder,
                              const uint8_t *chunk_ptr, size_t chunk_len);

/*
 * Ends the streaming response and releases the responder.
 * Idempotent: a repeated call returns HYPER4K_ERR_WRONG_STATE, not UB.
 *
 * finish is required even after write returned HYPER4K_ERR_CLIENT_GONE,
 * otherwise the responder's entry is never reclaimed.
 */
int32_t hyper4k_response_finish(Hyper4kResponder responder);

/* 优雅停止并释放服务器句柄。停止后 server 指针失效。 */
void hyper4k_server_stop(Hyper4kServer *server);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* HYPER4K_H */
