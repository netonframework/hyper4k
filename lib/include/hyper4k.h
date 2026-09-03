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

/* ---------------------------------------------------------------------------
 * ABI v4: shared surface
 *
 * Every type crossing this boundary is fixed width. A C `enum` has
 * implementation-defined width, so freezing values without freezing the
 * representation would freeze nothing. The static assertions below fail the
 * build if that ever regresses.
 * ------------------------------------------------------------------------- */

typedef int32_t Hyper4kStatus;

#define HYPER4K_STATUS_OK                 ((Hyper4kStatus)  0)
#define HYPER4K_STATUS_ABI_MISMATCH       ((Hyper4kStatus) -1)
#define HYPER4K_STATUS_STRUCT_SIZE        ((Hyper4kStatus) -2)
#define HYPER4K_STATUS_UNKNOWN_FLAGS      ((Hyper4kStatus) -3)
#define HYPER4K_STATUS_INVALID_ARG        ((Hyper4kStatus) -4)
#define HYPER4K_STATUS_UNSUPPORTED        ((Hyper4kStatus) -5)
#define HYPER4K_STATUS_CLIENT_CLOSED      ((Hyper4kStatus) -6)
#define HYPER4K_STATUS_OOM                ((Hyper4kStatus) -7)
#define HYPER4K_STATUS_RESOURCE_EXHAUSTED ((Hyper4kStatus) -8)
#define HYPER4K_STATUS_NOT_FOUND          ((Hyper4kStatus)-20)
#define HYPER4K_STATUS_ALREADY_DONE       ((Hyper4kStatus)-21)
#define HYPER4K_STATUS_NOT_PAUSED         ((Hyper4kStatus)-22)

typedef int32_t Hyper4kErrorKind;

#define HYPER4K_ERR_NONE             ((Hyper4kErrorKind)  0)
#define HYPER4K_ERR_DNS              ((Hyper4kErrorKind)  1)
#define HYPER4K_ERR_CONNECT          ((Hyper4kErrorKind)  2)
#define HYPER4K_ERR_TLS_CA           ((Hyper4kErrorKind)  3)
#define HYPER4K_ERR_TLS_HOSTNAME     ((Hyper4kErrorKind)  4)
#define HYPER4K_ERR_TLS_EXPIRED      ((Hyper4kErrorKind)  5)
#define HYPER4K_ERR_TLS_OTHER        ((Hyper4kErrorKind)  6)
#define HYPER4K_ERR_ALPN_NO_H2       ((Hyper4kErrorKind)  7)
#define HYPER4K_ERR_PROTOCOL         ((Hyper4kErrorKind)  8)
#define HYPER4K_ERR_TIMEOUT          ((Hyper4kErrorKind)  9)
#define HYPER4K_ERR_IDLE_TIMEOUT     ((Hyper4kErrorKind) 10)
#define HYPER4K_ERR_CANCELLED        ((Hyper4kErrorKind) 11)
#define HYPER4K_ERR_TRUNCATED        ((Hyper4kErrorKind) 12)
#define HYPER4K_ERR_OUTCOME_UNKNOWN  ((Hyper4kErrorKind) 13)

/* Headers have no "pause before the next chunk", so the two actions are
   separate types rather than one enum with an undefined combination. */
typedef int32_t Hyper4kHeadersAction;
#define HYPER4K_HEADERS_CONTINUE ((Hyper4kHeadersAction) 0)
#define HYPER4K_HEADERS_CANCEL   ((Hyper4kHeadersAction) 2)

typedef int32_t Hyper4kChunkAction;
#define HYPER4K_CHUNK_CONTINUE   ((Hyper4kChunkAction) 0)
#define HYPER4K_CHUNK_PAUSE      ((Hyper4kChunkAction) 1)
#define HYPER4K_CHUNK_CANCEL     ((Hyper4kChunkAction) 2)

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(Hyper4kStatus)        == 4, "Hyper4kStatus must be 32-bit");
_Static_assert(sizeof(Hyper4kErrorKind)     == 4, "Hyper4kErrorKind must be 32-bit");
_Static_assert(sizeof(Hyper4kHeadersAction) == 4, "Hyper4kHeadersAction must be 32-bit");
_Static_assert(sizeof(Hyper4kChunkAction)   == 4, "Hyper4kChunkAction must be 32-bit");
#endif

/* (major << 16) | minor */
uint32_t    hyper4k_abi_version(void);
/* NUL-terminated, static storage. Do not free. */
const char *hyper4k_version(void);
/* Bit is set only when the feature is implemented AND tested. */
uint64_t    hyper4k_server_capabilities(void);
uint64_t    hyper4k_client_capabilities(void);

#define HYPER4K_SERVER_CAP_HTTP1     (1ull << 0)
#define HYPER4K_SERVER_CAP_H2C       (1ull << 1)
#define HYPER4K_SERVER_CAP_STREAMING (1ull << 2)

#define HYPER4K_CLIENT_CAP_HTTP1     (1ull << 0)
#define HYPER4K_CLIENT_CAP_HTTP2     (1ull << 1)
#define HYPER4K_CLIENT_CAP_TLS       (1ull << 2)
#define HYPER4K_CLIENT_CAP_CUSTOM_CA (1ull << 3)
#define HYPER4K_CLIENT_CAP_CANCEL    (1ull << 4)
#define HYPER4K_CLIENT_CAP_STREAMING (1ull << 5)
#define HYPER4K_CLIENT_CAP_PROXY     (1ull << 6)  /* ABI 4.1 */


/* 借用的字节切片（ptr 可能为 NULL 当 len == 0） */
typedef struct Hyper4kSlice {
    const uint8_t *ptr;
    size_t len;
} Hyper4kSlice;

/* ---------------------------------------------------------------------------
 * ABI v4: outbound client
 *
 * Lifecycle:
 *   new -> send* -> close -> free
 *
 * Threading:
 *   send / cancel / resume / close may be called from ANY thread, including a
 *   callback thread. All four are re-entrant and non-blocking.
 *   free requires exclusive ownership and MUST NOT be called from a callback
 *   thread: it waits for the very bridge that is running the callback.
 *   free(NULL) and double free are NOT safe; the wrapper must call it once.
 *
 * Callbacks:
 *   Ordering per request_id: OnHeaders -> OnChunk* -> OnDone, strictly serial.
 *   Different request_ids may run concurrently, so the implementations must be
 *   thread safe. OnDone fires exactly once per accepted request and nothing
 *   follows it. Slices are borrowed for the duration of the call only.
 *   An exception must never cross this boundary: the crate is panic = "abort".
 *   Return CANCEL to abandon a request from inside a callback instead.
 * ------------------------------------------------------------------------- */

typedef struct Hyper4kClient Hyper4kClient;

typedef struct {
    Hyper4kSlice name;
    Hyper4kSlice value;
} Hyper4kHeader;

typedef struct {
    Hyper4kErrorKind kind;          /* stable category */
    uint32_t         protocol_code; /* e.g. an HTTP/2 error code, else 0       */
    Hyper4kSlice     message;       /* borrowed; for logs only, never branch   */
} Hyper4kError;

/* client flags */
#define HYPER4K_CLIENT_HTTP2_REQUIRED    (1ull << 0)  /* fail, never downgrade  */
#define HYPER4K_CLIENT_CA_REPLACE_SYSTEM (1ull << 1)  /* default is "append"    */

typedef struct {
    uint32_t       abi_version;
    uint32_t       struct_size;
    uint64_t       flags;
    uint64_t       connect_timeout_ms;    /* 0 disables                        */
    uint64_t       request_timeout_ms;    /* 0 disables; covers all retries    */
    uint64_t       read_idle_timeout_ms;  /* 0 disables; re-armed per chunk    */
    uint32_t       max_retries;           /* ADDITIONAL attempts; 0 = try once */
    uint32_t       max_inflight_requests; /* 0 = built-in default (1024)       */
    uint64_t       max_buffered_bytes;    /* 0 = built-in default (64 MiB)     */
    const uint8_t *custom_ca_pem;         /* NULL = platform roots only        */
    size_t         custom_ca_pem_len;
    /* ABI 4.1. NULL/0 = direct. "http://host[:port]": plaintext targets are
       sent in absolute-form, TLS targets through a CONNECT tunnel. Credentials
       and non-HTTP proxies are refused at hyper4k_client_new. */
    const uint8_t *proxy_url;
    size_t         proxy_url_len;
} Hyper4kClientOptions;

/* The Rust side is the source of truth for this layout. Before this assert the
   header lacked two fields (max_inflight_requests, max_buffered_bytes): a C or
   Kotlin caller then wrote custom_ca_pem eight bytes early, so the CA bundle
   silently never applied while the client reported success. */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(Hyper4kClientOptions) == 88, "Hyper4kClientOptions layout must match lib/src/client/mod.rs");
#endif

typedef struct {
    uint32_t             abi_version;
    uint32_t             struct_size;
    Hyper4kSlice         method;
    Hyper4kSlice         url;
    const Hyper4kHeader *headers;
    size_t               header_count;
    const uint8_t       *body_ptr;
    size_t               body_len;
    /* UINT64_MAX inherits the client default, 0 disables, else overrides.
       Inherit is NOT 0: a zeroed struct would silently disable the timeout. */
    uint64_t             read_idle_timeout_ms;
} Hyper4kClientRequest;

/*
 * Fill with this build's defaults. Pass the size YOU allocated: an older caller
 * loading a newer library would otherwise be written past the end of its own
 * buffer. Only min(struct_size, our size) bytes are touched.
 */
Hyper4kStatus hyper4k_client_options_init(Hyper4kClientOptions *opts,
                                          uint32_t struct_size);
Hyper4kStatus hyper4k_client_request_init(Hyper4kClientRequest *request,
                                          uint32_t struct_size);

/* headers/chunk actions are returned by the callbacks; see the constants above. */
typedef Hyper4kHeadersAction (*Hyper4kOnHeaders)(void *user_data,
                                                 uint64_t request_id,
                                                 uint16_t status,
                                                 uint8_t version, /* 1 or 2 */
                                                 const Hyper4kHeader *headers,
                                                 size_t header_count);

typedef Hyper4kChunkAction (*Hyper4kOnChunk)(void *user_data,
                                             uint64_t request_id,
                                             const uint8_t *ptr,
                                             size_t len);

/* error == NULL means success. HTTP 4xx/5xx are successes, not errors. */
typedef void (*Hyper4kOnDone)(void *user_data,
                              uint64_t request_id,
                              const Hyper4kError *error);

Hyper4kStatus hyper4k_client_new(const Hyper4kClientOptions *opts,
                                 Hyper4kClient **out_client);

/* Idempotent, non-blocking. Every accepted request still gets one OnDone. */
void hyper4k_client_close(Hyper4kClient *client);

/* Blocks until nothing can call back any more, then frees. */
void hyper4k_client_free(Hyper4kClient *client);

/*
 * Submit a request. on_chunk may be NULL to discard the body.
 *
 * Guarantees: *out_request_id is written before any event can be produced, and
 * no callback re-enters the calling thread synchronously. A callback MAY run on
 * another thread concurrently with this returning.
 */
Hyper4kStatus hyper4k_client_send(Hyper4kClient *client,
                                  const Hyper4kClientRequest *request,
                                  Hyper4kOnHeaders on_headers,
                                  Hyper4kOnChunk on_chunk,
                                  Hyper4kOnDone on_done,
                                  void *user_data,
                                  uint64_t *out_request_id);

/* ACCEPTED / ALREADY_DONE / NOT_FOUND. A cancelled request still gets OnDone. */
Hyper4kStatus hyper4k_client_cancel(Hyper4kClient *client, uint64_t request_id);

/*
 * Resume a body paused by HYPER4K_CHUNK_PAUSE. The paused chunk is NOT replayed.
 * Calling this from inside the pausing callback is allowed and is not lost.
 */
Hyper4kStatus hyper4k_client_resume(Hyper4kClient *client, uint64_t request_id);

/* Diagnostics: parked streams across all pooled connections. */
uint32_t hyper4k_client_paused_stream_count(Hyper4kClient *client);

/* 不透明句柄 */
typedef struct Hyper4kServer Hyper4kServer;
typedef uint64_t Hyper4kResponder;


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
