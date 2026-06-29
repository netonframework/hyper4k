/*
 * hyper4k — C ABI
 *
 * 零拷贝设计原则：
 *   - 进方向（请求）：Rust 把请求字段以 (ptr,len) 借用给回调，回调期间有效，
 *     直到你调用 hyper4k_respond() 为止。Kotlin 侧若要异步处理，必须先把需要
 *     的字节拷走，再返回。
 *   - 出方向（响应）：你把响应字节以 (ptr,len) 传回，hyper4k_respond() 内部拷贝
 *     一次后即可释放你的缓冲。
 *
 * 线程模型（push 模型）：
 *   on_request 回调在 Tokio worker 线程上被调用。语义上它应当“尽快返回”，并在
 *   之后（可在另一线程）调用 hyper4k_respond() 完成该请求。每个请求对应一个独立的
 *   Hyper4kResponder*，只能被 respond 一次。
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
typedef struct Hyper4kResponder Hyper4kResponder;

/* 借用的字节切片（ptr 可能为 NULL 当 len == 0） */
typedef struct Hyper4kSlice {
    const uint8_t *ptr;
    size_t len;
} Hyper4kSlice;

/*
 * 单次请求视图。所有切片在 on_request 调用期间有效，
 * 在你调用 hyper4k_respond(responder, ...) 之前保持有效。
 * headers 为扁平文本块，每行 "Name: Value\n"。
 */
typedef struct Hyper4kRequest {
    Hyper4kSlice method;    /* GET / POST / ...                 */
    Hyper4kSlice path;      /* /foo/bar （不含 query）          */
    Hyper4kSlice query;     /* a=1&b=2 （不含 '?'）             */
    Hyper4kSlice headers;   /* "Name: Value\n" 串联             */
    Hyper4kSlice body;      /* 已聚合的请求体（v1 非流式）      */
    Hyper4kResponder *responder;
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
 * 完成一个请求。每个 responder 必须且只能调用一次。
 * headers 与 body 在本调用内被拷贝，返回后你的缓冲即可释放。
 * headers 编码同请求：每行 "Name: Value\n"（可为空 -> 传 NULL,0）。
 */
void hyper4k_respond(Hyper4kResponder *responder,
                     uint16_t status,
                     const uint8_t *headers_ptr, size_t headers_len,
                     const uint8_t *body_ptr, size_t body_len);

/* 优雅停止并释放服务器句柄。停止后 server 指针失效。 */
void hyper4k_server_stop(Hyper4kServer *server);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* HYPER4K_H */
