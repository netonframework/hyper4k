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
 * ABI v3：流式响应
 *
 * 下面三个函数与 hyper4k_respond 互斥：一个 responder 要么走一次性应答，
 * 要么走流式，不能混用（混用返回 HYPER4K_ERR_WRONG_STATE，不是 UB）。
 * ------------------------------------------------------------------------- */

#define HYPER4K_OK                 (1)   /* 操作成功                                     */
#define HYPER4K_FAILED             (0)   /* responder 已失效或已完成（v2 语义）          */
#define HYPER4K_ERR_WRONG_STATE   (-4)   /* responder 状态不允许该操作                   */
#define HYPER4K_ERR_CLIENT_GONE   (-5)   /* 客户端已断开，停止写入并 finish              */
#define HYPER4K_ERR_WOULD_BLOCK   (-6)   /* 需要阻塞，但调用线程是引擎线程（见 write）   */

/*
 * 开始一个流式响应：立即发出状态行与响应头，body 随后分块写出。
 *
 * 调用后 responder 进入流式状态，hyper4k_respond 对它失效。必须以
 * hyper4k_response_finish 收尾。
 *
 * headers 编码同 v2：每行 "Name: Value\n"。
 * 不要自己设置 Content-Length —— 流式响应由引擎按协议选择
 * chunked(HTTP/1.1) 或 DATA 帧(HTTP/2)。
 */
int32_t hyper4k_response_begin(Hyper4kResponder responder,
                              uint16_t status,
                              const uint8_t *headers_ptr, size_t headers_len);

/*
 * 写出一个 body 块。数据在本调用内被拷贝，返回后你的缓冲即可释放。
 *
 * 背压：客户端读得慢时本函数会阻塞调用线程直到下游可写。因此它 MUST 在能安全
 * 阻塞的线程上调用。若在引擎线程（Tokio worker）上调用且此刻需要阻塞，本函数
 * 不会阻塞，而是立即返回 HYPER4K_ERR_WOULD_BLOCK —— 收到这个码说明调用方把
 * 流式写入留在了引擎线程上，应切到可阻塞的调度器。
 *
 * 返回 HYPER4K_ERR_CLIENT_GONE 表示客户端已断开：停止产生数据并调用 finish
 * 收尾，这不是错误路径，SSE 客户端关页面就是这个码。
 */
int32_t hyper4k_response_write(Hyper4kResponder responder,
                              const uint8_t *chunk_ptr, size_t chunk_len);

/*
 * 结束流式响应并释放 responder。之后该 responder 失效。
 * 幂等：重复调用返回 HYPER4K_ERR_WRONG_STATE 而不是 UB。
 *
 * 即使 write 已经返回 HYPER4K_ERR_CLIENT_GONE，也仍要调用一次 finish，
 * 否则该 responder 的条目不会被回收。
 */
int32_t hyper4k_response_finish(Hyper4kResponder responder);

/* 优雅停止并释放服务器句柄。停止后 server 指针失效。 */
void hyper4k_server_stop(Hyper4kServer *server);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* HYPER4K_H */
