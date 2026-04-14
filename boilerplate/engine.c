#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_BACKLOG 32
#define COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 128
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
#define STOP_GRACE_SECONDS 3

typedef enum {
    CMD_START = 1,
    CMD_RUN = 2,
    CMD_PS = 3,
    CMD_LOGS = 4,
    CMD_STOP = 5,
    CMD_WAIT = 6
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_HARD_LIMIT_KILLED,
    CONTAINER_EXITED,
    CONTAINER_START_FAILED
} container_state_t;

typedef enum {
    TERMINATION_NONE = 0,
    TERMINATION_NORMAL_EXIT,
    TERMINATION_STOPPED_GRACEFUL,
    TERMINATION_STOPPED_FORCED,
    TERMINATION_HARD_LIMIT_KILLED,
    TERMINATION_SIGNALLED,
    TERMINATION_START_FAILED
} termination_reason_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int result;
    int status;
    uint32_t payload_len;
} control_response_header_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    time_t finished_at;
    container_state_t state;
    termination_reason_t reason;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int stop_force_kill;
    int monitor_registered;
    int log_read_fd;
    pthread_t producer_thread;
    int producer_started;
    int producer_joined;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int shutting_down;
    pthread_t logger_thread;
    pthread_t reaper_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_arg_t;

typedef struct {
    int result;
    int status;
    char *payload;
} client_response_t;

static volatile sig_atomic_t g_supervisor_stop = 0;
static volatile sig_atomic_t g_run_signal = 0;

static void supervisor_signal_handler(int signo)
{
    (void)signo;
    g_supervisor_stop = 1;
}

static void run_signal_handler(int signo)
{
    (void)signo;
    g_run_signal = 1;
}

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int write_all(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t written = 0;

    while (written < len) {
        ssize_t n = write(fd, p + written, len - written);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        written += (size_t)n;
    }

    return 0;
}

static int read_all(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    size_t read_bytes = 0;

    while (read_bytes < len) {
        ssize_t n = read(fd, p + read_bytes, len - read_bytes);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        read_bytes += (size_t)n;
    }

    return 0;
}

static char *dup_text(const char *s)
{
    size_t len;
    char *out;

    if (!s)
        return NULL;

    len = strlen(s);
    out = malloc(len + 1);
    if (!out)
        return NULL;

    memcpy(out, s, len + 1);
    return out;
}

static char *format_text(const char *fmt, ...)
{
    va_list ap;
    va_list ap_copy;
    int needed;
    char *buf;

    va_start(ap, fmt);
    va_copy(ap_copy, ap);
    needed = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (needed < 0) {
        va_end(ap_copy);
        return NULL;
    }

    buf = malloc((size_t)needed + 1);
    if (!buf) {
        va_end(ap_copy);
        return NULL;
    }

    vsnprintf(buf, (size_t)needed + 1, fmt, ap_copy);
    va_end(ap_copy);
    return buf;
}

static int send_response(int fd, int result, int status, const char *payload)
{
    control_response_header_t hdr;
    size_t payload_len = payload ? strlen(payload) : 0;

    hdr.result = result;
    hdr.status = status;
    hdr.payload_len = (uint32_t)payload_len;

    if (write_all(fd, &hdr, sizeof(hdr)) < 0)
        return -1;

    if (payload_len > 0 && write_all(fd, payload, payload_len) < 0)
        return -1;

    return 0;
}

static int connect_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}
static int send_request_and_receive(const control_request_t *req, client_response_t *resp)
{
    int fd;
    control_response_header_t hdr;
    char *payload = NULL;

    memset(resp, 0, sizeof(*resp));

    fd = connect_control_socket();
    if (fd < 0)
        return -1;

    if (write_all(fd, req, sizeof(*req)) < 0) {
        close(fd);
        return -1;
    }

    if (read_all(fd, &hdr, sizeof(hdr)) < 0) {
        close(fd);
        return -1;
    }

    if (hdr.payload_len > 0) {
        if (hdr.payload_len > (16U * 1024U * 1024U)) {
            close(fd);
            return -1;
        }
        payload = malloc((size_t)hdr.payload_len + 1);
        if (!payload) {
            close(fd);
            return -1;
        }
        if (read_all(fd, payload, hdr.payload_len) < 0) {
            free(payload);
            close(fd);
            return -1;
        }
        payload[hdr.payload_len] = '\0';
    }

    close(fd);
    resp->result = hdr.result;
    resp->status = hdr.status;
    resp->payload = payload;
    return 0;
}

static int parse_mib_flag(const char *flag, const char *value, unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc, char *argv[], int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static int validate_container_id(const char *id)
{
    size_t i;
    size_t len;

    if (!id)
        return 0;

    len = strlen(id);
    if (len == 0 || len >= CONTAINER_ID_LEN)
        return 0;

    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char)id[i];
        if (!isalnum(c) && c != '-' && c != '_')
            return 0;
    }

    return 1;
}

static int canonicalize_rootfs(const char *input, char output[PATH_MAX])
{
    struct stat st;
    char *resolved;

    resolved = realpath(input, output);
    if (!resolved)
        return -1;

    if (stat(output, &st) < 0)
        return -1;

    if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case CONTAINER_EXITED:
        return "exited";
    case CONTAINER_START_FAILED:
        return "start_failed";
    default:
        return "unknown";
    }
}

static const char *reason_to_string(termination_reason_t reason)
{
    switch (reason) {
    case TERMINATION_NONE:
        return "none";
    case TERMINATION_NORMAL_EXIT:
        return "normal_exit";
    case TERMINATION_STOPPED_GRACEFUL:
        return "stopped_graceful";
    case TERMINATION_STOPPED_FORCED:
        return "stopped_forced";
    case TERMINATION_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case TERMINATION_SIGNALLED:
        return "signalled";
    case TERMINATION_START_FAILED:
        return "start_failed";
    default:
        return "unknown";
    }
}

static int is_terminal_state(container_state_t state)
{
    return state == CONTAINER_STOPPED ||
           state == CONTAINER_HARD_LIMIT_KILLED ||
           state == CONTAINER_EXITED ||
           state == CONTAINER_START_FAILED;
}

static int is_active_state(container_state_t state)
{
    return state == CONTAINER_STARTING || state == CONTAINER_RUNNING;
}

static container_record_t *find_container_by_id_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (strncmp(cur->id, id, CONTAINER_ID_LEN) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (cur->host_pid == pid)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static int rootfs_in_use_locked(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (is_active_state(cur->state) && strncmp(cur->rootfs, rootfs, PATH_MAX) == 0)
            return 1;
        cur = cur->next;
    }
    return 0;
}

static int running_count_locked(supervisor_ctx_t *ctx)
{
    int count = 0;
    container_record_t *cur = ctx->containers;

    while (cur) {
        if (is_active_state(cur->state))
            count++;
        cur = cur->next;
    }
    return count;
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}
static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0)
            continue;

        (void)write_all(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

static void *log_producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, parg->container_id, sizeof(item.container_id) - 1);

    for (;;) {
        n = read(parg->read_fd, item.data, sizeof(item.data));
        if (n > 0) {
            item.length = (size_t)n;
            if (bounded_buffer_push(&parg->ctx->log_buffer, &item) != 0)
                break;
            continue;
        }

        if (n == 0)
            break;

        if (errno == EINTR)
            continue;

        break;
    }

    close(parg->read_fd);
    free(parg);
    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0)
        return 127;
    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0)
        return 127;
    close(cfg->log_write_fd);

    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        return 127;

    if (cfg->nice_value != 0)
        (void)setpriority(PRIO_PROCESS, 0, cfg->nice_value);

    (void)mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL);

    if (chroot(cfg->rootfs) < 0)
        return 127;
    if (chdir("/") < 0)
        return 127;

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST)
        return 127;
    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        return 127;

    execl(cfg->command, cfg->command, (char *)NULL);
    return 127;
}

static void update_exit_metadata_locked(container_record_t *rec, int status)
{
    if (rec->state == CONTAINER_START_FAILED) {
        rec->finished_at = time(NULL);
        return;
    }

    rec->finished_at = time(NULL);

    if (WIFEXITED(status)) {
        rec->exit_code = WEXITSTATUS(status);
        rec->exit_signal = 0;
        if (rec->stop_requested) {
            rec->state = CONTAINER_STOPPED;
            rec->reason = rec->stop_force_kill ? TERMINATION_STOPPED_FORCED
                                               : TERMINATION_STOPPED_GRACEFUL;
        } else {
            rec->state = CONTAINER_EXITED;
            rec->reason = TERMINATION_NORMAL_EXIT;
        }
        return;
    }

    if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        rec->exit_code = 128 + sig;
        rec->exit_signal = sig;

        if (rec->stop_requested) {
            rec->state = CONTAINER_STOPPED;
            rec->reason = (rec->stop_force_kill || sig == SIGKILL)
                              ? TERMINATION_STOPPED_FORCED
                              : TERMINATION_STOPPED_GRACEFUL;
        } else if (sig == SIGKILL) {
            rec->state = CONTAINER_HARD_LIMIT_KILLED;
            rec->reason = TERMINATION_HARD_LIMIT_KILLED;
        } else {
            rec->state = CONTAINER_EXITED;
            rec->reason = TERMINATION_SIGNALLED;
        }
    }
}

static void *reaper_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;

    for (;;) {
        int status = 0;
        pid_t pid = waitpid(-1, &status, WNOHANG);

        if (pid > 0) {
            container_record_t *rec = NULL;
            pthread_t producer_to_join;
            int should_join = 0;
            int should_unregister = 0;
            char id_copy[CONTAINER_ID_LEN];
            pid_t pid_copy = pid;

            memset(id_copy, 0, sizeof(id_copy));
            pthread_mutex_lock(&ctx->metadata_lock);
            rec = find_container_by_pid_locked(ctx, pid);
            if (rec) {
                update_exit_metadata_locked(rec, status);
                if (rec->monitor_registered) {
                    rec->monitor_registered = 0;
                    should_unregister = 1;
                    strncpy(id_copy, rec->id, sizeof(id_copy) - 1);
                }
                if (rec->producer_started && !rec->producer_joined) {
                    rec->producer_joined = 1;
                    rec->producer_started = 0;
                    producer_to_join = rec->producer_thread;
                    should_join = 1;
                }
            }
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (should_unregister)
                (void)unregister_from_monitor(ctx->monitor_fd, id_copy, pid_copy);

            if (should_join)
                (void)pthread_join(producer_to_join, NULL);

            continue;
        }

        if (pid == 0) {
            if (ctx->shutting_down) {
                int running = 0;
                pthread_mutex_lock(&ctx->metadata_lock);
                running = running_count_locked(ctx);
                pthread_mutex_unlock(&ctx->metadata_lock);
                if (running == 0) {
                    int tmp = 0;
                    pid_t probe = waitpid(-1, &tmp, WNOHANG);
                    if (probe < 0 && errno == ECHILD)
                        break;
                }
            }
            usleep(100000);
            continue;
        }

        if (errno == EINTR)
            continue;

        if (errno == ECHILD) {
            if (ctx->shutting_down) {
                int running = 0;
                pthread_mutex_lock(&ctx->metadata_lock);
                running = running_count_locked(ctx);
                pthread_mutex_unlock(&ctx->metadata_lock);
                if (running == 0)
                    break;
            }
            usleep(100000);
            continue;
        }

        usleep(100000);
    }

    return NULL;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           container_record_t **out_rec,
                           char *err,
                           size_t err_size)
{
    char resolved_rootfs[PATH_MAX];
    int log_pipe[2] = {-1, -1};
    child_config_t *cfg = NULL;
    void *stack = NULL;
    pid_t pid = -1;
    container_record_t *rec = NULL;
    producer_arg_t *parg = NULL;
    int rc;
    int logfd;

    *out_rec = NULL;
    err[0] = '\0';

    if (!validate_container_id(req->container_id)) {
        snprintf(err, err_size, "Invalid container ID: %s\n", req->container_id);
        return -1;
    }

    if (req->command[0] == '\0') {
        snprintf(err, err_size, "Container command cannot be empty\n");
        return -1;
    }

    if (canonicalize_rootfs(req->rootfs, resolved_rootfs) != 0) {
        snprintf(err, err_size, "Invalid container rootfs: %s\n", req->rootfs);
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id_locked(ctx, req->container_id) != NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(err, err_size, "Container ID already exists: %s\n", req->container_id);
        return -1;
    }
    if (rootfs_in_use_locked(ctx, resolved_rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(err, err_size, "Rootfs already in use: %s\n", resolved_rootfs);
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(log_pipe) < 0) {
        snprintf(err, err_size, "pipe failed: %s\n", strerror(errno));
        return -1;
    }

    cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        snprintf(err, err_size, "calloc failed for child config\n");
        close(log_pipe[0]);
        close(log_pipe[1]);
        return -1;
    }

    strncpy(cfg->id, req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs, resolved_rootfs, sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command, sizeof(cfg->command) - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = log_pipe[1];

    stack = malloc(STACK_SIZE);
    if (!stack) {
        snprintf(err, err_size, "malloc failed for child stack\n");
        close(log_pipe[0]);
        close(log_pipe[1]);
        free(cfg);
        return -1;
    }

    pid = clone(child_fn,
                (char *)stack + STACK_SIZE,
                CLONE_NEWUTS | CLONE_NEWPID | CLONE_NEWNS | SIGCHLD,
                cfg);

    free(stack);
    free(cfg);

    if (pid < 0) {
        snprintf(err, err_size, "clone failed: %s\n", strerror(errno));
        close(log_pipe[0]);
        close(log_pipe[1]);
        return -1;
    }

    close(log_pipe[1]);
    log_pipe[1] = -1;

    rec = calloc(1, sizeof(*rec));
    if (!rec) {
        snprintf(err, err_size, "calloc failed for container record\n");
        kill(pid, SIGKILL);
        close(log_pipe[0]);
        return -1;
    }

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    strncpy(rec->rootfs, resolved_rootfs, sizeof(rec->rootfs) - 1);
    strncpy(rec->command, req->command, sizeof(rec->command) - 1);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->reason = TERMINATION_NONE;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->nice_value = req->nice_value;
    rec->log_read_fd = log_pipe[0];
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);

    logfd = open(rec->log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (logfd < 0) {
        snprintf(err, err_size, "Cannot create log file for %s\n", rec->id);
        kill(pid, SIGKILL);
        close(rec->log_read_fd);
        free(rec);
        return -1;
    }
    close(logfd);

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    parg = calloc(1, sizeof(*parg));
    if (!parg) {
        pthread_mutex_lock(&ctx->metadata_lock);
        rec->state = CONTAINER_START_FAILED;
        rec->reason = TERMINATION_START_FAILED;
        rec->exit_code = 1;
        rec->stop_requested = 1;
        rec->stop_force_kill = 1;
        rec->finished_at = time(NULL);
        pthread_mutex_unlock(&ctx->metadata_lock);
        kill(pid, SIGKILL);
        close(rec->log_read_fd);
        rec->log_read_fd = -1;
        snprintf(err, err_size, "Cannot allocate producer thread args\n");
        return -1;
    }

    parg->ctx = ctx;
    parg->read_fd = rec->log_read_fd;
    strncpy(parg->container_id, rec->id, sizeof(parg->container_id) - 1);

    rc = pthread_create(&rec->producer_thread, NULL, log_producer_thread, parg);
    if (rc != 0) {
        free(parg);
        pthread_mutex_lock(&ctx->metadata_lock);
        rec->state = CONTAINER_START_FAILED;
        rec->reason = TERMINATION_START_FAILED;
        rec->exit_code = 1;
        rec->stop_requested = 1;
        rec->stop_force_kill = 1;
        rec->finished_at = time(NULL);
        pthread_mutex_unlock(&ctx->metadata_lock);
        kill(pid, SIGKILL);
        close(rec->log_read_fd);
        rec->log_read_fd = -1;
        snprintf(err, err_size, "pthread_create failed for producer thread\n");
        return -1;
    }
    rec->producer_started = 1;

    if (register_with_monitor(ctx->monitor_fd,
                              rec->id,
                              rec->host_pid,
                              rec->soft_limit_bytes,
                              rec->hard_limit_bytes) == 0) {
        rec->monitor_registered = 1;
    } else {
        rec->monitor_registered = 0;
    }

    *out_rec = rec;
    return 0;
}
static int build_ps_payload(supervisor_ctx_t *ctx, char **payload_out)
{
    size_t cap = 2048;
    size_t len = 0;
    char *buf = malloc(cap);
    container_record_t *cur;

    if (!buf)
        return -1;

    buf[0] = '\0';

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    if (!cur) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        free(buf);
        *payload_out = dup_text("No containers tracked.\n");
        return (*payload_out == NULL) ? -1 : 0;
    }

    while (cur) {
        char started_buf[32] = "-";
        char finished_buf[32] = "-";
        struct tm tm_value;
        int needed;

        if (localtime_r(&cur->started_at, &tm_value))
            strftime(started_buf, sizeof(started_buf), "%Y-%m-%d %H:%M:%S", &tm_value);
        if (cur->finished_at != 0 && localtime_r(&cur->finished_at, &tm_value))
            strftime(finished_buf, sizeof(finished_buf), "%Y-%m-%d %H:%M:%S", &tm_value);

        needed = snprintf(NULL,
                          0,
                          "%s pid=%d state=%s reason=%s soft=%luMiB hard=%luMiB nice=%d exit_code=%d exit_signal=%d started=%s finished=%s rootfs=%s log=%s\n",
                          cur->id,
                          cur->host_pid,
                          state_to_string(cur->state),
                          reason_to_string(cur->reason),
                          cur->soft_limit_bytes >> 20,
                          cur->hard_limit_bytes >> 20,
                          cur->nice_value,
                          cur->exit_code,
                          cur->exit_signal,
                          started_buf,
                          finished_buf,
                          cur->rootfs,
                          cur->log_path);
        if (needed < 0) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            free(buf);
            return -1;
        }

        if (len + (size_t)needed + 1 > cap) {
            char *grown;
            while (len + (size_t)needed + 1 > cap)
                cap *= 2;
            grown = realloc(buf, cap);
            if (!grown) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                free(buf);
                return -1;
            }
            buf = grown;
        }

        snprintf(buf + len,
                 cap - len,
                 "%s pid=%d state=%s reason=%s soft=%luMiB hard=%luMiB nice=%d exit_code=%d exit_signal=%d started=%s finished=%s rootfs=%s log=%s\n",
                 cur->id,
                 cur->host_pid,
                 state_to_string(cur->state),
                 reason_to_string(cur->reason),
                 cur->soft_limit_bytes >> 20,
                 cur->hard_limit_bytes >> 20,
                 cur->nice_value,
                 cur->exit_code,
                 cur->exit_signal,
                 started_buf,
                 finished_buf,
                 cur->rootfs,
                 cur->log_path);
        len += (size_t)needed;
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    *payload_out = buf;
    return 0;
}

static int read_file_payload(const char *path, char **payload_out)
{
    int fd;
    size_t cap = 4096;
    size_t len = 0;
    char *buf;

    fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    buf = malloc(cap + 1);
    if (!buf) {
        close(fd);
        return -1;
    }

    for (;;) {
        char chunk[4096];
        ssize_t n = read(fd, chunk, sizeof(chunk));
        if (n > 0) {
            if (len + (size_t)n > cap) {
                char *grown;
                while (len + (size_t)n > cap)
                    cap *= 2;
                grown = realloc(buf, cap + 1);
                if (!grown) {
                    free(buf);
                    close(fd);
                    return -1;
                }
                buf = grown;
            }
            memcpy(buf + len, chunk, (size_t)n);
            len += (size_t)n;
            continue;
        }
        if (n == 0)
            break;
        if (errno == EINTR)
            continue;
        free(buf);
        close(fd);
        return -1;
    }

    close(fd);
    buf[len] = '\0';
    *payload_out = buf;
    return 0;
}

static int stop_container(supervisor_ctx_t *ctx,
                          const char *container_id,
                          int *forced_kill,
                          char *err,
                          size_t err_size)
{
    container_record_t *rec;
    pid_t pid;
    int i;

    *forced_kill = 0;
    err[0] = '\0';

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container_by_id_locked(ctx, container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(err, err_size, "No such container: %s\n", container_id);
        return -1;
    }

    if (is_terminal_state(rec->state)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return 0;
    }

    rec->stop_requested = 1;
    pid = rec->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(pid, SIGTERM) < 0 && errno != ESRCH) {
        snprintf(err, err_size, "Failed to send SIGTERM to %s: %s\n", container_id, strerror(errno));
        return -1;
    }

    for (i = 0; i < STOP_GRACE_SECONDS * 10; i++) {
        int done = 0;
        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_id_locked(ctx, container_id);
        if (!rec || is_terminal_state(rec->state))
            done = 1;
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (done)
            return 0;
        usleep(100000);
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container_by_id_locked(ctx, container_id);
    if (rec && is_active_state(rec->state)) {
        rec->stop_force_kill = 1;
        pid = rec->host_pid;
    } else {
        pid = -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pid > 0) {
        if (kill(pid, SIGKILL) < 0 && errno != ESRCH) {
            snprintf(err, err_size, "Failed to send SIGKILL to %s: %s\n", container_id, strerror(errno));
            return -1;
        }
        *forced_kill = 1;
    }

    return 0;
}

static void process_request(supervisor_ctx_t *ctx,
                            const control_request_t *req,
                            int *result,
                            int *status,
                            char **payload)
{
    *result = 1;
    *status = 0;
    *payload = NULL;

    switch (req->kind) {
    case CMD_START:
    case CMD_RUN: {
        container_record_t *rec = NULL;
        char err[256];
        int rc;

        rc = start_container(ctx, req, &rec, err, sizeof(err));
        if (rc != 0) {
            *result = 1;
            *payload = dup_text(err);
            if (!*payload)
                *payload = dup_text("Failed to start container\n");
            break;
        }

        if (req->kind == CMD_START)
            *payload = format_text("Started container %s pid=%d\n", rec->id, rec->host_pid);
        else
            *payload = format_text("Run started for container %s pid=%d\n", rec->id, rec->host_pid);

        if (!*payload)
            *payload = dup_text("Started container\n");
        *result = 0;
        *status = 0;
        break;
    }

    case CMD_WAIT: {
        container_record_t *rec;

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_id_locked(ctx, req->container_id);
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            *result = 1;
            *payload = format_text("No such container: %s\n", req->container_id);
            break;
        }

        if (!is_terminal_state(rec->state)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            *result = 2;
            *payload = NULL;
            break;
        }

        *status = rec->exit_code;
        *payload = format_text("%s state=%s reason=%s exit_code=%d exit_signal=%d\n",
                               rec->id,
                               state_to_string(rec->state),
                               reason_to_string(rec->reason),
                               rec->exit_code,
                               rec->exit_signal);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!*payload)
            *payload = dup_text("Container finished\n");
        *result = 0;
        break;
    }

    case CMD_PS: {
        if (build_ps_payload(ctx, payload) != 0) {
            *result = 1;
            *payload = dup_text("Failed to build ps output\n");
            break;
        }
        *result = 0;
        break;
    }

    case CMD_LOGS: {
        container_record_t *rec;
        char path[PATH_MAX];

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_id_locked(ctx, req->container_id);
        if (!rec) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            *result = 1;
            *payload = format_text("No such container: %s\n", req->container_id);
            break;
        }
        strncpy(path, rec->log_path, sizeof(path) - 1);
        path[sizeof(path) - 1] = '\0';
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (read_file_payload(path, payload) != 0) {
            *result = 1;
            *payload = format_text("Could not read log file for %s\n", req->container_id);
            break;
        }

        *result = 0;
        break;
    }

    case CMD_STOP: {
        char err[256];
        int forced_kill = 0;
        int rc;

        rc = stop_container(ctx, req->container_id, &forced_kill, err, sizeof(err));
        if (rc != 0) {
            *result = 1;
            *payload = dup_text(err);
            if (!*payload)
                *payload = dup_text("Stop failed\n");
            break;
        }

        if (forced_kill)
            *payload = format_text("Stop escalated to SIGKILL for %s\n", req->container_id);
        else
            *payload = format_text("Stop requested for %s\n", req->container_id);

        if (!*payload)
            *payload = dup_text("Stop requested\n");
        *result = 0;
        break;
    }

    default:
        *result = 1;
        *payload = dup_text("Unknown command\n");
        break;
    }
}
static void join_remaining_producers(supervisor_ctx_t *ctx)
{
    for (;;) {
        container_record_t *cur;
        pthread_t tid;
        int found = 0;

        pthread_mutex_lock(&ctx->metadata_lock);
        cur = ctx->containers;
        while (cur) {
            if (cur->producer_started && !cur->producer_joined) {
                cur->producer_joined = 1;
                cur->producer_started = 0;
                tid = cur->producer_thread;
                found = 1;
                break;
            }
            cur = cur->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!found)
            break;

        (void)pthread_join(tid, NULL);
    }
}

static void free_all_records(supervisor_ctx_t *ctx)
{
    container_record_t *cur;
    container_record_t *next;

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    ctx->containers = NULL;
    pthread_mutex_unlock(&ctx->metadata_lock);

    while (cur) {
        next = cur->next;
        free(cur);
        cur = next;
    }
}

static void request_stop_for_all(supervisor_ctx_t *ctx, int force)
{
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    while (cur) {
        if (is_active_state(cur->state)) {
            cur->stop_requested = 1;
            if (force)
                cur->stop_force_kill = 1;
            (void)kill(cur->host_pid, force ? SIGKILL : SIGTERM);
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static int run_supervisor(const char *base_rootfs)
{
    supervisor_ctx_t ctx;
    struct sigaction sa;
    struct stat st;
    int rc;

    if (!base_rootfs || stat(base_rootfs, &st) < 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Invalid base rootfs: %s\n", base_rootfs ? base_rootfs : "(null)");
        return 1;
    }

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        perror("open /dev/container_monitor");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger_thread");
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.reaper_thread, NULL, reaper_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create reaper_thread");
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) < 0 || sigaction(SIGTERM, &sa, NULL) < 0) {
        perror("sigaction");
        g_supervisor_stop = 1;
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        g_supervisor_stop = 1;
    }

    if (!g_supervisor_stop) {
        struct sockaddr_un addr;

        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        unlink(CONTROL_PATH);
        if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("bind");
            g_supervisor_stop = 1;
        } else if (listen(ctx.server_fd, CONTROL_BACKLOG) < 0) {
            perror("listen");
            g_supervisor_stop = 1;
        }
    }

    while (!g_supervisor_stop) {
        struct pollfd pfd;
        int poll_rc;
        int client_fd;
        control_request_t req;
        int result;
        int status;
        char *payload = NULL;

        memset(&pfd, 0, sizeof(pfd));
        pfd.fd = ctx.server_fd;
        pfd.events = POLLIN;

        poll_rc = poll(&pfd, 1, 500);
        if (poll_rc < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (poll_rc == 0)
            continue;
        if (!(pfd.revents & POLLIN))
            continue;

        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            continue;
        }

        if (read_all(client_fd, &req, sizeof(req)) < 0) {
            (void)send_response(client_fd, 1, 0, "Invalid request\n");
            close(client_fd);
            continue;
        }

        process_request(&ctx, &req, &result, &status, &payload);
        (void)send_response(client_fd, result, status, payload);
        free(payload);
        close(client_fd);
    }

    ctx.shutting_down = 1;
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);

    request_stop_for_all(&ctx, 0);

    {
        int i;
        for (i = 0; i < STOP_GRACE_SECONDS * 10; i++) {
            int running;
            pthread_mutex_lock(&ctx.metadata_lock);
            running = running_count_locked(&ctx);
            pthread_mutex_unlock(&ctx.metadata_lock);
            if (running == 0)
                break;
            usleep(100000);
        }
    }

    request_stop_for_all(&ctx, 1);

    pthread_join(ctx.reaper_thread, NULL);
    join_remaining_producers(&ctx);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    close(ctx.monitor_fd);
    free_all_records(&ctx);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}
static int send_stop_intent_for_run(const char *container_id)
{
    control_request_t req;
    client_response_t resp;
    int rc;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    rc = send_request_and_receive(&req, &resp);
    if (rc == 0) {
        free(resp.payload);
        return (resp.result == 0) ? 0 : -1;
    }

    return -1;
}

static int cmd_start_like(command_kind_t kind, int argc, char *argv[])
{
    control_request_t req;
    client_response_t resp;
    int rc;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s %s <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0],
                kind == CMD_RUN ? "run" : "start");
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = kind;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    rc = send_request_and_receive(&req, &resp);
    if (rc != 0) {
        perror("control request");
        return 1;
    }

    if (resp.payload && resp.payload[0] != '\0')
        fputs(resp.payload, stdout);

    if (kind == CMD_START) {
        int exit_rc = (resp.result == 0) ? 0 : 1;
        free(resp.payload);
        return exit_rc;
    }

    if (resp.result != 0) {
        free(resp.payload);
        return 1;
    }

    free(resp.payload);

    {
        struct sigaction sa;
        struct sigaction old_int;
        struct sigaction old_term;
        int stop_sent = 0;
        int run_exit = 1;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = run_signal_handler;
        sigemptyset(&sa.sa_mask);
        sigaction(SIGINT, &sa, &old_int);
        sigaction(SIGTERM, &sa, &old_term);

        g_run_signal = 0;

        for (;;) {
            control_request_t wait_req;
            client_response_t wait_resp;

            if (g_run_signal && !stop_sent) {
                (void)send_stop_intent_for_run(req.container_id);
                stop_sent = 1;
                g_run_signal = 0;
            }

            memset(&wait_req, 0, sizeof(wait_req));
            wait_req.kind = CMD_WAIT;
            strncpy(wait_req.container_id, req.container_id, sizeof(wait_req.container_id) - 1);

            rc = send_request_and_receive(&wait_req, &wait_resp);
            if (rc != 0) {
                perror("wait request");
                run_exit = 1;
                break;
            }

            if (wait_resp.result == 2) {
                free(wait_resp.payload);
                usleep(200000);
                continue;
            }

            if (wait_resp.payload && wait_resp.payload[0] != '\0')
                fputs(wait_resp.payload, stdout);

            if (wait_resp.result != 0)
                run_exit = 1;
            else
                run_exit = wait_resp.status;

            free(wait_resp.payload);
            break;
        }

        sigaction(SIGINT, &old_int, NULL);
        sigaction(SIGTERM, &old_term, NULL);
        return run_exit;
    }
}

static int cmd_ps(void)
{
    control_request_t req;
    client_response_t resp;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    if (send_request_and_receive(&req, &resp) != 0) {
        perror("control request");
        return 1;
    }

    if (resp.payload && resp.payload[0] != '\0')
        fputs(resp.payload, stdout);

    free(resp.payload);
    return (resp.result == 0) ? 0 : 1;
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    client_response_t resp;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    if (send_request_and_receive(&req, &resp) != 0) {
        perror("control request");
        return 1;
    }

    if (resp.payload && resp.payload[0] != '\0')
        fputs(resp.payload, stdout);

    free(resp.payload);
    return (resp.result == 0) ? 0 : 1;
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    client_response_t resp;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    if (send_request_and_receive(&req, &resp) != 0) {
        perror("control request");
        return 1;
    }

    if (resp.payload && resp.payload[0] != '\0')
        fputs(resp.payload, stdout);

    free(resp.payload);
    return (resp.result == 0) ? 0 : 1;
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start_like(CMD_START, argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_start_like(CMD_RUN, argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}

