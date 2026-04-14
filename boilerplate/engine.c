/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"
#include <sys/resource.h>

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

static volatile sig_atomic_t g_supervisor_stop = 0;
static void supervisor_signal_handler(int signo);
typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int stdout_fd;
    int stderr_fd;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

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

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int stdout_write_fd;
    int stderr_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} log_producer_arg_t;

static container_record_t *find_container_by_id(supervisor_ctx_t *ctx, const char *id);
static container_record_t *find_container_by_pid(supervisor_ctx_t *ctx, pid_t pid);
static int rootfs_in_use(supervisor_ctx_t *ctx, const char *rootfs);
static void reap_children(supervisor_ctx_t *ctx);
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

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
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

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
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
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
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

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}
static void supervisor_signal_handler(int signo)
{
    (void)signo;
    g_supervisor_stop = 1;
}
static container_record_t *find_container_by_id(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (strncmp(cur->id, id, CONTAINER_ID_LEN) == 0)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static container_record_t *find_container_by_pid(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if (cur->host_pid == pid)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static int rootfs_in_use(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur = ctx->containers;
    while (cur) {
        if ((cur->state == CONTAINER_STARTING || cur->state == CONTAINER_RUNNING) &&
            strncmp(cur->rootfs, rootfs, PATH_MAX) == 0)
            return 1;
        cur = cur->next;
    }
    return 0;
}
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *rec;

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = find_container_by_pid(ctx, pid);

        if (rec) {
            if (WIFEXITED(status)) {
                rec->exit_code = WEXITSTATUS(status);
                rec->exit_signal = 0;

                if (rec->stop_requested)
                    rec->state = CONTAINER_STOPPED;
                else
                    rec->state = CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                rec->exit_code = 128 + WTERMSIG(status);
                rec->exit_signal = WTERMSIG(status);

                if (rec->stop_requested)
                    rec->state = CONTAINER_STOPPED;
                else if (WTERMSIG(status) == SIGKILL)
                    rec->state = CONTAINER_KILLED;
                else
                    rec->state = CONTAINER_EXITED;
            }
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
    }
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

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

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

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

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

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;

        fprintf(stderr, "consumer writing %zu bytes for %s\n", item.length, item.container_id);

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("open log file");
            continue;
        }

        if (write(fd, item.data, item.length) < 0) {
            perror("write log file");
        }

        close(fd);
    }

    return NULL;
}
void *log_producer_thread(void *arg)
{
    log_producer_arg_t *parg = (log_producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    while ((n = read(parg->read_fd, item.data, sizeof(item.data))) > 0) {
    fprintf(stderr, "producer read %zd bytes for %s\n", n, parg->container_id);
        memset(item.container_id, 0, sizeof(item.container_id));
        strncpy(item.container_id, parg->container_id, sizeof(item.container_id) - 1);
        item.length = (size_t)n;

        if (bounded_buffer_push(&parg->ctx->log_buffer, &item) != 0)
            break;
    }

    close(parg->read_fd);
    free(parg);
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    if (cfg->nice_value != 0) {
        if (setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0) {
            perror("setpriority");
        }
    }

    if (dup2(cfg->stdout_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        return 1;
    }

    if (dup2(cfg->stderr_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        return 1;
    }

    close(cfg->stdout_write_fd);
    close(cfg->stderr_write_fd);
    fprintf(stderr, "[%s] child starting command %s\n", cfg->id, cfg->command);

    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }

    mkdir("/proc", 0555);

    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
        return 1;
    }

    execl(cfg->command, cfg->command, (char *)NULL);

    perror("execl");
    return 1;
}
int register_with_monitor(int monitor_fd,
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

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

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

    (void)rootfs;

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
    {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGINT, &sa, NULL) < 0) {
        perror("sigaction SIGINT");
    }

    if (sigaction(SIGTERM, &sa, NULL) < 0) {
        perror("sigaction SIGTERM");
    }
}

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        bounded_buffer_begin_shutdown(&ctx.log_buffer);
        pthread_join(ctx.logger_thread, NULL);
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    {
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

        unlink(CONTROL_PATH);

        if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            perror("bind");
            close(ctx.server_fd);
            bounded_buffer_begin_shutdown(&ctx.log_buffer);
            pthread_join(ctx.logger_thread, NULL);
            close(ctx.monitor_fd);
            bounded_buffer_destroy(&ctx.log_buffer);
            pthread_mutex_destroy(&ctx.metadata_lock);
            return 1;
        }

        if (listen(ctx.server_fd, 8) < 0) {
            perror("listen");
            unlink(CONTROL_PATH);
            close(ctx.server_fd);
            bounded_buffer_begin_shutdown(&ctx.log_buffer);
            pthread_join(ctx.logger_thread, NULL);
            close(ctx.monitor_fd);
            bounded_buffer_destroy(&ctx.log_buffer);
            pthread_mutex_destroy(&ctx.metadata_lock);
            return 1;
        }
    }
    fprintf(stderr, "Supervisor initialized and listening on %s\n", CONTROL_PATH);

    while (!g_supervisor_stop) {
    reap_children(&ctx);
        int client_fd;
        control_request_t req;
        control_response_t resp;
        ssize_t n;

client_fd = accept(ctx.server_fd, NULL, NULL);
if (client_fd < 0) {
    if (errno == EINTR && g_supervisor_stop)
        break;
    if (errno == EINTR)
        continue;
    perror("accept");
    continue;
}
        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));

        n = read(client_fd, &req, sizeof(req));
        if (n != (ssize_t)sizeof(req)) {
            resp.status = 1;
            snprintf(resp.message, sizeof(resp.message), "Invalid request");
            write(client_fd, &resp, sizeof(resp));
            close(client_fd);
            continue;
        }

        switch (req.kind) {

                            case CMD_PS:
        {
            char buf[CONTROL_MESSAGE_LEN];
            container_record_t *cur;
            size_t used = 0;
            struct tm *tm_info;
            char timebuf[64];

            pthread_mutex_lock(&ctx.metadata_lock);

            cur = ctx.containers;
            if (!cur) {
                snprintf(resp.message, sizeof(resp.message),
                         "No containers tracked.");
            } else {
                resp.message[0] = '\0';

                while (cur) {
                    tm_info = localtime(&cur->started_at);
                    if (tm_info) {
                        strftime(timebuf, sizeof(timebuf),
                                 "%Y-%m-%d %H:%M:%S", tm_info);
                    } else {
                        snprintf(timebuf, sizeof(timebuf), "unknown");
                    }

                    int written = snprintf(
                        buf, sizeof(buf),
                        "%s pid=%d state=%s soft=%luMiB hard=%luMiB exit_code=%d exit_signal=%d started=%s log=%s\n",
                        cur->id,
                        cur->host_pid,
                        state_to_string(cur->state),
                        cur->soft_limit_bytes >> 20,
                        cur->hard_limit_bytes >> 20,
                        cur->exit_code,
                        cur->exit_signal,
                        timebuf,
                        cur->log_path
                    );

                    if (written < 0)
                        break;

                    if (used + (size_t)written >= sizeof(resp.message))
                        break;

                    strncat(resp.message, buf,
                            sizeof(resp.message) - strlen(resp.message) - 1);
                    used = strlen(resp.message);

                    cur = cur->next;
                }
            }

            pthread_mutex_unlock(&ctx.metadata_lock);
            resp.status = 0;
            break;
        }
        case CMD_START:
{
    int out_pipe[2];
    int err_pipe[2];
    child_config_t *cfg;
    container_record_t *rec;
    pthread_t prod_out, prod_err;
    log_producer_arg_t *out_arg, *err_arg;
    void *stack;
    pid_t pid;

    pthread_mutex_lock(&ctx.metadata_lock);

    if (find_container_by_id(&ctx, req.container_id) != NULL) {
        pthread_mutex_unlock(&ctx.metadata_lock);
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "Container ID already exists: %s", req.container_id);
                 
        break;
    }

    if (rootfs_in_use(&ctx, req.rootfs)) {
        pthread_mutex_unlock(&ctx.metadata_lock);
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "Rootfs already in use: %s", req.rootfs);
        break;
    }

    pthread_mutex_unlock(&ctx.metadata_lock);

    if (pipe(out_pipe) < 0 || pipe(err_pipe) < 0) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "pipe failed");
        break;
    }

    cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "calloc child config failed");
        close(out_pipe[0]); close(out_pipe[1]);
        close(err_pipe[0]); close(err_pipe[1]);
        break;
    }

    strncpy(cfg->id, req.container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs, req.rootfs, sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req.command, sizeof(cfg->command) - 1);
    cfg->nice_value = req.nice_value;
    cfg->stdout_write_fd = out_pipe[1];
    cfg->stderr_write_fd = err_pipe[1];

    stack = malloc(STACK_SIZE);
    if (!stack) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "malloc stack failed");
        close(out_pipe[0]); close(out_pipe[1]);
        close(err_pipe[0]); close(err_pipe[1]);
        free(cfg);
        break;
    }

    pid = clone(child_fn,
                (char *)stack + STACK_SIZE,
                CLONE_NEWUTS | CLONE_NEWPID | CLONE_NEWNS | SIGCHLD,
                cfg);

    if (pid < 0) {
        perror("clone");
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "clone failed");
        close(out_pipe[0]); close(out_pipe[1]);
        close(err_pipe[0]); close(err_pipe[1]);
        free(cfg);
        free(stack);
        break;
    }

    close(out_pipe[1]);
    close(err_pipe[1]);

    rec = calloc(1, sizeof(*rec));
    if (!rec) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "calloc container record failed");
        kill(pid, SIGKILL);
        close(out_pipe[0]);
        close(err_pipe[0]);
        free(stack);
        break;
    }

    strncpy(rec->id, req.container_id, sizeof(rec->id) - 1);
    strncpy(rec->rootfs, req.rootfs, sizeof(rec->rootfs) - 1);
    strncpy(rec->command, req.command, sizeof(rec->command) - 1);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req.soft_limit_bytes;
    rec->hard_limit_bytes = req.hard_limit_bytes;
    rec->nice_value = req.nice_value;
    rec->stdout_fd = out_pipe[0];
    rec->stderr_fd = err_pipe[0];
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);
    {
    int logfd = open(rec->log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (logfd >= 0)
        close(logfd);
}

    pthread_mutex_lock(&ctx.metadata_lock);
    rec->next = ctx.containers;
    ctx.containers = rec;
    pthread_mutex_unlock(&ctx.metadata_lock);

    register_with_monitor(ctx.monitor_fd, rec->id, rec->host_pid,
                          rec->soft_limit_bytes, rec->hard_limit_bytes);

    out_arg = calloc(1, sizeof(*out_arg));
    err_arg = calloc(1, sizeof(*err_arg));
    if (out_arg && err_arg) {
        out_arg->ctx = &ctx;
        out_arg->read_fd = out_pipe[0];
        strncpy(out_arg->container_id, rec->id, sizeof(out_arg->container_id) - 1);

        err_arg->ctx = &ctx;
        err_arg->read_fd = err_pipe[0];
        strncpy(err_arg->container_id, rec->id, sizeof(err_arg->container_id) - 1);

        pthread_create(&prod_out, NULL, log_producer_thread, out_arg);
        pthread_detach(prod_out);

        pthread_create(&prod_err, NULL, log_producer_thread, err_arg);
        pthread_detach(prod_err);
    }

    resp.status = 0;
    snprintf(resp.message, sizeof(resp.message),
             "Started container %s with PID %d", rec->id, rec->host_pid);
    break;
}
            break;
               case CMD_RUN:
        {
            int out_pipe[2];
            int err_pipe[2];
            child_config_t *cfg;
            container_record_t *rec;
            pthread_t prod_out, prod_err;
            log_producer_arg_t *out_arg, *err_arg;
            void *stack;
            pid_t pid;
            int status;

            pthread_mutex_lock(&ctx.metadata_lock);

            if (find_container_by_id(&ctx, req.container_id) != NULL) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "Container ID already exists: %s", req.container_id);
                         
                break;
            }

            if (rootfs_in_use(&ctx, req.rootfs)) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "Rootfs already in use: %s", req.rootfs);
                break;
            }

            pthread_mutex_unlock(&ctx.metadata_lock);

            if (pipe(out_pipe) < 0 || pipe(err_pipe) < 0) {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "pipe failed");
                break;
            }

            cfg = calloc(1, sizeof(*cfg));
            if (!cfg) {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "calloc child config failed");
                close(out_pipe[0]); close(out_pipe[1]);
                close(err_pipe[0]); close(err_pipe[1]);
                break;
            }

            strncpy(cfg->id, req.container_id, sizeof(cfg->id) - 1);
            strncpy(cfg->rootfs, req.rootfs, sizeof(cfg->rootfs) - 1);
            strncpy(cfg->command, req.command, sizeof(cfg->command) - 1);
            cfg->nice_value = req.nice_value;
            cfg->stdout_write_fd = out_pipe[1];
            cfg->stderr_write_fd = err_pipe[1];

            stack = malloc(STACK_SIZE);
            if (!stack) {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "malloc stack failed");
                close(out_pipe[0]); close(out_pipe[1]);
                close(err_pipe[0]); close(err_pipe[1]);
                free(cfg);
                break;
            }

            pid = clone(child_fn,
                        (char *)stack + STACK_SIZE,
                        CLONE_NEWUTS | CLONE_NEWPID | CLONE_NEWNS | SIGCHLD,
                        cfg);

            if (pid < 0) {
                perror("clone");
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "clone failed");
                close(out_pipe[0]); close(out_pipe[1]);
                close(err_pipe[0]); close(err_pipe[1]);
                free(cfg);
                free(stack);
                break;
            }

            close(out_pipe[1]);
            close(err_pipe[1]);

            rec = calloc(1, sizeof(*rec));
            if (!rec) {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "calloc container record failed");
                kill(pid, SIGKILL);
                close(out_pipe[0]);
                close(err_pipe[0]);
                free(stack);
                break;
            }

            strncpy(rec->id, req.container_id, sizeof(rec->id) - 1);
            strncpy(rec->rootfs, req.rootfs, sizeof(rec->rootfs) - 1);
            strncpy(rec->command, req.command, sizeof(rec->command) - 1);
            rec->host_pid = pid;
            rec->started_at = time(NULL);
            rec->state = CONTAINER_RUNNING;
            rec->soft_limit_bytes = req.soft_limit_bytes;
            rec->hard_limit_bytes = req.hard_limit_bytes;
            rec->nice_value = req.nice_value;
            rec->stdout_fd = out_pipe[0];
            rec->stderr_fd = err_pipe[0];
            snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);
            {
    int logfd = open(rec->log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (logfd >= 0)
        close(logfd);
}

            pthread_mutex_lock(&ctx.metadata_lock);
            rec->next = ctx.containers;
            ctx.containers = rec;
            pthread_mutex_unlock(&ctx.metadata_lock);

            register_with_monitor(ctx.monitor_fd, rec->id, rec->host_pid,
                                  rec->soft_limit_bytes, rec->hard_limit_bytes);

            out_arg = calloc(1, sizeof(*out_arg));
            err_arg = calloc(1, sizeof(*err_arg));
            if (out_arg && err_arg) {
                out_arg->ctx = &ctx;
                out_arg->read_fd = out_pipe[0];
                strncpy(out_arg->container_id, rec->id, sizeof(out_arg->container_id) - 1);

                err_arg->ctx = &ctx;
                err_arg->read_fd = err_pipe[0];
                strncpy(err_arg->container_id, rec->id, sizeof(err_arg->container_id) - 1);

                pthread_create(&prod_out, NULL, log_producer_thread, out_arg);
                pthread_detach(prod_out);

                pthread_create(&prod_err, NULL, log_producer_thread, err_arg);
                pthread_detach(prod_err);
            }

            if (waitpid(pid, &status, 0) < 0) {
                perror("waitpid");
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "RUN waitpid failed for %s", rec->id);
                break;
            }

            pthread_mutex_lock(&ctx.metadata_lock);
            if (WIFEXITED(status)) {
                rec->exit_code = WEXITSTATUS(status);
                rec->exit_signal = 0;
                rec->state = CONTAINER_EXITED;
                resp.status = rec->exit_code;
            } else if (WIFSIGNALED(status)) {
                rec->exit_code = 128 + WTERMSIG(status);
                rec->exit_signal = WTERMSIG(status);
                rec->state = CONTAINER_KILLED;
                resp.status = rec->exit_code;
            } else {
                resp.status = 1;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            snprintf(resp.message, sizeof(resp.message),
                     "Run finished for %s with status %d",
                     rec->id, resp.status);
            break;
        }
                case CMD_LOGS:
        {
            container_record_t *rec;
            int fd;
            ssize_t n;

            pthread_mutex_lock(&ctx.metadata_lock);
            rec = find_container_by_id(&ctx, req.container_id);
            if (!rec) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "No such container: %s", req.container_id);
                break;
            }

            fd = open(rec->log_path, O_RDONLY);
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (fd < 0) {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "Could not open log file for %s", req.container_id);
                break;
            }

            memset(resp.message, 0, sizeof(resp.message));
            n = read(fd, resp.message, sizeof(resp.message) - 1);
            close(fd);

            if (n < 0) {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "Failed to read log file for %s", req.container_id);
                break;
            }

            resp.status = 0;
            break;
        }
               case CMD_STOP:
        {
            container_record_t *rec;

            pthread_mutex_lock(&ctx.metadata_lock);
            rec = find_container_by_id(&ctx, req.container_id);
            if (!rec) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "No such container: %s", req.container_id);
                break;
            }

            rec->stop_requested = 1;
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (kill(rec->host_pid, SIGTERM) < 0) {
                perror("kill SIGTERM");
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "Failed to stop container %s", req.container_id);
                break;
            }

            pthread_mutex_lock(&ctx.metadata_lock);
            rec->state = CONTAINER_STOPPED;
            pthread_mutex_unlock(&ctx.metadata_lock);

            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Stopped container %s", req.container_id);
            break;
        }
        default:
            resp.status = 1;
            snprintf(resp.message, sizeof(resp.message),
                     "Unknown command");
            break;
        }

        write(client_fd, &resp, sizeof(resp));
        close(client_fd);
         reap_children(&ctx);
    }

    unlink(CONTROL_PATH);
    close(ctx.server_fd);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    close(ctx.monitor_fd);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
    
    
    fprintf(stderr, "Supervisor shutting down cleanly.\n");
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t n;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    n = write(fd, req, sizeof(*req));
    if (n != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(fd);
        return 1;
    }

    n = read(fd, &resp, sizeof(resp));
    if (n != (ssize_t)sizeof(resp)) {
        perror("read response");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0')
        printf("%s\n", resp.message);

    close(fd);
    return resp.status;
}
static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
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
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
