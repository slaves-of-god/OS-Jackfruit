#define _GNU_SOURCE
#include <sched.h>
#include <sys/mount.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>

#define STACK_SIZE (1024 * 1024)
#define MAX_CONTAINERS 20

typedef enum {
    CONTAINER_RUNNING,
    CONTAINER_STOPPED
} state_t;

typedef struct container {
    char id[32];
    pid_t pid;
    state_t state;
    time_t start_time;
    struct container *next;
} container_t;

typedef struct {
    char id[32];
    char rootfs[256];
    char command[128];
    int nice_value;
} child_config_t;

container_t *head = NULL;

/* ================= CHILD FUNCTION ================= */

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    // hostname isolation
    sethostname(cfg->id, strlen(cfg->id));

    // filesystem isolation
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot failed");
        return 1;
    }

    chdir("/");

    // mount proc
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc failed");
        return 1;
    }

    // set nice
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    // execute command
    char *args[] = {cfg->command, NULL};
    execvp(args[0], args);

    perror("exec failed");
    return 1;
}

/* ================= SIGNAL HANDLER ================= */

void sigchld_handler(int sig)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_t *cur = head;
        while (cur) {
            if (cur->pid == pid) {
                cur->state = CONTAINER_STOPPED;
            }
            cur = cur->next;
        }
    }
}

/* ================= ADD CONTAINER ================= */

void add_container(char *id, pid_t pid)
{
    container_t *node = malloc(sizeof(container_t));
    strcpy(node->id, id);
    node->pid = pid;
    node->state = CONTAINER_RUNNING;
    node->start_time = time(NULL);
    node->next = head;
    head = node;
}

/* ================= PRINT CONTAINERS ================= */

void list_containers()
{
    container_t *cur = head;

    printf("\nID\tPID\tSTATE\n");
    while (cur) {
        printf("%s\t%d\t%s\n",
               cur->id,
               cur->pid,
               cur->state == CONTAINER_RUNNING ? "running" : "stopped");
        cur = cur->next;
    }
}

/* ================= STOP CONTAINER ================= */

void stop_container(char *id)
{
    container_t *cur = head;

    while (cur) {
        if (strcmp(cur->id, id) == 0) {
            kill(cur->pid, SIGTERM);
            cur->state = CONTAINER_STOPPED;
            printf("Stopped %s\n", id);
            return;
        }
        cur = cur->next;
    }

    printf("Container not found\n");
}

/* ================= SUPERVISOR ================= */

void run_supervisor()
{
    signal(SIGCHLD, sigchld_handler);

    while (1) {
        printf("\nCommands: start | ps | stop | exit\n> ");

        char cmd[32];
        scanf("%s", cmd);

        if (strcmp(cmd, "start") == 0) {

            char id[32], rootfs[256], command[128];

            printf("Enter: <id> <rootfs> <command>\n> ");
            scanf("%s %s %s", id, rootfs, command);

            char *stack = malloc(STACK_SIZE);

            child_config_t *cfg = malloc(sizeof(child_config_t));
            strcpy(cfg->id, id);
            strcpy(cfg->rootfs, rootfs);
            strcpy(cfg->command, command);
            cfg->nice_value = 0;

            pid_t pid = clone(child_fn,
                              stack + STACK_SIZE,
                              CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                              cfg);

            if (pid < 0) {
                perror("clone failed");
                continue;
            }

            add_container(id, pid);

            printf("Started %s with PID %d\n", id, pid);
        }

        else if (strcmp(cmd, "ps") == 0) {
            list_containers();
        }

        else if (strcmp(cmd, "stop") == 0) {
            char id[32];
            printf("Enter id: ");
            scanf("%s", id);
            stop_container(id);
        }

        else if (strcmp(cmd, "exit") == 0) {
            break;
        }

        else {
            printf("Unknown command\n");
        }
    }
}

/* ================= MAIN ================= */

int main(int argc, char *argv[])
{
    if (argc < 2) {
        printf("Usage: %s supervisor\n", argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        run_supervisor();
    } else {
        printf("Only supervisor mode supported for Task 1\n");
    }

    return 0;
}
