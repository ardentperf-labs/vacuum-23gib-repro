#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/aio_abi.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    char *path;
    int fd;
    uint64_t blocks;
    uint64_t size_bytes;
} data_file;

static volatile sig_atomic_t stopping = 0;
static uint64_t zero_length_segments_skipped = 0;
static uint64_t subpage_segments_skipped = 0;
static uint64_t subpage_bytes_skipped = 0;

static void stop_signal(int sig) {
    (void)sig;
    stopping = 1;
}

static uint64_t random_next(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static int is_segment(const char *base, const char *name) {
    size_t n = strlen(base);
    if (strncmp(base, name, n) != 0) return 0;
    if (name[n] == '\0') return 1;
    if (name[n] != '.') return 0;
    if (name[n + 1] == '\0') return 0;
    for (const char *p = name + n + 1; *p; p++)
        if (*p < '0' || *p > '9') return 0;
    return 1;
}

static int compare_path(const void *a, const void *b) {
    const data_file *fa = a, *fb = b;
    const char *na = strrchr(fa->path, '/');
    const char *nb = strrchr(fb->path, '/');
    na = na ? na + 1 : fa->path;
    nb = nb ? nb + 1 : fb->path;
    return strcmp(na, nb);
}

static void close_files(data_file *files, size_t count) {
    for (size_t i = 0; i < count; i++) {
        if (files[i].fd >= 0) close(files[i].fd);
        free(files[i].path);
    }
    free(files);
}

static data_file *open_relation_segments(const char *base_path, size_t *count_out,
                                        uint64_t *bytes_out) {
    char *copy = strdup(base_path);
    if (!copy) return NULL;
    char *slash = strrchr(copy, '/');
    if (!slash) {
        free(copy);
        errno = EINVAL;
        return NULL;
    }
    char *base = strdup(slash + 1);
    if (!base) {
        free(copy);
        return NULL;
    }
    *slash = '\0';
    DIR *dir = opendir(copy);
    if (!dir) {
        free(base);
        free(copy);
        return NULL;
    }
    data_file *files = NULL;
    size_t count = 0;
    uint64_t total_bytes = 0;
    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        if (!is_segment(base, ent->d_name)) continue;
        size_t need = strlen(copy) + strlen(ent->d_name) + 2;
        char *path = malloc(need);
        if (!path) goto fail;
        snprintf(path, need, "%s/%s", copy, ent->d_name);
        int fd = open(path, O_RDONLY | O_DIRECT | O_CLOEXEC);
        if (fd < 0) {
            free(path);
            goto fail;
        }
        struct stat st;
        if (fstat(fd, &st) != 0) {
            int saved = errno;
            close(fd);
            free(path);
            errno = saved;
            goto fail;
        }
        if (!S_ISREG(st.st_mode)) {
            close(fd);
            free(path);
            errno = EINVAL;
            goto fail;
        }
        if (st.st_size == 0) {
            zero_length_segments_skipped++;
            close(fd);
            free(path);
            continue;
        }
        if (st.st_size < 4096) {
            subpage_segments_skipped++;
            subpage_bytes_skipped += (uint64_t)st.st_size;
            total_bytes += (uint64_t)st.st_size;
            close(fd);
            free(path);
            continue;
        }
        data_file *grown = realloc(files, (count + 1) * sizeof(*files));
        if (!grown) {
            close(fd);
            free(path);
            goto fail;
        }
        files = grown;
        files[count++] = (data_file){.path = path, .fd = fd,
                                     .blocks = (uint64_t)st.st_size / 4096,
                                     .size_bytes = (uint64_t)st.st_size};
        total_bytes += (uint64_t)st.st_size;
    }
    closedir(dir);
    free(base);
    free(copy);
    if (!count) {
        free(files);
        errno = ENOENT;
        return NULL;
    }
    qsort(files, count, sizeof(*files), compare_path);
    *count_out = count;
    *bytes_out = total_bytes;
    return files;
fail: {
        int saved = errno;
        closedir(dir);
        free(base);
        free(copy);
        close_files(files, count);
        errno = saved;
        return NULL;
    }
}

typedef struct {
    struct iocb cb;
    void *buf;
    uint64_t submitted_ns;
    int active;
} io_slot;

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static double realtime_epoch(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) return 0;
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int select_offset(data_file *files, size_t file_count, size_t block_pages,
                         uint64_t *seed, size_t *file_index, off_t *offset) {
    uint64_t total = 0;
    for (size_t i = 0; i < file_count; i++) {
        if (files[i].blocks < block_pages) continue;
        total += files[i].blocks / block_pages;
    }
    if (!total) return -1;
    uint64_t pick = random_next(seed) % total;
    for (size_t i = 0; i < file_count; i++) {
        uint64_t slots = files[i].blocks / block_pages;
        if (pick < slots) {
            *file_index = i;
            *offset = (off_t)(pick * block_pages * 4096);
            return 0;
        }
        pick -= slots;
    }
    return -1;
}

static int submit_read(aio_context_t context, io_slot *slot, size_t slot_index,
                       data_file *files, size_t file_count, size_t block_size,
                       uint64_t *seed) {
    size_t file_index;
    off_t offset;
    if (select_offset(files, file_count, block_size / 4096, seed, &file_index, &offset) != 0)
        return -1;
    memset(&slot->cb, 0, sizeof(slot->cb));
    slot->cb.aio_data = (uint64_t)slot_index;
    slot->cb.aio_lio_opcode = IOCB_CMD_PREAD;
    slot->cb.aio_fildes = (uint32_t)files[file_index].fd;
    slot->cb.aio_buf = (uint64_t)(uintptr_t)slot->buf;
    slot->cb.aio_nbytes = (uint64_t)block_size;
    slot->cb.aio_offset = offset;
    slot->submitted_ns = monotonic_ns();
    struct iocb *list[1] = {&slot->cb};
    long rc;
    do {
        rc = syscall(SYS_io_submit, context, 1L, list);
    } while (rc < 0 && errno == EINTR && !stopping);
    if (rc != 1) return -1;
    slot->active = 1;
    return 0;
}

static int worker(data_file *files, size_t file_count, size_t block_size,
                  unsigned long depth, uint64_t seed) {
    struct sigaction sa = {.sa_handler = stop_signal};
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    if (!seed) seed = 1;
    aio_context_t context = 0;
    if (syscall(SYS_io_setup, (unsigned)depth, &context) != 0) {
        dprintf(STDERR_FILENO, "io_setup failed: %s\n", strerror(errno));
        return 2;
    }
    io_slot *slots = calloc(depth, sizeof(*slots));
    if (!slots) {
        syscall(SYS_io_destroy, context);
        return 2;
    }
    for (unsigned long i = 0; i < depth; i++) {
        int rc = posix_memalign(&slots[i].buf, 4096, block_size);
        if (rc != 0) {
            for (unsigned long j = 0; j < i; j++) free(slots[j].buf);
            free(slots);
            syscall(SYS_io_destroy, context);
            dprintf(STDERR_FILENO, "posix_memalign failed: %s\n", strerror(rc));
            return 2;
        }
    }
    dprintf(STDOUT_FILENO, "{\"event\":\"worker_ready\",\"pid\":%ld,\"ioDepth\":%lu}\n",
            (long)getpid(), depth);
    uint64_t ops = 0, bytes = 0, failures = 0, total_latency_ns = 0, max_latency_ns = 0;
    unsigned long active = 0;
    for (unsigned long i = 0; i < depth && !stopping; i++) {
        if (submit_read(context, &slots[i], i, files, file_count, block_size, &seed) == 0)
            active++;
        else {
            failures++;
            break;
        }
    }
    int failed = failures > 0;
    while ((!stopping && !failed) || active > 0) {
        if (!active) break;
        struct io_event events[64];
        struct timespec timeout = {.tv_sec = 1, .tv_nsec = 0};
        long n = syscall(SYS_io_getevents, context, 1L, (long)depth, events, &timeout);
        if (n < 0) {
            if (errno == EINTR) continue;
            failures++;
            failed = 1;
            continue;
        }
        for (long i = 0; i < n; i++) {
            size_t index = (size_t)events[i].data;
            if (index >= depth || !slots[index].active) {
                failures++;
                failed = 1;
                continue;
            }
            slots[index].active = 0;
            active--;
            uint64_t elapsed = monotonic_ns() - slots[index].submitted_ns;
            if (events[i].res == (int64_t)block_size) {
                ops++;
                bytes += (uint64_t)events[i].res;
                total_latency_ns += elapsed;
                if (elapsed > max_latency_ns) max_latency_ns = elapsed;
            } else {
                failures++;
                if (failures >= 10) failed = 1;
            }
            if (!stopping && !failed) {
                if (submit_read(context, &slots[index], index, files, file_count,
                                block_size, &seed) == 0)
                    active++;
                else {
                    failures++;
                    failed = 1;
                }
            }
        }
    }
    dprintf(STDOUT_FILENO,
            "{\"event\":\"worker_exit\",\"pid\":%ld,\"exitUnixEpoch\":%.6f,\"readOps\":%" PRIu64
            ",\"readBytes\":%" PRIu64 ",\"errors\":%" PRIu64
            ",\"meanReadLatencyMs\":%.6f,\"maxReadLatencyMs\":%.6f}\n",
            (long)getpid(), realtime_epoch(), ops, bytes, failures,
            ops ? (double)total_latency_ns / (double)ops / 1e6 : 0,
            (double)max_latency_ns / 1e6);
    for (unsigned long i = 0; i < depth; i++) free(slots[i].buf);
    free(slots);
    syscall(SYS_io_destroy, context);
    return failures >= 10 ? 2 : 0;
}

static int parse_positive(const char *s, unsigned long max, unsigned long *out) {
    char *end = NULL;
    errno = 0;
    unsigned long n = strtoul(s, &end, 10);
    if (errno || !end || *end || n == 0 || n > max) return -1;
    *out = n;
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr, "usage: %s RELATION_FILENODE_PATH PROCESSES BLOCK_BYTES IO_DEPTH\n", argv[0]);
        return 64;
    }
    unsigned long workers, block_size, depth;
    if (parse_positive(argv[2], 128, &workers) != 0 ||
        parse_positive(argv[3], 4 * 1024 * 1024, &block_size) != 0 ||
        parse_positive(argv[4], 64, &depth) != 0 ||
        block_size % 4096 != 0) {
        fprintf(stderr, "processes must be 1..128; block bytes must be 4096..4194304 and a multiple of 4096; io_depth 1..64\n");
        return 64;
    }
    size_t file_count = 0;
    uint64_t relation_bytes = 0;
    data_file *files = open_relation_segments(argv[1], &file_count, &relation_bytes);
    if (!files) {
        fprintf(stderr, "cannot open relation segments at %s: %s\n", argv[1], strerror(errno));
        return 66;
    }
    uint64_t eligible_bytes = 0;
    size_t eligible_segments = 0, excluded_segments = 0;
    for (size_t i = 0; i < file_count; i++) {
        uint64_t slots = files[i].blocks / (block_size / 4096);
        uint64_t eligible = slots * block_size;
        eligible_bytes += eligible;
        if (eligible) eligible_segments++;
        else excluded_segments++;
    }
    if (!eligible_bytes) {
        fprintf(stderr, "no relation segment contains a full aligned direct-read block\n");
        close_files(files, file_count);
        return 66;
    }
    struct sigaction sa = {.sa_handler = stop_signal};
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    pid_t *children = calloc(workers, sizeof(*children));
    if (!children) {
        perror("calloc");
        close_files(files, file_count);
        return 70;
    }
    unsigned long started = 0;
    for (; started < workers; started++) {
        pid_t pid = fork();
        if (pid < 0) break;
        if (pid == 0) {
            uint64_t seed = (uint64_t)time(NULL) ^ ((uint64_t)getpid() << 32) ^ started;
            int status = worker(files, file_count, block_size, depth, seed);
            _exit(status);
        }
        children[started] = pid;
    }
    dprintf(STDOUT_FILENO,
            "{\"event\":\"started\",\"pid\":%ld,\"requestedWorkers\":%lu,"
            "\"startedWorkers\":%lu,\"ioDepthPerProcess\":%lu,\"maxOutstandingReads\":%lu,"
            "\"blockBytes\":%lu,\"relationBytes\":%" PRIu64
            ",\"eligibleRelationBytes\":%" PRIu64 ",\"excludedRelationBytes\":%" PRIu64
            ",\"eligibleSegments\":%zu,\"excludedSegmentsForReadSize\":%zu"
            ",\"zeroLengthSegmentsSkipped\":%" PRIu64
            ",\"subPageSegmentsSkipped\":%" PRIu64
            ",\"subPageBytesSkipped\":%" PRIu64
            ",\"segments\":%zu,\"readOnly\":true,\"directIo\":true}\n",
            (long)getpid(), workers, started, depth, workers * depth, block_size, relation_bytes,
            eligible_bytes, relation_bytes - eligible_bytes, eligible_segments, excluded_segments,
            zero_length_segments_skipped, subpage_segments_skipped, subpage_bytes_skipped, file_count);
    if (started != workers) {
        stopping = 1;
        fprintf(stderr, "could not start all reader processes: %s\n", strerror(errno));
    }
    int exit_code = started == workers ? 0 : 1;
    while (1) {
        if (stopping)
            for (unsigned long i = 0; i < started; i++)
                if (children[i] > 0) kill(children[i], SIGTERM);
        int remaining = 0;
        for (unsigned long i = 0; i < started; i++) {
            if (children[i] <= 0) continue;
            int status;
            pid_t done = waitpid(children[i], &status, stopping ? 0 : WNOHANG);
            if (done == children[i]) {
                children[i] = 0;
                if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) exit_code = 1;
            } else if (done == 0) {
                remaining++;
            } else if (done < 0 && errno != EINTR) {
                children[i] = 0;
                exit_code = 1;
            }
        }
        if (!remaining) break;
        if (!stopping) usleep(100000);
    }
    dprintf(STDOUT_FILENO,
            "{\"event\":\"stopped\",\"pid\":%ld,\"startedWorkers\":%lu,"
            "\"exitCode\":%d}\n", (long)getpid(), started, exit_code);
    free(children);
    close_files(files, file_count);
    return exit_code;
}
