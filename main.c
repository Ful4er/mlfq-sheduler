#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <limits.h>

void* thread_function(void* arg);

#define BLOCK_PROB 5

#define MAX_THREADS 50
#define BASE_QUANTUM_MS 8
#define MAX_BLOCKS 3
#define AGING_THRESHOLD 500
#define ERROR_LOG_FILE "error.log"
#define EXEC_LOG_FILE "execution.log"
#define TEXT_LOG_FILE "text.log"
#define MAX_EVENT_TYPES 3
#define BUFFER_SIZE 256
#define MAX_LOG_MESSAGE 1024
#define MAX_QUEUES 4
#define FCFS_AGING_STEP_MS 5

long measure_clock_overhead() {
    struct timespec t1, t2;
    const int iterations = 100;
    long total = 0;
    for (int i = 0; i < iterations; i++) {
        clock_gettime(CLOCK_MONOTONIC_RAW, &t1);
        clock_gettime(CLOCK_MONOTONIC_RAW, &t2);
        long diff = (t2.tv_sec - t1.tv_sec) * 1000000L +
                    (t2.tv_nsec - t1.tv_nsec) / 1000L;
        total += diff;
    }
    return total / iterations / 1000;
}

typedef enum {
    DISK_IO,
    NETWORK_IO,
    KEYBOARD_INPUT,
    MOUSE_INPUT,
    TIMER_EVENT,
    NONE
} EventType;

typedef enum {
    RR,
    FCFS
} SchedulerType;

typedef struct {
    int thread_id;
    off_t file_position;
    int queue_level;
    int remaining_quantum_ms;
    int is_completed;
    EventType blocked_on;
    int block_count;
    int is_ready;
    int arrival_time;
    int waiting_time_ms;
    int start_time;
    SchedulerType scheduler_type;
    int total_wait_time_ms;
    int queue_entry_time;
    int just_arrived;
    EventType last_blocked_event;
    struct timespec execution_start;
    struct timespec execution_end;
    struct timespec queue_entry_timestamp;
    long total_exec_time_ms;
} ThreadState;

typedef struct QueueLevel {
    int level_id;
    int time_quantum_ms;
    SchedulerType scheduler_type;
    struct QueueLevel* next;
} QueueLevel;

typedef struct {
    EventType type;
    int duration_ms;
    char description[50];
} EventInfo;

QueueLevel* queue_head = NULL;
ThreadState thread_states[MAX_THREADS + 1];
EventInfo event_types[MAX_EVENT_TYPES];
int file_descriptor = -1;
off_t file_size;
struct timespec start_timestamp;
FILE* error_log = NULL;
FILE* exec_log = NULL;
FILE* text_log = NULL;
int output_to_console = -1;
int current_time = 0;
int total_threads = 0;
int globalQueueCounter = 0;

long avg_clock_overhead_ms = 0;

pthread_mutex_t output_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t text_log_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t global_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t thread_states_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

const char* event_type_names[] = {
    "DISK_IO", "NETWORK_IO", "KEYBOARD_INPUT", "NONE"
};

void log_message(const char* format, ...) {
    pthread_mutex_lock(&output_mutex);

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);
    long elapsed_ms = (now.tv_sec - start_timestamp.tv_sec) * 1000 +
                      (now.tv_nsec - start_timestamp.tv_nsec) / 1000000;
    int sec = elapsed_ms / 1000;
    int msec = elapsed_ms % 1000;

    va_list args;
    va_start(args, format);
    char buffer[MAX_LOG_MESSAGE];
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);

    char final_message[MAX_LOG_MESSAGE + 64];
    snprintf(final_message, sizeof(final_message), "[%d:%03d] %s\n", sec, msec, buffer);

    if (output_to_console)
        printf("%s", final_message);
    else if (exec_log) {
        fprintf(exec_log, "%s", final_message);
        fflush(exec_log);
    }

    pthread_mutex_unlock(&output_mutex);
}

void log_error(const char* format, ...) {
    pthread_mutex_lock(&output_mutex);
    va_list args;
    va_start(args, format);
    char buffer[MAX_LOG_MESSAGE];
    vsnprintf(buffer, sizeof(buffer), format, args);
    strcat(buffer, "\n");
    fprintf(stderr, "%s", buffer);
    if (error_log) {
        fprintf(error_log, "%s", buffer);
        fflush(error_log);
    }
    va_end(args);
    pthread_mutex_unlock(&output_mutex);
}

void log_text(const char* format, ...) {
    va_list args;
    va_start(args, format);
    char buffer[MAX_LOG_MESSAGE];
    vsnprintf(buffer, sizeof(buffer), format, args);

    pthread_mutex_lock(&text_log_mutex);

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);
    long elapsed_ms = (now.tv_sec - start_timestamp.tv_sec) * 1000 +
                      (now.tv_nsec - start_timestamp.tv_nsec) / 1000000;
    int sec = elapsed_ms / 1000;
    int msec = elapsed_ms % 1000;

    if (text_log) {
        fprintf(text_log, "[%d:%03d] %s\n", sec, msec, buffer);
        fflush(text_log);
    }

    pthread_mutex_unlock(&text_log_mutex);
    va_end(args);
}

void init_event_types() {
    event_types[DISK_IO] = (EventInfo){DISK_IO, 100, "Disk I/O Operation"};
    event_types[NETWORK_IO] = (EventInfo){NETWORK_IO, 150, "Network I/O Operation"};
    event_types[KEYBOARD_INPUT] = (EventInfo){KEYBOARD_INPUT, 50, "Keyboard Input"};
}

const char* get_event_name(EventType type) {
    return (type >= DISK_IO && type <= NONE) ? event_type_names[type] : event_type_names[NONE];
}

void init_logs() {
    if (output_to_console == -1) {
        fprintf(stderr, "Output destination not specified. Use -c or -e flag.\n");
        exit(EXIT_FAILURE);
    }
    error_log = fopen(ERROR_LOG_FILE, "a");
    if (!error_log) {
        perror("Failed to open error log");
        exit(EXIT_FAILURE);
    }
    text_log = fopen(TEXT_LOG_FILE, "a");
    if (!text_log) {
        fclose(error_log);
        perror("Failed to open text log");
        exit(EXIT_FAILURE);
    }
    if (!output_to_console) {
        exec_log = fopen(EXEC_LOG_FILE, "a");
        if (!exec_log) {
            fclose(error_log);
            fclose(text_log);
            perror("Failed to open execution log");
            exit(EXIT_FAILURE);
        }
    }
    char time_buf[100];
    time_t now = time(NULL);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", localtime(&now));
    pthread_mutex_lock(&output_mutex);
    if (output_to_console)
        fprintf(stdout, "\n=== Log started at %s ===\n\n", time_buf);
    else if (exec_log)
        fprintf(exec_log, "\n=== Log started at %s ===\n\n", time_buf);
    fprintf(text_log, "\n=== Text log started at %s ===\n\n", time_buf);
    pthread_mutex_unlock(&output_mutex);
}

void init_queues() {
    QueueLevel* levels[MAX_QUEUES];
    for (int i = 0; i < MAX_QUEUES; i++) {
        levels[i] = malloc(sizeof(QueueLevel));
        if (!levels[i]) {
            log_error("Failed to allocate queue level");
            exit(EXIT_FAILURE);
        }
        levels[i]->level_id = i;
        if (i < MAX_QUEUES - 1) {
            char scheduler_choice;
            pthread_mutex_lock(&output_mutex);
            fprintf(stdout, "Select scheduler for Queue %d (R for RR, F for FCFS): ", i);
            pthread_mutex_unlock(&output_mutex);
            if (scanf(" %c", &scheduler_choice) != 1) {
                log_error("Failed to read scheduler choice");
                exit(EXIT_FAILURE);
            }
            if (scheduler_choice == 'F' || scheduler_choice == 'f') {
                levels[i]->scheduler_type = FCFS;
                levels[i]->time_quantum_ms = 0;
            } else {
                levels[i]->scheduler_type = RR;
                int default_quantum = 0;
                switch(i) {
                    case 0: default_quantum = 8; break;
                    case 1: default_quantum = 16; break;
                    case 2: default_quantum = 32; break;
                    default: default_quantum = 8; break;
                }
                pthread_mutex_lock(&output_mutex);
                fprintf(stdout, "Enter time quantum (ms) for Queue %d (RR) [рекомендуется %d мс]: ", i, default_quantum);
                pthread_mutex_unlock(&output_mutex);
                if (scanf("%d", &levels[i]->time_quantum_ms) != 1) {
                    log_error("Failed to read time quantum");
                    exit(EXIT_FAILURE);
                }
            }
        } else {
            levels[i]->scheduler_type = FCFS;
            levels[i]->time_quantum_ms = 0;
        }
        levels[i]->next = NULL;
        if (i > 0)
            levels[i - 1]->next = levels[i];
    }
    queue_head = levels[0];
}

void display_queues() {
    char msg[1024];
    snprintf(msg, sizeof(msg), "\n=== MLFQ Status at Time %d ===", current_time);
    log_message("%s", msg);
    QueueLevel* current = queue_head;
    while (current) {
        char thread_entries[1024] = "";
        int ready_count = 0;
        int blocked_count = 0;
        ThreadState* queue_threads[MAX_THREADS];
        ThreadState* blocked_threads[MAX_THREADS];

        for (int i = 1; i <= total_threads; i++) {
            if (thread_states[i].thread_id &&
                !thread_states[i].is_completed &&
                thread_states[i].queue_level == current->level_id &&
                thread_states[i].start_time <= current_time)
            {
                if (thread_states[i].is_ready) {
                    queue_threads[ready_count++] = &thread_states[i];
                } else {
                    blocked_threads[blocked_count++] = &thread_states[i];
                }
            }
        }

        for (int i = 0; i < ready_count - 1; i++) {
            for (int j = i + 1; j < ready_count; j++) {
                if (queue_threads[i]->queue_entry_time > queue_threads[j]->queue_entry_time) {
                    ThreadState* temp = queue_threads[i];
                    queue_threads[i] = queue_threads[j];
                    queue_threads[j] = temp;
                }
            }
        }

        for (int i = 0; i < blocked_count - 1; i++) {
            for (int j = i + 1; j < blocked_count; j++) {
                if (blocked_threads[i]->queue_entry_time > blocked_threads[j]->queue_entry_time) {
                    ThreadState* temp = blocked_threads[i];
                    blocked_threads[i] = blocked_threads[j];
                    blocked_threads[j] = temp;
                }
            }
        }

        for (int i = 0; i < ready_count; i++) {
            char entry[50];
            snprintf(entry, sizeof(entry), "T%d(R) ", queue_threads[i]->thread_id);
            strncat(thread_entries, entry, sizeof(thread_entries) - strlen(thread_entries) - 1);
        }

        if (ready_count > 0 && blocked_count > 0) {
            strncat(thread_entries, "| ", sizeof(thread_entries) - strlen(thread_entries) - 1);
        }

        for (int i = 0; i < blocked_count; i++) {
            char entry[50];
            snprintf(entry, sizeof(entry), "T%d(B:%s) ",
                     blocked_threads[i]->thread_id,
                     get_event_name(blocked_threads[i]->blocked_on));
            strncat(thread_entries, entry, sizeof(thread_entries) - strlen(thread_entries) - 1);
        }

        if (ready_count == 0 && blocked_count == 0) {
            strncpy(thread_entries, "-", sizeof(thread_entries) - 1);
            thread_entries[sizeof(thread_entries)-1] = '\0';
        }

        int max_msg_len = sizeof(msg) - 50;
        if (max_msg_len < 0)
            max_msg_len = 0;
        if (current->scheduler_type == RR)
            snprintf(msg, sizeof(msg), "Queue %d (RR, Q=%dms): %.*s",
                     current->level_id, current->time_quantum_ms, max_msg_len, thread_entries);
        else
            snprintf(msg, sizeof(msg), "Queue %d (FCFS): %.*s",
                     current->level_id, max_msg_len, thread_entries);
        log_message("%s", msg);
        current = current->next;
    }
    log_message("=============================");
}

void handle_blocked_threads() {
    int any_unblocked = 0;
    for (int i = 1; i <= total_threads; i++) {
        ThreadState* state = &thread_states[i];
        if (!state->is_completed && !state->is_ready) {
            if (state->blocked_on < DISK_IO || state->blocked_on >= NONE) {
                pthread_mutex_lock(&global_counter_mutex);
                state->queue_entry_time = ++globalQueueCounter;
                pthread_mutex_unlock(&global_counter_mutex);
                clock_gettime(CLOCK_MONOTONIC_RAW, &state->queue_entry_timestamp);
                state->blocked_on = NONE;
                state->is_ready = 1;
                continue;
            }
            int completion_prob = 30;
            switch(state->blocked_on) {
                case DISK_IO:         completion_prob = 30; break;
                case NETWORK_IO:      completion_prob = 20; break;
                case KEYBOARD_INPUT:  completion_prob = 40; break;
                default:              completion_prob = 0; break;
            }
            if (rand() % 100 < completion_prob) {
                EventType prev_event = state->blocked_on;
                state->is_ready = 1;
                any_unblocked = 1;
                int old_queue = state->queue_level;
                int new_queue = old_queue;
                switch(prev_event) {
                    case KEYBOARD_INPUT:
                        new_queue = 0;
                        break;
                    case DISK_IO:
                        if (old_queue >= 2)
                            new_queue = 1;
                        break;
                    default:
                        if (old_queue == 3)
                            new_queue = 2;
                        break;
                }
                pthread_mutex_lock(&global_counter_mutex);
                state->queue_entry_time = ++globalQueueCounter;
                pthread_mutex_unlock(&global_counter_mutex);
                clock_gettime(CLOCK_MONOTONIC_RAW, &state->queue_entry_timestamp);
                if (new_queue != old_queue) {
                    state->queue_level = new_queue;
                    QueueLevel* new_q = queue_head;
                    while(new_q && new_q->level_id != new_queue)
                        new_q = new_q->next;
                    if(new_q){
                        if(new_q->scheduler_type == RR)
                            state->remaining_quantum_ms = new_q->time_quantum_ms;
                        else
                            state->remaining_quantum_ms = 0;
                    }
                    log_message("T%d: Unblocked and moved to Q%d", i, state->queue_level);
                } else {
                    log_message("T%d: Unblocked", i);
                }
                state->blocked_on = NONE;
            }
        }
    }
    if (any_unblocked)
        log_message("%d threads were unblocked", any_unblocked);
}

void promote_aging_threads() {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);

    for (int i = 1; i <= total_threads; i++) {
        ThreadState* state = &thread_states[i];
        if (!state->is_completed && state->is_ready) {
            long wait_time = (now.tv_sec - state->queue_entry_timestamp.tv_sec) * 1000 +
                             (now.tv_nsec - state->queue_entry_timestamp.tv_nsec) / 1000000;
            if (wait_time >= AGING_THRESHOLD && state->queue_level > 0) {
                state->queue_level--;
                pthread_mutex_lock(&global_counter_mutex);
                state->queue_entry_time = ++globalQueueCounter;
                pthread_mutex_unlock(&global_counter_mutex);
                clock_gettime(CLOCK_MONOTONIC_RAW, &state->queue_entry_timestamp);
                QueueLevel* new_q = queue_head;
                while (new_q && new_q->level_id != state->queue_level)
                    new_q = new_q->next;
                if (new_q) {
                    if (new_q->scheduler_type == RR)
                        state->remaining_quantum_ms = new_q->time_quantum_ms;
                    else
                        state->remaining_quantum_ms = 0;
                }
                log_message("T%d: Promoted to Q%d due to aging", i, state->queue_level);
            }
        }
    }
}

int find_highest_priority_thread(int num_threads) {
    int selected_thread = -1;
    int earliest_arrival = INT_MAX;
    int highest_priority = INT_MAX;
    for (int i = 1; i <= num_threads; i++) {
        ThreadState* state = &thread_states[i];
        if (!state->is_completed && state->is_ready && state->start_time <= current_time) {
            if (state->queue_level < highest_priority)
                highest_priority = state->queue_level;
        }
    }
    for (int i = 1; i <= num_threads; i++) {
        ThreadState* state = &thread_states[i];
        if (!state->is_completed && state->is_ready &&
            state->start_time <= current_time &&
            state->queue_level == highest_priority)
        {
            if (state->queue_entry_time < earliest_arrival) {
                earliest_arrival = state->queue_entry_time;
                selected_thread = i;
            }
        }
    }
    return selected_thread;
}

void schedule_threads(int num_threads) {
    int all_completed = 0;
    while (!all_completed) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC_RAW, &now);
        current_time = (now.tv_sec - start_timestamp.tv_sec) * 1000 +
                       (now.tv_nsec - start_timestamp.tv_nsec) / 1000000;

        all_completed = 1;
        int any_active = 0;
        for (int i = 1; i <= num_threads; i++) {
            if (!thread_states[i].is_completed) {
                all_completed = 0;
                if (thread_states[i].start_time <= current_time)
                    any_active = 1;
            }
        }
        if (all_completed)
            break;
        if (!any_active) {
            struct timespec delay = {0, 10 * 1000000};
            nanosleep(&delay, NULL);
            continue;
        }
        display_queues();
        handle_blocked_threads();
        promote_aging_threads();

        int next_thread = find_highest_priority_thread(num_threads);
        if (next_thread == -1) {
            struct timespec delay = {0, 10 * 1000000};
            nanosleep(&delay, NULL);
            continue;
        }
        pthread_mutex_lock(&thread_states_mutex);
        ThreadState* state = &thread_states[next_thread];
        pthread_mutex_unlock(&thread_states_mutex);

        {
            struct timespec now_ts;
            clock_gettime(CLOCK_MONOTONIC_RAW, &now_ts);
            long waiting_ms = (now_ts.tv_sec - state->queue_entry_timestamp.tv_sec) * 1000 +
                              (now_ts.tv_nsec - state->queue_entry_timestamp.tv_nsec) / 1000000;
            state->total_wait_time_ms += waiting_ms;
        }

        QueueLevel* q = queue_head;
        while (q && q->level_id != state->queue_level)
            q = q->next;
        if (!q) {
            log_error("Invalid queue level for T%d", next_thread);
            continue;
        }
        log_message("Scheduling T%d from Q%d (%s)", next_thread, state->queue_level,
                    q->scheduler_type == RR ? "RR" : "FCFS");
        int *tid = malloc(sizeof(int));
        if (!tid) {
            log_error("Failed to allocate memory for thread id");
            continue;
        }
        *tid = next_thread;
        pthread_t thread;
        if (pthread_create(&thread, NULL, thread_function, tid) != 0) {
            log_error("T%d: Creation failed (errno: %d, %s)", next_thread, errno, strerror(errno));
            free(tid);
            continue;
        }
        pthread_join(thread, NULL);
        if (state->is_completed) {
            log_message("T%d: Finished execution", next_thread);
        } else {
            q = queue_head;
            while (q && q->level_id != state->queue_level)
                q = q->next;
            if (q && q->scheduler_type == RR && state->remaining_quantum_ms <= 0) {
                int new_level = state->queue_level + 1;
                if (new_level < MAX_QUEUES) {
                    state->queue_level = new_level;
                    pthread_mutex_lock(&global_counter_mutex);
                    state->queue_entry_time = ++globalQueueCounter;
                    pthread_mutex_unlock(&global_counter_mutex);
                    clock_gettime(CLOCK_MONOTONIC_RAW, &state->queue_entry_timestamp);
                    QueueLevel* new_q = queue_head;
                    while (new_q && new_q->level_id != new_level)
                        new_q = new_q->next;
                    if (new_q) {
                        if (new_q->scheduler_type == RR)
                            state->remaining_quantum_ms = new_q->time_quantum_ms;
                        else
                            state->remaining_quantum_ms = 0;
                    }
                    log_message("T%d: Demoted to Q%d", next_thread, new_level);
                }
            }
        }
    }
}

void parse_args(int argc, char* argv[]) {
    int opt;
    while ((opt = getopt(argc, argv, "ce")) != -1) {
        switch (opt) {
            case 'c':
                output_to_console = 1;
                break;
            case 'e':
                output_to_console = 0;
                break;
            default:
                fprintf(stderr, "Usage: %s -c|-e <threads> <filename>\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
    if (output_to_console == -1) {
        fprintf(stderr, "You must specify either -c (console) or -e (error log)\n");
        exit(EXIT_FAILURE);
    }
}

void print_statistics(int num_threads) {
    log_message("\n=== Final Statistics ===");
    log_message("Thread | Queue | Scheduler | Wait Time (ms) | Blocks | Exec Time (ms)");
    log_message("---------------------------------------------------------------");
    for (int i = 1; i <= num_threads; i++) {
        ThreadState* state = &thread_states[i];
        QueueLevel* q = queue_head;
        while (q && q->level_id != state->queue_level)
            q = q->next;
        char statLine[256];
        snprintf(statLine, sizeof(statLine), "T%-5d | Q%-5d | %-9s | %-14d | %-6d | %ld",
                 state->thread_id,
                 state->queue_level,
                 q ? (q->scheduler_type == RR ? "RR" : "FCFS") : "UNKNOWN",
                 state->total_wait_time_ms,
                 state->block_count,
                 state->total_exec_time_ms);
        log_message("%s", statLine);
    }
}

void cleanup() {
    if (file_descriptor != -1)
        close(file_descriptor);
    if (error_log)
        fclose(error_log);
    if (exec_log)
        fclose(exec_log);
    if (text_log)
        fclose(text_log);
    QueueLevel* current = queue_head;
    while (current) {
        QueueLevel* next = current->next;
        free(current);
        current = next;
    }
    pthread_mutex_destroy(&output_mutex);
    pthread_mutex_destroy(&text_log_mutex);
    pthread_mutex_destroy(&global_counter_mutex);
    pthread_mutex_destroy(&thread_states_mutex);
    pthread_mutex_destroy(&file_mutex);
}

void assign_queues_to_threads(int num_threads) {
    char choice[4];
    pthread_mutex_lock(&output_mutex);
    printf("Assign queues:\n"
           "1 - Manual (select for each thread)\n"
           "2 - Chaos-mode (full random)\n"
           "Your choice: ");
    pthread_mutex_unlock(&output_mutex);
    if (scanf("%3s", choice) != 1) {
        log_error("Failed to read queue assignment choice");
        exit(EXIT_FAILURE);
    }
    if (strcmp(choice, "1") == 0) {
        for (int i = 1; i <= num_threads; i++) {
            int queue;
            while (1) {
                pthread_mutex_lock(&output_mutex);
                printf("Enter queue level (0-%d) for thread %d: ", MAX_QUEUES - 1, i);
                pthread_mutex_unlock(&output_mutex);
                if (scanf("%d", &queue) != 1) {
                    log_error("Failed to read queue level");
                    exit(EXIT_FAILURE);
                }
                if (queue >= 0 && queue < MAX_QUEUES) {
                    thread_states[i].queue_level = queue;
                    pthread_mutex_lock(&global_counter_mutex);
                    thread_states[i].queue_entry_time = ++globalQueueCounter;
                    pthread_mutex_unlock(&global_counter_mutex);
                    clock_gettime(CLOCK_MONOTONIC_RAW, &thread_states[i].queue_entry_timestamp);
                    QueueLevel* q = queue_head;
                    while (q && q->level_id != queue)
                        q = q->next;
                    if (q) {
                        if (q->scheduler_type == RR)
                            thread_states[i].remaining_quantum_ms = q->time_quantum_ms;
                        else
                            thread_states[i].remaining_quantum_ms = 0;
                    }
                    break;
                } else {
                    pthread_mutex_lock(&output_mutex);
                    printf("Invalid queue level. Please enter 0-%d.\n", MAX_QUEUES - 1);
                    pthread_mutex_unlock(&output_mutex);
                }
            }
        }
    } else {
        pthread_mutex_lock(&output_mutex);
        printf("Using chaos mode - completely random queue assignment\n");
        pthread_mutex_unlock(&output_mutex);
        for (int i = 1; i <= num_threads; i++) {
            thread_states[i].queue_level = rand() % (MAX_QUEUES - 1);
            pthread_mutex_lock(&global_counter_mutex);
            thread_states[i].queue_entry_time = ++globalQueueCounter;
            pthread_mutex_unlock(&global_counter_mutex);
            clock_gettime(CLOCK_MONOTONIC_RAW, &thread_states[i].queue_entry_timestamp);
            QueueLevel* q = queue_head;
            while (q && q->level_id != thread_states[i].queue_level)
                q = q->next;
            if (q) {
                if (q->scheduler_type == RR)
                    thread_states[i].remaining_quantum_ms = q->time_quantum_ms;
                else
                    thread_states[i].remaining_quantum_ms = 0;
            }
        }
    }
}

int my_read(int fd, void* buffer, size_t count, long* remaining_quantum, SchedulerType scheduler_type, int* quantum_exhausted) {
    if (scheduler_type != RR) {
        return read(fd, buffer, count);
    }

    if (*remaining_quantum <= 0) {
        *quantum_exhausted = 1;
        return 0;
    }

    size_t adjusted_count = (count > 512) ? 512 : count;

    struct timespec read_start, read_end;
    clock_gettime(CLOCK_MONOTONIC_RAW, &read_start);

    int bytes_read = read(fd, buffer, adjusted_count);

    clock_gettime(CLOCK_MONOTONIC_RAW, &read_end);

    long elapsed_ns = (read_end.tv_sec - read_start.tv_sec) * 1000000000L +
                      (read_end.tv_nsec - read_start.tv_nsec);
    long elapsed_ms = elapsed_ns / 1000000;

    *remaining_quantum -= elapsed_ms;

    if (*remaining_quantum <= 0) {
        *quantum_exhausted = 1;
    }

    return bytes_read;
}

void* thread_function(void* arg) {
    int thread_id = *((int*)arg);
    free(arg);
    ThreadState* state = &thread_states[thread_id];
    clock_gettime(CLOCK_MONOTONIC_RAW, &state->execution_start);

    if (state->is_completed)
        goto finish;

    char buffer[BUFFER_SIZE];
    int read_count = 0;
    long total_bytes_read = 0;
    off_t start_position = state->file_position;

    QueueLevel* q = queue_head;
    while (q && q->level_id != state->queue_level) q = q->next;

    long remaining_quantum = 0;
    if (q && q->scheduler_type == RR) {
        remaining_quantum = q->time_quantum_ms;
        state->remaining_quantum_ms = remaining_quantum;
    }

    struct timespec quantum_start;
    clock_gettime(CLOCK_MONOTONIC_RAW, &quantum_start);

    while (!state->is_completed) {
        struct timespec iter_start, iter_end;
        clock_gettime(CLOCK_MONOTONIC_RAW, &iter_start);

        if (!state->is_ready) {
            log_message("T%d: Not ready, skipping execution", thread_id);
            break;
        }

        if (q && q->scheduler_type == RR) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC_RAW, &now);
            long elapsed_ms = (now.tv_sec - quantum_start.tv_sec) * 1000 +
                              (now.tv_nsec - quantum_start.tv_nsec) / 1000000;
            if (elapsed_ms >= q->time_quantum_ms) {
                long total_used = elapsed_ms;
                log_message("T%d: Quantum pre-check exhausted (%ld ms used) in Q%d", thread_id, total_used, state->queue_level);
                state->remaining_quantum_ms = 0;
                break;
            }
            remaining_quantum = q->time_quantum_ms - elapsed_ms;
            state->remaining_quantum_ms = remaining_quantum;
        }

        pthread_mutex_lock(&file_mutex);
        off_t current_position = state->file_position;

        if (current_position >= file_size) {
            log_message("T%d: EOF reached (pos %ld/%ld)", thread_id, current_position, file_size);
            log_text("Thread T%d COMPLETED READING ENTIRE FILE", thread_id);
            log_text("  Start position: %ld", start_position);
            log_text("  Total reads: %d", read_count);
            log_text("  Total bytes read: %ld", current_position - start_position);
            log_text("  Final position: %ld/%ld (100%%)", file_size, file_size);
            state->is_completed = 1;
            pthread_mutex_unlock(&file_mutex);
            break;
        }

        if (state->queue_level < MAX_QUEUES - 1 &&
            state->block_count < MAX_BLOCKS &&
            (rand() % 100) < (BLOCK_PROB))
        {
            EventType event = (EventType)(rand() % MAX_EVENT_TYPES);
            log_message("T%d: Randomly blocking on %s", thread_id, get_event_name(event));
            log_text("Thread T%d QUANTUM BLOCK:", thread_id);
            log_text("  Start position in quantum: %ld", start_position);
            log_text("  Current position: %ld", state->file_position);
            log_text("  Reads in quantum: %d", read_count);
            log_text("  Bytes read in quantum: %ld", total_bytes_read);
            log_text("----------------------------------------\n\n");
            state->block_count++;
            state->last_blocked_event = event;
            state->blocked_on = event;
            state->is_ready = 0;
            state->just_arrived = 0;
            pthread_mutex_unlock(&file_mutex);
            break;
        }

        off_t seek_result = lseek(file_descriptor, current_position, SEEK_SET);
        if (seek_result == (off_t)-1) {
            log_error("T%d: Failed to seek in file: %s", thread_id, strerror(errno));
            state->is_completed = 1;
            pthread_mutex_unlock(&file_mutex);
            break;
        }

        int quantum_exhausted = 0;
        int bytes_read = my_read(file_descriptor, buffer, BUFFER_SIZE - 1, &remaining_quantum, q->scheduler_type, &quantum_exhausted);

        if (quantum_exhausted && q->scheduler_type == RR) {
            long total_used = q->time_quantum_ms;
            log_message("T%d: Quantum exhausted (%ld ms used) in Q%d", thread_id, total_used, state->queue_level);
            log_text("Thread T%d QUANTUM END:", thread_id);
            log_text("  Start position in quantum: %ld", start_position);
            log_text("  Current position: %ld", state->file_position);
            log_text("  Reads in quantum: %d", read_count);
            log_text("  Bytes read in quantum: %ld", total_bytes_read);
            log_text("----------------------------------------\n\n");
            state->remaining_quantum_ms = 0;
            pthread_mutex_unlock(&file_mutex);
            break;
        }

        if (bytes_read < 0) {
            log_error("T%d: Read error: %s", thread_id, strerror(errno));
            state->is_completed = 1;
            pthread_mutex_unlock(&file_mutex);
            break;
        } else if (bytes_read == 0) {
            log_message("T%d: Reached end of file, completing execution", thread_id);
            log_text("Thread T%d COMPLETED READING ENTIRE FILE", thread_id);
            log_text("  Start position: %ld", start_position);
            log_text("  Total reads: %d", read_count);
            log_text("  Total bytes read: %ld", current_position - start_position);
            log_text("  Final position: %ld/%ld (100%%)", file_size, file_size);
            state->is_completed = 1;
            pthread_mutex_unlock(&file_mutex);
            break;
        }
        buffer[bytes_read] = '\0';
        read_count++;
        total_bytes_read += bytes_read;

        log_text("Thread T%d READ OPERATION #%d:", thread_id, read_count);
        log_text("  Queue level: %d", state->queue_level);
        log_text("  Position before read: %ld/%ld (%.1f%%)",
                current_position, file_size,
                ((double)current_position/file_size)*100);
        log_text("  Bytes read: %d", bytes_read);
        log_text("  Position after read: %ld/%ld (%.1f%%)",
                current_position + bytes_read, file_size,
                ((double)(current_position + bytes_read)/file_size)*100);
        log_text("  Total bytes read in quantum: %ld", total_bytes_read);
        log_text("  Content:");
        log_text("----------------------------------------");
        log_text("%s", buffer);
        log_text("----------------------------------------");

        state->file_position = current_position + bytes_read;
        pthread_mutex_unlock(&file_mutex);

        int delay_ms = 1;
        struct timespec delay = {
            .tv_sec = 0,
            .tv_nsec = delay_ms * 1000000
        };
        nanosleep(&delay, NULL);

        clock_gettime(CLOCK_MONOTONIC_RAW, &iter_end);
        if(q->scheduler_type == RR) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC_RAW, &now);
            long elapsed_ms = (now.tv_sec - quantum_start.tv_sec) * 1000 +
                              (now.tv_nsec - quantum_start.tv_nsec) / 1000000;
            remaining_quantum = q->time_quantum_ms - elapsed_ms;
            state->remaining_quantum_ms = remaining_quantum;
        }
    }

finish:
    clock_gettime(CLOCK_MONOTONIC_RAW, &state->execution_end);
    long run_exec_time = 0;
    if (state->blocked_on != NONE) {
        run_exec_time = 0;
    } else {
        if (q && q->scheduler_type == RR) {
            run_exec_time = (state->execution_end.tv_sec - quantum_start.tv_sec) * 1000 +
                           (state->execution_end.tv_nsec - quantum_start.tv_nsec) / 1000000;
            log_message("T%d: Execution time for this run: %ld ms (RR quantum)", thread_id, run_exec_time);
        } else {
            run_exec_time = (state->execution_end.tv_sec - state->execution_start.tv_sec) * 1000 +
                           (state->execution_end.tv_nsec - state->execution_start.tv_nsec) / 1000000;
            log_message("T%d: Execution time for this run: %ld ms", thread_id, run_exec_time);
        }
    }
    state->total_exec_time_ms += run_exec_time;
    return NULL;
}

int main(int argc, char* argv[]) {
    srand(time(NULL));

    avg_clock_overhead_ms = measure_clock_overhead();

    parse_args(argc, argv);
    if (optind + 2 > argc) {
        fprintf(stderr, "Usage: %s -c|-e <threads> <filename>\n", argv[0]);
        return 1;
    }
    init_logs();
    init_event_types();
    int num_threads = atoi(argv[optind]);
    if (num_threads < 1 || num_threads > MAX_THREADS) {
        fprintf(stderr, "Invalid thread count (1-%d)\n", MAX_THREADS);
        cleanup();
        return 1;
    }
    total_threads = num_threads;
    const char* filename = argv[optind+1];
    log_message("Opening file: %s", filename);
    if ((file_descriptor = open(filename, O_RDONLY)) < 0) {
        log_error("Failed to open file: %s", strerror(errno));
        cleanup();
        return 1;
    }
    file_size = lseek(file_descriptor, 0, SEEK_END);
    log_message("File size: %ld bytes", (long)file_size);
    lseek(file_descriptor, 0, SEEK_SET);
    clock_gettime(CLOCK_MONOTONIC_RAW, &start_timestamp);
    init_queues();
    for (int i = 1; i <= num_threads; i++) {
        memset(&thread_states[i], 0, sizeof(ThreadState));
        thread_states[i].thread_id = i;
        thread_states[i].file_position = 0;
        thread_states[i].is_completed = 0;
        thread_states[i].blocked_on = NONE;
        thread_states[i].block_count = 0;
        thread_states[i].is_ready = 1;
        thread_states[i].arrival_time = i;
        thread_states[i].waiting_time_ms = 0;
        thread_states[i].start_time = thread_states[i].arrival_time;
        pthread_mutex_lock(&global_counter_mutex);
        thread_states[i].queue_entry_time = ++globalQueueCounter;
        pthread_mutex_unlock(&global_counter_mutex);
        clock_gettime(CLOCK_MONOTONIC_RAW, &thread_states[i].queue_entry_timestamp);
        thread_states[i].total_wait_time_ms = 0;
        thread_states[i].just_arrived = 0;
        thread_states[i].last_blocked_event = NONE;
        thread_states[i].total_exec_time_ms = 0;
    }
    assign_queues_to_threads(num_threads);
    log_message("=== Starting MLFQ Scheduler ===");
    schedule_threads(num_threads);
    log_message("=== Scheduler finished ===");
    print_statistics(num_threads);
    cleanup();
    return 0;
}