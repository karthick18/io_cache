#ifndef _TASK_POOL_H_
#define _TASK_POOL_H_

#include <pthread.h>

#ifdef __cplusplus
extern "C"
{
#endif

#ifdef __linux__
#define DECL_TASK_ID(task_id)  unsigned long long task_id;
#else
#define DECL_TASK_ID(task_id) 
#endif

#define current_forward_time() ({                   \
    struct timespec _t = {0};                       \
    clock_gettime(CLOCK_MONOTONIC, &_t);            \
    (long long)_t.tv_sec*1000000 + _t.tv_nsec/1000; \
})

#define timeout_to_tv_abs(timeout, tv) do {                 \
    clock_gettime(CLOCK_REALTIME, &(tv));                   \
    (tv).tv_sec += (timeout).sec + (timeout).msec/1000;     \
    (tv).tv_nsec += ((timeout).msec % 1000) * 1000 * 1000;  \
}while(0)

#define timeout_to_tv_rel(timeout, tv) do {                 \
    (tv).tv_sec = (timeout).sec + (timeout).msec/1000;      \
    (tv).tv_nsec = ((timeout).msec % 1000) * 1000 * 1000;   \
}while(0)

#define task_pool_delay(timeout) do {           \
    struct timespec _t = {0};                   \
    struct timespec _r = {0};                   \
    int _err = 0;                               \
    timeout_to_tv_rel(timeout, _t);             \
    __retry:                                    \
    while ( (_err = nanosleep(&_t, &_r) ) < 0 ) \
    {                                           \
        if(errno == EINTR)                      \
        {                                       \
            _t = _r;                            \
            _r = (struct timespec){0};          \
            goto __retry;                       \
        }                                       \
    }                                           \
}while(0)

typedef struct task_pool_timeout
{
    unsigned int sec;
    unsigned int msec;
} task_pool_timeout_t;

typedef struct task_pool_stats
{
    pthread_t tid;
    DECL_TASK_ID(task_id)
    long long       start_time;
}task_pool_stats_t;

typedef struct task_pool_usage
{
    int num_tasks;
    int max_tasks;
    int num_idle_tasks;
} task_pool_usage_t;

typedef void * (*task_pool_callback_t)(void *);

typedef struct task_pool
{
    int        num_tasks;
    int max_tasks;
    int num_idle_tasks;
    int          priority;
    unsigned int         flags;
    task_pool_stats_t   *stats;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    task_pool_callback_t pre_idle_fn;
    void *pre_idle_cookie;
    task_pool_callback_t on_deck_fn;
    void *on_deck_cookie;
    int pending_jobs;
} task_pool_t;

typedef struct task_pool* task_pool_handle_t;

/* preIdleFunc can call task_poolRun to skip idling and add another job */
int task_pool_create(task_pool_handle_t *handle,int max_tasks, task_pool_callback_t pre_idle_fun, 
                     void * pre_idle_cookie);

int task_pool_delete(task_pool_handle_t handle);

int task_pool_run(task_pool_handle_t handle, task_pool_callback_t func, void * cookie);

/* Ensures that there is at least 1 thread running in the pool, and wakes any idle threads.
   All idle threads will call the "pre-idle callback" if one has been registered before going
   back into idle.   
 */ 
int task_pool_wake(task_pool_handle_t handle);

int task_pool_stop_async(task_pool_handle_t handle);

int task_pool_stop(task_pool_handle_t handle);

int task_pool_start(task_pool_handle_t handle);

int task_pool_quiesce(task_pool_handle_t handle);

int task_pool_resume(task_pool_handle_t handle);

int task_pool_free_unused(void);

int task_pool_stats_get(task_pool_handle_t handle, task_pool_usage_t *stats);

int task_pool_data_set(void *data);

int task_pool_data_get(void **data);

#ifdef __cplusplus
}
#endif

#endif
