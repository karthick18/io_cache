/*   Thread pool utility functions
;    Copyright (C) 2010-2011 A.R.Karthick 
;    <a.r.karthick@gmail.com, a_r_karthic@users.sourceforge.net>
;
;    This program is free software; you can redistribute it and/or modify
;    it under the terms of the GNU General Public License as published by
;    the Free Software Foundation; either version 2 of the License, or
;    (at your option) any later version.
;
;    This program is distributed in the hope that it will be useful,
;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;    GNU General Public License for more details.
;
;    You should have received a copy of the GNU General Public License
;    along with this program; if not, write to the Free Software
;    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
;
;
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#ifdef __linux__
#include <sys/types.h>
#include <sys/syscall.h>
#endif
#include <assert.h>
#include "task_pool.h"

#define TASK_POOL_RUNNING (0x1)
#define TASK_POOL_ACTIVE (0x2)

#ifdef __linux__
#define GET_TASK_ID(stats) ( (stats)->task_id )
#define SET_TASK_ID(stats) do { (stats)->task_id = (pthread_t)syscall(SYS_gettid); } while(0)
#define CLEAR_TASK_ID(stats) do { (stats)->task_id = 0; } while(0)
#else
#define SET_TASK_ID(stats) do {;}while(0)
#define GET_TASK_ID(stats) ( (stats)->tid )
#define CLEAR_TASK_ID(stats) do {;}while(0)
#endif

typedef struct task_pool_arg
{
    task_pool_t *tp;
    task_pool_stats_t *stats;
} task_pool_arg_t;

static pthread_key_t g_task_pool_key;
static pthread_once_t task_pool_once = PTHREAD_ONCE_INIT;
/*
 * Create a pthread key for storing task pool info. for each thread.
 */

static void task_pool_initialize(void)
{
    int rc = pthread_key_create(&g_task_pool_key, NULL);
    assert(rc == 0);
}

void task_pool_finalize(void)
{
    int rc = pthread_key_delete(g_task_pool_key);
    assert(rc == 0);
}

int task_pool_data_set(void * data)
{
    pthread_once(&task_pool_once, task_pool_initialize);
    return pthread_setspecific(g_task_pool_key, data);
}

int task_pool_data_get(void **thread_data)
{
    void *data;
    if(!thread_data)
        return -1;
    pthread_once(&task_pool_once, task_pool_initialize);
    data = pthread_getspecific(g_task_pool_key);
    *thread_data = data;
    return 0;
}


void *task_pool_entry(void *arg)
{
    task_pool_t *tp = ((task_pool_arg_t*)arg)->tp;
    task_pool_stats_t *stats = ((task_pool_arg_t*)arg)->stats;

    free(arg);

    SET_TASK_ID(stats);
    task_pool_data_set((void *)tp);
    pthread_mutex_lock(&tp->mutex);
    
    while( (tp->flags & TASK_POOL_RUNNING) ) 
    {      
        int rc = 0;
        task_pool_timeout_t timeout = { .sec=45, .msec=0 }; /* should be configurable? */
        if (tp->pre_idle_fn) 
        {
            pthread_mutex_unlock(&tp->mutex);
            tp->pre_idle_fn(tp->pre_idle_cookie);
            pthread_mutex_lock(&tp->mutex);
        }

        if (!tp->on_deck_fn)
        {
            struct timespec tv = {0};
            ++tp->num_idle_tasks;
            timeout_to_tv_abs(timeout, tv);
            rc = pthread_cond_timedwait(&tp->cond, &tp->mutex, &tv);
            --tp->num_idle_tasks;
            if (rc == ETIMEDOUT)
            {
                /*
                 * Keep atleast 2 tasks in the taskpool ready to process clashing incoming requests.
                 */
                if(tp->num_tasks > 2)
                {
                    /*
                     * Quit this task if its idle for quite sometime.
                     */
                    break;
                }
            }
        }

        while ( (tp->flags & TASK_POOL_RUNNING) 
                && 
                tp->on_deck_fn)
        {
            task_pool_callback_t       fn = tp->on_deck_fn;
            void *     cookie = tp->on_deck_cookie;

            /* Clear this so that another call can be loaded in */
            tp->on_deck_fn = 0;
            tp->on_deck_cookie = 0;
            pthread_cond_broadcast(&tp->cond);  /* Wake up any caller that is waiting to load a task */

            /* Note that its a bit inefficient to use the same Cond to both wake the task producers
               and consumers... */

            /* 
             * Check if the task pool was quiesced.
             */
            rescan:
            if( !(tp->flags & TASK_POOL_ACTIVE) )
            {
                /*
                 * We block here for a resume to wake us back.
                 */
                ++tp->num_idle_tasks;
                rc = pthread_cond_wait(&tp->cond, &tp->mutex);
                --tp->num_idle_tasks;
                if( !(tp->flags & TASK_POOL_RUNNING)) break;
                goto rescan;
            }

            /* Call the user's function with our locks unlocked */
            ++tp->pending_jobs;
            stats->start_time = current_forward_time();
            pthread_mutex_unlock(&tp->mutex);
            fn(cookie);
            pthread_mutex_lock(&tp->mutex);
            stats->start_time = 0;
            --tp->pending_jobs;
        }

    }
    stats->tid = 0;
    CLEAR_TASK_ID(stats);
    --tp->num_tasks;
    pthread_mutex_unlock(&tp->mutex);
    return NULL;
}


/* Must be called with the taskpool mutex locked */
static void task_pool_new_task(task_pool_t *tp)
{
    if (tp->num_tasks < tp->max_tasks)
    {
        pthread_attr_t attr;
        int i;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        for (i = 0; i < tp->max_tasks; i++)
        {
            if (!tp->stats[i].tid)
            {
                int rc = 0;
                task_pool_arg_t *arg = calloc(1, sizeof(*arg));
                assert(arg != NULL);
                arg->tp = tp;
                arg->stats = tp->stats + i;
                arg->stats->start_time = 0;
                rc = pthread_create(&tp->stats[i].tid, &attr, task_pool_entry, (void*)arg);
                assert(rc == 0);
                ++tp->num_tasks;
                return;
            }
        }
        fprintf(stderr, "Task pool is out of free task slots. NumTasks [%d], MaxTasks [%d]\n",
                tp->num_tasks, tp->max_tasks);
    }
}

int task_pool_wake(task_pool_handle_t handle)
{
    task_pool_t *tp = (task_pool_t *) handle;

    if(!tp) return -1;

    pthread_mutex_lock(&tp->mutex);

    /* If there are no idle tasks, start one up so that the pool is active */
    if (!tp->num_idle_tasks) task_pool_new_task(tp);

    pthread_cond_broadcast(&tp->cond);  /* Wake up tasks! */

    pthread_mutex_unlock(&tp->mutex); 

    return 0;
}

int task_pool_run(task_pool_handle_t handle, task_pool_callback_t func, void * cookie)
{
    task_pool_t *tp = (task_pool_t *) handle;

    if(!tp) return -1;

    pthread_mutex_lock(&tp->mutex);

    /* If there are no idle tasks, start one up for this new job */
    if (tp->num_idle_tasks==0) task_pool_new_task(tp);

    while(tp->on_deck_fn) /* While someone's job is already waiting for a task... */
    {
        int rc;
        task_pool_new_task(tp); /* ... create a new task for it. */
        rc = pthread_cond_wait(&tp->cond, &tp->mutex);
    }

    assert(tp->on_deck_fn == NULL);
    assert(tp->on_deck_cookie == NULL);

    tp->on_deck_fn = func;
    tp->on_deck_cookie = cookie;
    pthread_cond_broadcast(&tp->cond);  /* Wake up tasks! */

    pthread_mutex_unlock(&tp->mutex);
    return 0;  
}


int task_pool_create(task_pool_handle_t *handle, int max_tasks, 
                     task_pool_callback_t pre_idle_func, void * pre_idle_cookie)
{
    task_pool_t *task_pool=NULL;
    int rc = -1;

    if(!handle || !max_tasks) return -1;

    task_pool = (task_pool_t*) calloc(1, sizeof(*task_pool));
    if(!task_pool)
        goto out;

    task_pool->stats = calloc(max_tasks, sizeof(*task_pool->stats));
    if(!task_pool->stats)
        goto out_free;

    rc = pthread_mutex_init(&task_pool->mutex, NULL);
    assert(rc == 0);
    rc = pthread_cond_init(&task_pool->cond, NULL);
    assert(rc == 0);
    task_pool->flags |= (TASK_POOL_RUNNING | TASK_POOL_ACTIVE);
    task_pool->num_tasks = 0;
    task_pool->max_tasks = max_tasks;
    task_pool->num_idle_tasks = 0;
    task_pool->pre_idle_fn = pre_idle_func;
    task_pool->pre_idle_cookie = pre_idle_cookie;
    task_pool->pending_jobs = 0;
    *handle = (task_pool_handle_t) task_pool;
    rc = 0;
    goto out;

    out_free:
    if(task_pool->stats)
    {
        free(task_pool->stats);
    }
    if(task_pool) free(task_pool);

    out:
    return rc;
}

int task_pool_delete(task_pool_handle_t handle)
{
    int rc = 0;
    task_pool_t *tp = (task_pool_t*)handle;

    if(!tp) return -1;

    rc = task_pool_stop(handle);
    pthread_cond_destroy(&tp->cond);
    pthread_mutex_destroy(&tp->mutex);
    free(tp->stats);
    free(tp);
    return rc;  
}

int task_pool_start(task_pool_handle_t handle)
{
    task_pool_t *tp = (task_pool_t*)handle;
    if(!tp) 
        return -1;

    pthread_mutex_lock(&tp->mutex);
    /* set the flag and then wake them all up */
    tp->flags |= TASK_POOL_RUNNING;
    pthread_cond_broadcast(&tp->cond);

    pthread_mutex_unlock(&tp->mutex);
    
    return 0;
}

int task_pool_stop_async(task_pool_handle_t handle)
{
    task_pool_t *tp = (task_pool_t*)handle;
    if(!tp) 
        return -1;

    pthread_mutex_lock(&tp->mutex);
    /* set the flag and then wake them all up */
    tp->flags &= ~TASK_POOL_RUNNING;
    pthread_cond_broadcast(&tp->cond);

    pthread_mutex_unlock(&tp->mutex);
    
    return 0;
}


int task_pool_stop(task_pool_handle_t handle)
{
    task_pool_t *tp = (task_pool_t*)handle;
    pthread_t task_id = 0;
    int i = 0;
    task_pool_timeout_t delay = {.sec = 0, .msec = 500 };

    if(!tp) 
        return -1;

    task_id = pthread_self();

    task_pool_stop_async(handle);

    pthread_mutex_lock(&tp->mutex);
    pthread_cond_broadcast(&tp->cond);
    for (i = 0; i < tp->max_tasks; i++)
    {
        if (tp->stats[i].tid && tp->stats[i].tid != task_id)
        {
            int tries = 0;
            while(tp->stats[i].tid 
                  &&
                  tries++ < 10)
            {
                pthread_mutex_unlock(&tp->mutex);
                task_pool_delay(delay);
                pthread_mutex_lock(&tp->mutex);
            }
            if(tp->stats[i].tid) 
                pthread_kill(tp->stats[i].tid, SIGKILL);
        }
        tp->stats[i].tid = 0;
    }
    pthread_mutex_unlock(&tp->mutex);

    return 0;
}


int task_pool_resume(task_pool_handle_t handle)
{
  task_pool_t *tp = (task_pool_t*)handle;

  if(!tp) 
      return -1;

  pthread_mutex_lock(&tp->mutex);
  tp->flags |= TASK_POOL_ACTIVE;
  pthread_cond_broadcast(&tp->cond);
  pthread_mutex_unlock(&tp->mutex);

  return 0;
}

int task_pool_quiesce(task_pool_handle_t handle)
{
    task_pool_t *tp = handle;
    pthread_t task_id = 0;
    int pending_jobs = 0;
    task_pool_timeout_t delay = {.sec = 0, .msec = 50 };
    unsigned int i = 0;

    if(!tp)
        return -1;

    task_id = pthread_self();

    pthread_mutex_lock(&tp->mutex);
    tp->flags &= ~TASK_POOL_ACTIVE;
    /*
     * Ignore waiting on self
     */
    for(i = 0; i < tp->max_tasks; ++i)
    {
        if(tp->stats[i].tid == task_id) 
            ++pending_jobs;
    }

    while(tp->pending_jobs > pending_jobs)
    {
        pthread_mutex_unlock(&tp->mutex);
        task_pool_delay(delay);
        pthread_mutex_lock(&tp->mutex);
    }
    pthread_mutex_unlock(&tp->mutex);

    return 0;
}

int task_pool_stats_get(task_pool_handle_t handle, task_pool_usage_t *pool_usage)
{
    task_pool_t *pool = (task_pool_t*)handle;
    if(!pool || !pool_usage) 
        return -1;
    pthread_mutex_lock(&pool->mutex);
    pool_usage->num_tasks = pool->num_tasks;
    pool_usage->num_idle_tasks = pool->num_idle_tasks;
    pool_usage->max_tasks = pool->max_tasks;
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}
