/*   
 * io_cache : A read only io-cache expected to be used with LARGE file sizes
 * preferably >= 1 GB.
 *
 * The io-cache uses a fixed mmap chunk size of 4MB to scatter gather the IOs/file reads
 * The maximum size of the cache is 4GB or 1024 entries of 4MB each. 
 * The cache has an aging thread associated with it that cleans up unused references in the file.
 * The cache could be used when a user expects to read and process large data sets stored in files without worrying
 * about the maintenance of the read buffers for such a large file when you are expected to iterate over your data-set
 * and retain them for post-processing or later reference.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <time.h>
#include <errno.h>
#ifdef VALGRIND
#define NDEBUG
#endif
#include <assert.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/resource.h>
#include "io_cache.h"
#include "list.h"
#include "task_pool.h"

#ifndef MIN
#define MIN(x, y)  ( (x) < (y)  ? (x) : (y) )
#endif


#define IO_CACHE_MMAP_CHUNK_SHIFT (22)
#define IO_CACHE_MMAP_CHUNK_SIZE (1LL << IO_CACHE_MMAP_CHUNK_SHIFT) /* 4 MB mmap chunk size per cache. Should be power of 2 */
#define IO_CACHE_MMAP_CHUNK_MASK (IO_CACHE_MMAP_CHUNK_SIZE-1) 
#define IO_CACHE_MAX_ENTRIES (1024) /* capturing 4GB of mmapp'ed space*/
#define IO_CACHE_MAX_SIZE (IO_CACHE_MMAP_CHUNK_SIZE * IO_CACHE_MAX_ENTRIES)
#define IO_CACHE_SIZE_DEFAULT (64<<20U) /* 64 MB cache */
#define IO_CACHE_MAX_POOL_THREADS (0x4)

/*
 * The cache entry itself
 */
typedef struct io_cache_entry
{
#define IO_CACHE_ENTRY_AGE (0x40)
#define IO_CACHE_LIST_ACTIVE 0x1 /*cannot be claimed unless under heavy pressure and 0 references*/
#define IO_CACHE_LIST_INACTIVE 0x2 /*for reclaim*/

    struct list_head e_active_list ; /*index into the active list*/
    struct list_head e_inactive_list; /* index into the inactive list*/
    struct list_head e_reclaim_list;
    char *e_data; /*data mapped in this cache*/
    loff_t e_offset;  /* start span */
    long long e_size;  /* the size of the mmap span covered*/
    int e_age; /* age of the cache entry */
    int e_flags;/* cache entry flags*/
    unsigned int e_refcnt; /*cache entry refcnt*/
} io_cache_entry_t;

typedef struct io_cache
{
#define IO_CACHE_ACTIVE 0x1
    io_cache_entry_t *c_cache;
    loff_t c_offset; /* current offset of the cache */
    struct list_head c_active_list; /* most rcu/fru list*/
    struct list_head c_inactive_list ; /* inactive or target for cache reaps */
    io_cache_stats_t c_stats;
    pthread_mutex_t c_active_mutex; /*lock for the active list*/
    pthread_mutex_t c_inactive_mutex; /*lock for the inactive list*/
    pthread_mutex_t c_aging_mutex; /*to synchronize with aging thread*/
    pthread_cond_t c_aging_cond; /*cond for the aging thread*/
    task_pool_handle_t c_io_pool; /* the pool handling the IO-reads. Unused now till read-ahead is required */
    task_pool_handle_t c_aging_pool; /* the task pool for the aging cache with 1 thread */
    int c_fd;
    char c_filename[PATH_MAX];
    long long c_filesize;
    void (*c_read_callback)(char *data, loff_t offset, long long size); /* unused till read-ahead required*/
    int c_state; /* state of the io cache*/
} io_cache_t;

typedef struct io_cache_reclaim
{
    long long r_entries; /* entries to reclaim */
    struct list_head r_list; /*reclaim list*/
} io_cache_reclaim_t;

#define IO_CACHE_HIT(cache, cache_entry) do {   \
    ++(cache)->c_stats.s_hits;                  \
    (cache_entry)->e_age = IO_CACHE_ENTRY_AGE;  \
}while(0)

#define IO_CACHE_MISS(cache, cache_entry) do {  \
    ++(cache)->c_stats.s_misses;                \
}while(0)

#define IO_CACHE_SET_SCAN_TIME(cache) do {      \
    (cache)->c_stats.s_scan_time = time(NULL);  \
}while(0)

#define IO_CACHE_MLOCK(addr, size, err) do {                            \
    err = 0;                                                            \
    if(io_cache_mlock_enabled > 0)                                      \
    {                                                                   \
        err = mlock(addr, size);                                        \
    }                                                                   \
    else                                                                \
    {                                                                   \
        err = madvise(addr, size, MADV_SEQUENTIAL);                     \
        if(err < 0 )                                                    \
        {                                                               \
            log_error("madvise on size [%lld] failed with error [%s]\n", \
                      size, strerror(errno));                           \
            err = 0;                                                    \
        }                                                               \
    }                                                                   \
}while(0)

#define IO_CACHE_MUNLOCK(addr, size) do {       \
    if(io_cache_mlock_enabled > 0)              \
    {                                           \
        munlock(addr, size);                    \
    }                                           \
}while(0)

static int io_cache_mlock_enabled;
static int io_cache_default_log_level = LOG_INFO;
static pthread_once_t io_cache_rlimit_once = PTHREAD_ONCE_INIT;

#ifdef VALGRIND

static int alloc_map(io_cache_t *cache, io_cache_entry_t *cache_entry, long long size, loff_t offset)
{
    cache_entry->e_data = calloc(1, size);
    assert(cache_entry->e_data != NULL);
    /*
     * Fake a cache entry with an offset as its going to be a zero sink.
     */
    cache_entry->e_offset = offset;
    return 0;
}

static int free_map(io_cache_t *cache, io_cache_entry_t *cache_entry)
{
    assert(cache_entry->e_data != NULL);
    free(cache_entry->e_data);
    log(LOG_DEBUG, "Unmapping entry at offset [%lld], size [%lld]\n",
        (long long int)cache_entry->e_offset, cache_entry->e_size);
    cache_entry->e_data = NULL;
    cache_entry->e_offset = -1;
    return 0;
}

#else

static int alloc_map(io_cache_t *cache, io_cache_entry_t *cache_entry, long long size, loff_t offset)
{
    int err = -1;
    char *map = mmap(0, size, PROT_READ, MAP_PRIVATE, cache->c_fd, offset);
    if(map == MAP_FAILED)
    {
        log_error("Critical error [%s] trying to mmap chunk of size [%lld] at offset [%lld]\n",
                  strerror(errno), size, (long long int) offset);
        goto out;
    }
    IO_CACHE_MLOCK(map, size, err);
    if(err < 0)
    {
        log_error("Critical error [%s] trying to mlock chunk of size [%lld] at offset [%lld]\n",
                  strerror(errno), size, (long long int)offset);
        munmap(map, size);
        goto out;
    }
    cache_entry->e_data = map;
    cache_entry->e_offset = offset;
    err = 0;
    out:
    return err;
}

static int free_map(io_cache_t *cache, io_cache_entry_t *cache_entry)
{
    int err;
    assert(cache_entry->e_data != NULL);
    IO_CACHE_MUNLOCK(cache_entry->e_data, cache_entry->e_size);
    err = munmap(cache_entry->e_data, cache_entry->e_size);
    log(LOG_DEBUG, "Unmapped entry at offset [%lld], size [%lld]\n",
        (long long int)cache_entry->e_offset, cache_entry->e_size);
    if(err == 0)
    {
        cache_entry->e_data = NULL;
        cache_entry->e_offset = -1;
    }
    else
    {
        log_error("Error [%s] trying to munmap region [%p] at offset [%lld] of size [%lld]\n",
                  strerror(errno), (void*)cache_entry->e_data, (long long int)cache_entry->e_offset,
                  cache_entry->e_size);
    }
    return err;
}

#endif

void log_output(FILE *fptr, int level, const char *fmt, ...)
{
    va_list ptr;
    char buf[0xff+1];
    int bytes;
    static const char *const level_map[] = {"EMERG", "ALERT", "CRIT", "ERROR", "WARNING", "NOTICE", "INFO", "DEBUG"};
    if(!fptr || level > io_cache_default_log_level)
        return;
    bytes = snprintf(buf, sizeof(buf), "%s) ", level_map[level]);
    va_start(ptr, fmt);
    vsnprintf(buf+bytes, sizeof(buf) - bytes, fmt, ptr);
    va_end(ptr);
    
    fprintf(fptr, buf);
}

static void io_cache_rlimit_enable(void)
{
    if(!getuid() || !geteuid())
    {
        struct rlimit rlim;
        rlim.rlim_cur = RLIM_INFINITY;
        rlim.rlim_max = RLIM_INFINITY;
        if(setrlimit(RLIMIT_AS, &rlim) < 0)
        {
            log(LOG_WARNING, "setrlimit for RLIMIT_AS failed with error [%s]. Try running as root\n", 
                       strerror(errno));
            return;
        }
        if(setrlimit(RLIMIT_MEMLOCK, &rlim) < 0)
        {
            log(LOG_WARNING, "setrlimit for RLIMIT_MEMLOCK failed with error [%s]. Try running as root\n", 
                strerror(errno));
            return;
        }
        io_cache_mlock_enabled = 1;
    }
}

static int io_cache_offset_cmp(io_cache_entry_t *ref, io_cache_entry_t *iter)
{
    return ref->e_offset - iter->e_offset;
}

static __inline__ void active_cache_add_locked(io_cache_t *cache, io_cache_entry_t *cache_entry, int sort_flag)
{
    if( !(cache_entry->e_flags & IO_CACHE_LIST_ACTIVE) )
    {
        if(sort_flag)
            list_sort_add(&cache_entry->e_active_list, &cache->c_active_list, 
                          io_cache_entry_t, e_active_list, io_cache_offset_cmp);
        else
            list_add_tail(&cache_entry->e_active_list, &cache->c_active_list);
        cache_entry->e_flags |= IO_CACHE_LIST_ACTIVE;
        ++cache->c_stats.s_active_entries;
    }
}

static __inline__ void active_cache_add(io_cache_t *cache, io_cache_entry_t *cache_entry, int sort_flag)
{
    pthread_mutex_lock(&cache->c_active_mutex);
    active_cache_add_locked(cache, cache_entry, sort_flag);
    pthread_mutex_unlock(&cache->c_active_mutex);
}

static __inline__ void inactive_cache_add_locked(io_cache_t *cache, io_cache_entry_t *cache_entry)
{
    if(!(cache_entry->e_flags & IO_CACHE_LIST_INACTIVE))
    {
        list_add_tail(&cache_entry->e_inactive_list, &cache->c_inactive_list);
        cache_entry->e_flags |= IO_CACHE_LIST_INACTIVE;
        ++cache->c_stats.s_inactive_entries;
    }
}

static __inline__ void inactive_cache_add(io_cache_t *cache, io_cache_entry_t *cache_entry)
{
    pthread_mutex_lock(&cache->c_inactive_mutex);
    inactive_cache_add_locked(cache, cache_entry);
    pthread_mutex_unlock(&cache->c_inactive_mutex);
}

static __inline__ void active_cache_del_locked(io_cache_t *cache, io_cache_entry_t *cache_entry)
{
    if(cache_entry->e_flags & IO_CACHE_LIST_ACTIVE)
    {
        cache_entry->e_flags &= ~IO_CACHE_LIST_ACTIVE;
        --cache->c_stats.s_active_entries;
        list_del(&cache_entry->e_active_list);
    }
}

static __inline__ void active_cache_del(io_cache_t *cache, io_cache_entry_t *cache_entry)
{
    pthread_mutex_lock(&cache->c_active_mutex);
    active_cache_del_locked(cache, cache_entry);
    pthread_mutex_unlock(&cache->c_active_mutex);
}

static __inline__ void inactive_cache_del_locked(io_cache_t *cache, io_cache_entry_t *cache_entry)
{
    if(cache_entry->e_flags & IO_CACHE_LIST_INACTIVE)
    {
        cache_entry->e_flags &= ~IO_CACHE_LIST_INACTIVE;
        --cache->c_stats.s_inactive_entries;
        list_del(&cache->c_inactive_list);
    }
}

static __inline__ void inactive_cache_del(io_cache_t *cache, io_cache_entry_t *cache_entry)
{
    pthread_mutex_lock(&cache->c_inactive_mutex);
    inactive_cache_del_locked(cache, cache_entry);
    pthread_mutex_unlock(&cache->c_inactive_mutex);
}

/*
 * The aging thread just ages the entries in the cache.
 * Finds entries with age 0 and cleans them up. And moves the free cache slot into inactive list.
 * In order to have low lock contention times, it moves the list into a batch list head on the stack,
 * cleans the entry lockless and then moves the cleaned entries into the inactive list for reclaim
 * on a cache read.
 */
static void *io_cache_aging_thread(void *arg)
{
    io_cache_t *cache = arg;
    io_cache_entry_t *cache_entry = NULL;
    register struct list_head *iter;
    struct list_head *next;
    DECLARE_LIST_HEAD(reclaim_list);

    pthread_mutex_lock(&cache->c_aging_mutex);
    pthread_mutex_lock(&cache->c_active_mutex);
    while(cache->c_state & IO_CACHE_ACTIVE)
    {
        task_pool_timeout_t timeout = {.sec = 5, .msec =0};
        struct timespec tv = {0};
        long long reclaim_entries = 0;
        long long reclaim_size = 0;

        IO_CACHE_SET_SCAN_TIME(cache);
        for(iter = cache->c_active_list.next ; iter != &cache->c_active_list; iter = next)
        {
            cache_entry = list_entry(iter, io_cache_entry_t, e_active_list);
            next = iter->next;
            if(cache_entry->e_refcnt > 0)
                continue;
            if(!cache_entry->e_age)
            {
                assert( !(cache_entry->e_flags & IO_CACHE_LIST_INACTIVE) );
                active_cache_del_locked(cache, cache_entry);
                list_add_tail(&cache_entry->e_inactive_list, &reclaim_list);
                ++reclaim_entries;
            }
            else 
                cache_entry->e_age >>= 1; 
        }
        pthread_mutex_unlock(&cache->c_active_mutex);
        list_for_each(iter, &reclaim_list)
        {
            cache_entry = list_entry(iter, io_cache_entry_t, e_inactive_list);
            free_map(cache, cache_entry);
            reclaim_size += cache_entry->e_size;
            cache_entry->e_flags |= IO_CACHE_LIST_INACTIVE;
        }
        /*
         * Move the reclaimed list into the inactive list of the cache. 
         */
        pthread_mutex_lock(&cache->c_inactive_mutex);
        list_splice(&reclaim_list, &cache->c_inactive_list);
        cache->c_stats.s_inactive_entries += reclaim_entries;
        pthread_mutex_unlock(&cache->c_inactive_mutex);

        pthread_mutex_lock(&cache->c_active_mutex);
        cache->c_stats.s_cur_size -= reclaim_size;
        cache->c_stats.s_cur_entries -= reclaim_entries;
        pthread_mutex_unlock(&cache->c_active_mutex);
        timeout_to_tv_abs(timeout, tv);
        /*
         * Check if the cache state is still active.
         */
        if(!(cache->c_state & IO_CACHE_ACTIVE))
            goto out_unlock;
        pthread_cond_timedwait(&cache->c_aging_cond, &cache->c_aging_mutex, &tv);
        pthread_mutex_lock(&cache->c_active_mutex);
    }
    pthread_mutex_unlock(&cache->c_active_mutex);
    out_unlock:
    pthread_mutex_unlock(&cache->c_aging_mutex);
    return NULL;
}

static int validate_cache_size(unsigned long long cache_size)
{
    long nr_pages = 0;
    unsigned long long ram_size;
    if(cache_size > IO_CACHE_MAX_SIZE)
    {
        log_error("Cache size [%lld] exceeds max allowed cache size [%lld]\n",
                  cache_size, IO_CACHE_MAX_SIZE);
        return -1;
    }
    nr_pages = sysconf(_SC_PHYS_PAGES);
    if(nr_pages < 0) return 0;
    ram_size = nr_pages * getpagesize();
    if(cache_size * 2 > ram_size)
    {
        log_error("Cache size [%lld] is greater than twice of available ram size [%lld] bytes\n",
                  cache_size, ram_size);
        return -1;
    }
    return 0;
}

int io_cache_initialize(const char *filename, unsigned long long cache_size, 
                        void (*read_callback)(char *data, loff_t offset, long long size),
                        io_cache_handle_t *handle)
{
    io_cache_t *cache = NULL;
    int fd;
    int err = -1;
    loff_t offset = 0;
    unsigned long long i;
    struct stat stbuf;
    if(!filename || !handle)
        goto out;
    if(stat(filename,&stbuf))
    {
        log_error("Stats get for file [%s] returned [%s]\n",
                  filename, strerror(errno));
        goto out;
    }
    fd = open(filename, O_RDWR, 0777);
    if(fd < 0)
        goto out;

    if(!cache_size) 
        cache_size = IO_CACHE_SIZE_DEFAULT;
    if(cache_size > stbuf.st_size)
        cache_size = stbuf.st_size;
    /*
     * Align cache size to mmap chunk size.
     */
    cache_size += IO_CACHE_MMAP_CHUNK_MASK;
    cache_size &= ~IO_CACHE_MMAP_CHUNK_MASK;
    if(validate_cache_size(cache_size) < 0)
        goto out_close;

    cache = calloc(1,sizeof(*cache));
    assert(cache != NULL);
    cache->c_fd = fd;
    cache->c_filename[0] = 0;
    cache->c_filesize = stbuf.st_size;
    strncat(cache->c_filename, filename, sizeof(cache->c_filename)-1);
    cache->c_read_callback = read_callback;
    cache->c_stats.s_entries = cache_size >> IO_CACHE_MMAP_CHUNK_SHIFT;
    cache->c_stats.s_cur_entries = cache->c_stats.s_entries;
    cache->c_stats.s_map_size = IO_CACHE_MMAP_CHUNK_SIZE;
    cache->c_cache = calloc(cache->c_stats.s_entries, sizeof(*cache->c_cache));
    cache->c_stats.s_size = cache_size; 
    cache->c_stats.s_cur_size = cache_size;
    err = pthread_mutex_init(&cache->c_active_mutex, NULL);
    assert(err == 0);
    err = pthread_mutex_init(&cache->c_inactive_mutex, NULL);
    assert(err == 0);
    err = pthread_mutex_init(&cache->c_aging_mutex, NULL);
    assert(err == 0);
    err = pthread_cond_init(&cache->c_aging_cond, NULL);
    assert(err == 0);
    LIST_HEAD_INIT(&cache->c_active_list);
    LIST_HEAD_INIT(&cache->c_inactive_list);
    pthread_once(&io_cache_rlimit_once, io_cache_rlimit_enable);
    /*
     * We mmap the entries into the cache in mmap chunk sizes and fill the cache.
     */
    for(i = 0; i < cache->c_stats.s_entries; ++i)
    {
        err = alloc_map(cache, &cache->c_cache[i], IO_CACHE_MMAP_CHUNK_SIZE, offset);
        if(err < 0)
            goto out_mmap;

        cache->c_cache[i].e_size = IO_CACHE_MMAP_CHUNK_SIZE;
        cache->c_cache[i].e_age = IO_CACHE_ENTRY_AGE;
        /*
         * Add the cache entry to the inactive list.
         */
        active_cache_add(cache, &cache->c_cache[i], 0);
        offset += IO_CACHE_MMAP_CHUNK_SIZE;
    }

    /*
     * We are here when the cache is pinned! Go ahead and create the task pools
     */
    err = task_pool_create(&cache->c_io_pool, IO_CACHE_MAX_POOL_THREADS, 0, 0);
    if(err < 0)
        goto out_mmap;
    
    err = task_pool_create(&cache->c_aging_pool, 1, 0, 0);
    if(err < 0)
    {
        task_pool_delete(cache->c_io_pool);
        goto out_mmap;
    }
    /*
     * start the aging cache thread. after activating the cache.
     */
    cache->c_state |= IO_CACHE_ACTIVE;
    err = task_pool_run(cache->c_aging_pool, io_cache_aging_thread, (void*)cache);
    if(err < 0)
    {
        task_pool_delete(cache->c_io_pool);
        task_pool_delete(cache->c_aging_pool);
        goto out_mmap;
    }

    *handle = (io_cache_handle_t)cache;
    goto out;

    out_mmap:
    {
        long long j;
        for(j = 0; j < i; ++j)
            free_map(cache, &cache->c_cache[j]);
    }

    free(cache);

    out_close:
    close(fd);
    
    out:
    return err;
}

static int io_cache_age_cmp(const void *entry1, const void *entry2)
{
    io_cache_entry_t *c_entry1 = *(io_cache_entry_t**)entry1;
    io_cache_entry_t *c_entry2 = *(io_cache_entry_t**)entry2;
    return c_entry1->e_age - c_entry2->e_age;
}

static int io_cache_purge(io_cache_t *cache, io_cache_reclaim_t *reclaim_entry)
{
    int err = -1;
    io_cache_entry_t *cache_entry;
    long long reclaim_entries = reclaim_entry->r_entries;
    /*
     * First hit: inactive list
     */
    pthread_mutex_lock(&cache->c_inactive_mutex);
    while(reclaim_entries > 0
          &&
          !LIST_EMPTY(&cache->c_inactive_list))
    {
        struct list_head *reclaim = cache->c_inactive_list.next;
        cache_entry = list_entry(reclaim, io_cache_entry_t, e_inactive_list);
        assert(cache_entry->e_flags & IO_CACHE_LIST_INACTIVE);
        list_del(&cache_entry->e_inactive_list);
        list_add_tail(&cache_entry->e_inactive_list, &reclaim_entry->r_list);
        --cache->c_stats.s_inactive_entries;
        --reclaim_entries;
    }
    pthread_mutex_unlock(&cache->c_inactive_mutex);
    if(reclaim_entries > 0)
    {
        /*
         * Find entries with the minimum age
         */
        register struct list_head *iter;
        io_cache_entry_t **cache_entries = NULL;
        long long active_entries = 0;
        pthread_mutex_lock(&cache->c_active_mutex);
        /*
         * Check if there are slots available to reclaim.   
         */
        if(reclaim_entries + cache->c_stats.s_locked_entries > cache->c_stats.s_active_entries) 
        {
            pthread_mutex_unlock(&cache->c_active_mutex);
            log_error("No reclaim slots available in the active cache to fill [%lld] entries\n",
                      reclaim_entries);
            goto out;
            
        }
        /*
         * Look for active cache entries with the lowest age as a target for reclaim.
         * We don't have to maintain a sorted list/rbtree based on age for this because the offset
         * is already sorted and maintaining an additional sorted list when the upper bound of the cache
         * is going to be finite is an overkill.
         */
        cache_entries = calloc(cache->c_stats.s_active_entries, sizeof(*cache_entries));
        assert(cache_entries != NULL);
        list_for_each(iter, &cache->c_active_list)
        {
            cache_entry = list_entry(iter, io_cache_entry_t, e_active_list);
            assert(cache_entry->e_flags & IO_CACHE_LIST_ACTIVE);
            if(cache_entry->e_refcnt > 0) 
                continue;
            cache_entries[active_entries++] = cache_entry;
        }
        assert(active_entries + cache->c_stats.s_locked_entries == cache->c_stats.s_active_entries);
        qsort(cache_entries, active_entries, sizeof(*cache_entries), io_cache_age_cmp);
        active_entries = 0;
        while(reclaim_entries > 0)
        {
            cache_entry = cache_entries[active_entries++];
            log(LOG_DEBUG, "Purging cache entry with age [%d] at offset [%lld]\n", 
                cache_entry->e_age, (long long int) cache_entry->e_offset);
            free_map(cache, cache_entry);
            active_cache_del_locked(cache, cache_entry);
            list_add_tail(&cache_entry->e_inactive_list, &reclaim_entry->r_list);
            cache_entry->e_flags |= IO_CACHE_LIST_INACTIVE;
            cache_entry->e_age = 0;
            cache->c_stats.s_cur_size -= cache_entry->e_size;
            --cache->c_stats.s_cur_entries;
            --reclaim_entries;
        }
        pthread_mutex_unlock(&cache->c_active_mutex);
        free(cache_entries);
        /*
         * Should have reclaimed
         */
        assert(reclaim_entries == 0);
    }

    pthread_mutex_lock(&cache->c_inactive_mutex);
    if(cache->c_stats.s_inactive_entries < 2) /* trigger aging if at the threshold*/
    {
        pthread_cond_signal(&cache->c_aging_cond);
    }
    pthread_mutex_unlock(&cache->c_inactive_mutex);

    err = 0;
    out:
    return err;
}

/*
 * Called with active cache mutex held.
 */
static int __io_cache_verify(io_cache_t *cache)
{
    int err = -1;
    register struct list_head *iter;
    io_cache_entry_t *last_entry = NULL;
    list_for_each(iter, &cache->c_active_list)
    {
        io_cache_entry_t *cache_entry = list_entry(iter, io_cache_entry_t, e_active_list);
        if(last_entry 
           &&
           last_entry->e_offset + IO_CACHE_MMAP_CHUNK_SIZE > cache_entry->e_offset)
        {
            log_error("Last cache entry offset [%lld] with chunk size [%lld] is greater than "
                      "current entry offset [%lld]\n",
                      (long long int)last_entry->e_offset, last_entry->e_size,
                      (long long int)cache_entry->e_offset);
#if 0
            /*
             * No self healing as it should be a potential cache corruption.
             */
            active_cache_del_locked(cache, last_entry);
            cache->c_stats.s_cur_size -= IO_CACHE_MMAP_CHUNK_SIZE;
            --cache->c_stats.s_cur_entries;
            last_entry->e_data = NULL;
            last_entry->e_offset = -1;
            last_entry->e_size = IO_CACHE_MMAP_CHUNK_SIZE;
            last_entry->e_flags = 0;
            last_entry->e_refcnt = 0;
            last_entry->e_inactive_list.next = last_entry->e_inactive_list.prev = NULL;
            inactive_cache_add(cache, last_entry);
#endif
            return err;
        }
        last_entry = cache_entry;
    }
    pthread_mutex_unlock(&cache->c_active_mutex);
    pthread_mutex_lock(&cache->c_inactive_mutex);
    list_for_each(iter, &cache->c_inactive_list)
    {
        io_cache_entry_t *cache_entry = list_entry(iter, io_cache_entry_t, e_inactive_list);
        if(cache_entry->e_refcnt > 0)
        {
            log_error("Inactive cache entry having a reference count\n");
            goto out_unlock;
        }
    }
    err = 0;

    out_unlock:
    pthread_mutex_unlock(&cache->c_inactive_mutex);
    pthread_mutex_lock(&cache->c_active_mutex);

    return err;
}

int io_cache_verify(io_cache_handle_t handle)
{
    io_cache_t *cache = handle;
    int err = -1;
    if(!cache) return err;
    pthread_mutex_lock(&cache->c_active_mutex);
    err = __io_cache_verify(cache);
    pthread_mutex_unlock(&cache->c_active_mutex);
    return err;
}

/*
 * Called with the cache active_mutex held.
 */
static int __io_cache_read(io_cache_handle_t handle, char *buffer, loff_t offset, long long *len)
{
    io_cache_t *cache = handle;
    int err = -1;
    register struct list_head *iter;
    struct list_head *next;
    io_cache_entry_t *cache_entry;
    long long ori_size;
    long long size;
    long long count;
    loff_t cur_offset;
    loff_t end_offset;
    loff_t from_offset;
    struct list_head *cache_intersect = NULL;
    io_cache_reclaim_t cache_reclaim = {0};
    io_cache_entry_t **undo_list = NULL;
    long long undo_list_entries = 0;

    if(!cache || !buffer || offset < 0 || !len)
        goto out;

    if(offset >= cache->c_filesize || *len <= 0)
    {
        *len = 0;
        err = 0;
        goto out;
    }

    ori_size = size = *len;

    if(offset + size > cache->c_filesize)
    {
        ori_size = size = cache->c_filesize - offset;
    }

    /*
     * If the request is greater than the cache size itself, fail it.
     */
    if(size > cache->c_stats.s_size)
    {
        log_error("Request cache read size [%lld] greater than the cache size [%lld]\n",
                  size, cache->c_stats.s_size);
        goto out;
    }

    LIST_HEAD_INIT(&cache_reclaim.r_list);
    undo_list = calloc(cache->c_stats.s_entries, sizeof(*undo_list));
    assert(undo_list != NULL);

    /*
     * First step calculate the deficit and see if the cache is to be filled/extended.
     */
    cur_offset = offset;
    end_offset = cur_offset + size;
    list_for_each(iter, &cache->c_active_list)
    {
        cache_entry = list_entry(iter, io_cache_entry_t, e_active_list);

        if(end_offset <= cache_entry->e_offset)
            break;

        if(cache_entry->e_offset + cache_entry->e_size <= offset)
            continue;

        if(offset >= cache_entry->e_offset
           && 
           offset < cache_entry->e_offset + cache_entry->e_size)
        {
            cache_intersect = iter;
            from_offset = cur_offset = offset;
        }
        else
        {
            if(!cache_intersect)
            {
                cache_intersect = iter;
                cur_offset = cache_entry->e_offset;
            }
            from_offset = cache_entry->e_offset;
        }
        count = MIN(cache_entry->e_offset + cache_entry->e_size - from_offset, size);
        if(from_offset + count > end_offset)
            count = end_offset - from_offset;
        undo_list[undo_list_entries++] = cache_entry;
        ++cache_entry->e_refcnt;
        if(cache_entry->e_refcnt == 1)
            ++cache->c_stats.s_locked_entries;
        size -= count;
        if(size <= 0 ) 
            break;
    }

    if(size > 0)
    {
        pthread_mutex_unlock(&cache->c_active_mutex);
        /*
         * Reclaim the cache entries required. 
         */
        from_offset = offset & ~IO_CACHE_MMAP_CHUNK_MASK;
        size += offset - from_offset; /* offset alignment skews*/
        size += IO_CACHE_MMAP_CHUNK_MASK;
        size &= ~IO_CACHE_MMAP_CHUNK_MASK;
        cache_reclaim.r_entries = size >> IO_CACHE_MMAP_CHUNK_SHIFT;
        log(LOG_DEBUG, "Purging IO cache to reclaim [%lld] entries and size [%lld]\n",
            cache_reclaim.r_entries, size);
        err = io_cache_purge(cache, &cache_reclaim);
        pthread_mutex_lock(&cache->c_active_mutex);
        if(err < 0)
        {
            log_error("Cache read for size [%lld] failed to reclaim [%lld] entries as the cache is hot\n", 
                      size, cache_reclaim.r_entries);
            goto out_undo;
        }
    }

    err = -1;
    size = ori_size;
    /*
     * If we intersected, then try filling the holes here.
     */
    if(cache_intersect)
    {
        for(iter = cache_intersect; size > 0 && cur_offset < end_offset &&
                iter != &cache->c_active_list; iter = next)
        {
            next = iter->next;
            cache_entry = list_entry(iter, io_cache_entry_t, e_active_list);
            if(cur_offset < cache_entry->e_offset) /* hole */
            {
                struct list_head *reclaim = cache_reclaim.r_list.next;
                assert(!LIST_EMPTY(&cache_reclaim.r_list)); /* this should be pre-filled*/
                cache_entry = list_entry(reclaim, io_cache_entry_t, e_inactive_list);
                log(LOG_DEBUG, "Mapping a cache entry(hole) at offset [%lld] of size [%lld]\n", 
                    (long long int)cur_offset, cache_entry->e_size);
                err = alloc_map(cache, cache_entry, cache_entry->e_size, cur_offset);
                if(err < 0 )
                    goto out_undo;
                err = -1;
                list_del(&cache_entry->e_inactive_list);
                --cache_reclaim.r_entries;
                cache_entry->e_flags &= ~IO_CACHE_LIST_INACTIVE;
                cache_entry->e_flags |= IO_CACHE_LIST_ACTIVE;
                cache_entry->e_age = IO_CACHE_ENTRY_AGE;
                list_add_tail(&cache_entry->e_active_list, iter); /* add the hole before this entry */
                ++cache->c_stats.s_active_entries;
                ++cache->c_stats.s_cur_entries;
                cache->c_stats.s_cur_size += cache_entry->e_size;
                count = MIN(cache_entry->e_size, size);
                if(cur_offset + count > end_offset)
                    count = end_offset - cur_offset;
                log(LOG_DEBUG, "Copying buffer to offset [%lld], from [%lld], count [%lld]\n",
                    (long long int)(cur_offset - offset), 
                    (long long int)cache_entry->e_offset, count);
                memcpy(buffer + cur_offset - offset,
                       cache_entry->e_data, count);
                next = iter; /* rescan the same entry to accomodate more holes*/
                IO_CACHE_MISS(cache, cache_entry);
            }
            else
            {
                /*
                 * here the cur_offset should be equal to the cache entry offset when not intersecting
                 */
                if(!cache_intersect)
                    assert(cur_offset == cache_entry->e_offset);
                else cache_intersect = NULL;
                count = MIN(cache_entry->e_offset + cache_entry->e_size - cur_offset,
                            size);
                if(cur_offset + count > end_offset)
                    count = end_offset - cur_offset;
                log(LOG_DEBUG, "Copying buffer to offset [%lld], from [%lld], count [%lld]\n",
                    (long long int)(cur_offset - offset), 
                    (long long int)cur_offset, count);
                memcpy(buffer + cur_offset - offset,
                       cache_entry->e_data + cur_offset - cache_entry->e_offset,
                       count);
                IO_CACHE_HIT(cache, cache_entry);
            }
            cur_offset += count;
            size -= count;
        }
    }

    /*
     * Check for wrap arounds. so we start from the beginning
     */
    if(size > 0 && cur_offset >= end_offset)
    {
        cur_offset = offset;
    }
    /*
     * Align cur_offset to mmap chunk size
     */
    cur_offset &= ~IO_CACHE_MMAP_CHUNK_MASK;
    
    /*
     * Now bind the holes or gaps in the cache by filling the reclaim list (if any left)
     */
    while(size > 0 && !LIST_EMPTY(&cache_reclaim.r_list))
    {
        struct list_head *reclaim = cache_reclaim.r_list.next;
        cache_entry = list_entry(reclaim, io_cache_entry_t, e_inactive_list);
        log(LOG_DEBUG, "Mapping entry at offset [%lld] of size [%lld]\n", 
            (long long int)cur_offset, cache_entry->e_size);
        err = alloc_map(cache, cache_entry, cache_entry->e_size, cur_offset);
        if(err < 0)
            goto out_undo;
        err = -1;
        list_del(&cache_entry->e_inactive_list);
        --cache_reclaim.r_entries;
        cache_entry->e_flags &= ~IO_CACHE_LIST_INACTIVE;
        cache_entry->e_age = IO_CACHE_ENTRY_AGE;
        active_cache_add_locked(cache, cache_entry, 1);
        ++cache->c_stats.s_cur_entries;
        cache->c_stats.s_cur_size += cache_entry->e_size;
        if(cur_offset < offset)
            cur_offset = offset;
        count = MIN(cache_entry->e_offset + cache_entry->e_size - cur_offset, size);
        log(LOG_DEBUG, "Copying buffer to offset [%lld], from [%lld], count [%lld]\n",
            (long long int)(cur_offset - offset),
            (long long int)cur_offset, count);
        assert(cur_offset >= cache_entry->e_offset);
        memcpy(buffer + cur_offset - offset,
               cache_entry->e_data + cur_offset - cache_entry->e_offset,
               count);
        cur_offset += count;
        size -= count;
        IO_CACHE_MISS(cache, cache_entry);
    }

    cache->c_offset = end_offset;
    assert(size <= 0); /* we should have copied the entire buffer*/
    *len = ori_size;
    err = 0;

    out_undo:
    {
        long long i;
        for(i = 0; i < undo_list_entries; ++i)
        {
            cache_entry = undo_list[i];
            --cache_entry->e_refcnt;
            if(!cache_entry->e_refcnt)
                --cache->c_stats.s_locked_entries;
        }
    }

    free(undo_list);

    if(!LIST_EMPTY(&cache_reclaim.r_list))
    {
        log(LOG_WARNING, "Cache reclaim list not emptied. Marking them inactive\n");
        pthread_mutex_unlock(&cache->c_active_mutex);
        pthread_mutex_lock(&cache->c_inactive_mutex);
        list_splice(&cache_reclaim.r_list, &cache->c_inactive_list);
        cache->c_stats.s_inactive_entries += cache_reclaim.r_entries;
        pthread_mutex_unlock(&cache->c_inactive_mutex);
        pthread_mutex_lock(&cache->c_active_mutex);
    }

    out:
    return err;
}

int io_cache_read(io_cache_handle_t handle, char *buffer, loff_t offset, long long *len)
{
    int err = -1;
    io_cache_t *cache = handle;
    if(!cache) return err;
    pthread_mutex_lock(&cache->c_active_mutex);
    err = __io_cache_read(handle, buffer, offset, len);
    pthread_mutex_unlock(&cache->c_active_mutex);
    return err;
}

/*
 * Reads from the current cache offset maintained.
 */
int io_cache_read_extended(io_cache_handle_t handle, char *buffer, long long *len)
{
    int err = -1;
    io_cache_t *cache = handle;
    if(!cache) return err;
    pthread_mutex_lock(&cache->c_active_mutex);
    err = __io_cache_read(handle, buffer, cache->c_offset, len);
    pthread_mutex_unlock(&cache->c_active_mutex);
    return err;
}

/*
 * Change the offset into the cache or fetch from it. 
 * The caller should maintain atomicity w.r.t cache offset changing behind
 * the back after the invocation of this call but before the invocation of read
 */
int io_cache_offset_set(io_cache_handle_t handle, loff_t offset)
{
    io_cache_t *cache = handle;
    if(!cache || offset < 0 || offset >= cache->c_filesize) return -1;
    pthread_mutex_lock(&cache->c_active_mutex);
    cache->c_offset = offset;
    pthread_mutex_unlock(&cache->c_active_mutex);
    return 0;
}

int io_cache_offset_get(io_cache_handle_t handle, loff_t *offset)
{
    io_cache_t *cache = handle;
    if(!cache || !offset) return -1;
    pthread_mutex_lock(&cache->c_active_mutex);
    *offset = cache->c_offset;
    pthread_mutex_unlock(&cache->c_active_mutex);
    return 0;
}

static int __io_cache_entry_lock(io_cache_t *cache, loff_t offset, long long len, int ref)
{
    int err = -1;
    io_cache_entry_t *cache_entry;
    register struct list_head *iter;
    struct list_head *next;
    struct list_head *cache_intersect = NULL;
    loff_t end_offset, cur_offset, from_offset;
    long long size, ori_size, count = 0;
    io_cache_reclaim_t cache_reclaim = {0};
    io_cache_entry_t **undo_list;
    long long undo_list_entries = 0;

    if(!cache || offset < 0 || !len || !ref) 
        goto out;
    
    if(len > cache->c_stats.s_size || offset >= cache->c_filesize)
        goto out;

    if(offset + len > cache->c_filesize)
        len = cache->c_filesize - offset;

    end_offset = offset + len;
    cur_offset = offset;
    ori_size = size = len;

    LIST_HEAD_INIT(&cache_reclaim.r_list);
    undo_list = calloc(cache->c_stats.s_entries, sizeof(*undo_list)); /*the max it could grow*/
    assert(undo_list != NULL);
    pthread_mutex_lock(&cache->c_active_mutex);
    /*
     * For unlock, there should be NO holes in the range being unlocked as the memory map
     * should exist in the cache for that range.
     */
    if(ref < 0)
    {
        list_for_each(iter, &cache->c_active_list)
        {
            cache_entry = list_entry(iter, io_cache_entry_t, e_active_list);
            if(end_offset <= cache_entry->e_offset)
                break;
            if(cache_intersect)
            {
                cur_offset = cache_entry->e_offset;
            }
            else if(offset >= cache_entry->e_offset
                    &&
                    offset < cache_entry->e_offset + cache_entry->e_size)
            {
                cache_intersect = iter;
                cur_offset = offset;
            }
            else continue;

            if(cache_entry->e_refcnt == 0) /* already unlocked*/
            {
                log(LOG_WARNING, "Cache entry at offset [%lld] already unlocked\n",
                    (long long int)cache_entry->e_offset);
                goto next;
            }

            undo_list[undo_list_entries++] = cache_entry;
            cache_entry->e_refcnt += ref;
            if(cache_entry->e_refcnt == 0)
                --cache->c_stats.s_locked_entries;

            next:
            count = MIN(cache_entry->e_offset + cache_entry->e_size - cur_offset, size);
            size -= count;
            if(size <= 0)
            {
                pthread_mutex_unlock(&cache->c_active_mutex);
                err = 0;
                goto out_free;
            }
        }
        goto out_undo; /* undo the changes done to the references on failure */
    }
    /*
     * Here for lock
     */
    list_for_each(iter, &cache->c_active_list)
    {
        cache_entry = list_entry(iter, io_cache_entry_t, e_active_list);
        if(end_offset <= cache_entry->e_offset)
            break;
        if(cache_entry->e_offset + cache_entry->e_size <= offset)
            continue;
        if(offset >= cache_entry->e_offset
           &&
           offset < cache_entry->e_offset + cache_entry->e_size)
        {
            cache_intersect = iter;
            from_offset = cur_offset = offset;
        }
        else
        {
            if(!cache_intersect)
            {
                cache_intersect = iter;
                cur_offset = cache_entry->e_offset;
            }
            from_offset = cache_entry->e_offset;
        }
        count = MIN(cache_entry->e_offset + cache_entry->e_size - from_offset, size);
        if(count + from_offset > end_offset)
            count = end_offset - from_offset;
        size -= count;
        /*
         * Grab reference count for lock
         */
        undo_list[undo_list_entries++] = cache_entry;
        if(!cache_entry->e_refcnt)
            ++cache->c_stats.s_locked_entries;
        cache_entry->e_refcnt += ref;
        if(size <= 0)
        {
            pthread_mutex_unlock(&cache->c_active_mutex);
            err = 0;
            goto out_free;
        }
    }

    pthread_mutex_unlock(&cache->c_active_mutex);
    /*
     * Reclaim entries to pin them.
     */
    from_offset = offset & ~IO_CACHE_MMAP_CHUNK_MASK;
    size += offset - from_offset;
    size += IO_CACHE_MMAP_CHUNK_MASK;
    size &= ~IO_CACHE_MMAP_CHUNK_MASK;
    cache_reclaim.r_entries = size >> IO_CACHE_MMAP_CHUNK_SHIFT;
    err = io_cache_purge(cache, &cache_reclaim);
    pthread_mutex_lock(&cache->c_active_mutex);
    if(err < 0)
    {
        log_error("Cache entry lock failed to reclaim [%lld] entries for size [%lld] as cache looks hot\n",
                  cache_reclaim.r_entries, size);
        goto out_undo;
    }

    err = -1;
    size = ori_size;
    if(cache_intersect)
    {
        for(iter = cache_intersect; size > 0 && cur_offset < end_offset &&
                iter != &cache->c_active_list; iter = next)
        {
            next = iter->next;
            cache_entry = list_entry(iter, io_cache_entry_t, e_active_list);
            if(cur_offset < cache_entry->e_offset)
            {
                struct list_head *reclaim;
                assert(!LIST_EMPTY(&cache_reclaim.r_list));
                reclaim = cache_reclaim.r_list.next;
                cache_entry = list_entry(reclaim, io_cache_entry_t, e_inactive_list);
                err = alloc_map(cache, cache_entry, cache_entry->e_size, cur_offset);
                if(err < 0)
                    goto out_undo;
                err = -1;
                list_del(&cache_entry->e_inactive_list);
                --cache_reclaim.r_entries;
                cache_entry->e_flags &= ~IO_CACHE_LIST_INACTIVE;
                cache_entry->e_flags |= IO_CACHE_LIST_ACTIVE;
                cache_entry->e_age = IO_CACHE_ENTRY_AGE;
                list_add_tail(&cache_entry->e_active_list, iter);
                ++cache->c_stats.s_cur_entries;
                ++cache->c_stats.s_active_entries;
                cache->c_stats.s_cur_size += cache_entry->e_size;
                IO_CACHE_MISS(cache, cache_entry);
                undo_list[undo_list_entries++] = cache_entry;
                if(!cache_entry->e_refcnt)
                    ++cache->c_stats.s_locked_entries;
                cache_entry->e_refcnt += ref;
                next = iter; /*rescan the same entry*/
            }
            else
            {
                if(!cache_intersect)
                    assert(cache_entry->e_offset == cur_offset);
                else cache_intersect = NULL;
                IO_CACHE_HIT(cache, cache_entry);
            }
            count = MIN(cache_entry->e_offset + cache_entry->e_size - cur_offset, 
                        size);
            if(count + cur_offset > end_offset)
                count = end_offset - cur_offset;
            cur_offset += count;
            size -= count;
        }
    }

    if(size > 0 && cur_offset >= end_offset) /* wrap */
        cur_offset = offset;

    cur_offset &= ~IO_CACHE_MMAP_CHUNK_MASK; /*align offset to the mmap chunk size*/
    while(size > 0 && !LIST_EMPTY(&cache_reclaim.r_list))
    {
        struct list_head *reclaim = cache_reclaim.r_list.next;
        cache_entry = list_entry(reclaim, io_cache_entry_t, e_inactive_list);
        err = alloc_map(cache, cache_entry, cache_entry->e_size, cur_offset);
        if(err < 0)
            goto out_undo;
        err = -1;
        list_del(&cache_entry->e_inactive_list);
        --cache_reclaim.r_entries;
        cache_entry->e_flags &= ~IO_CACHE_LIST_INACTIVE;
        cache_entry->e_age = IO_CACHE_ENTRY_AGE;
        active_cache_add_locked(cache, cache_entry, 1); /* sort and add */
        undo_list[undo_list_entries++] = cache_entry;
        if(!cache_entry->e_refcnt)
            ++cache->c_stats.s_locked_entries;
        cache_entry->e_refcnt += ref;
        ++cache->c_stats.s_cur_entries;
        cache->c_stats.s_cur_size += cache_entry->e_size;
        IO_CACHE_MISS(cache, cache_entry);
        if(cur_offset < offset)
            cur_offset = offset;
        count = MIN(cache_entry->e_offset + cache_entry->e_size - cur_offset, size);
        cur_offset += count;
        size -= count;
    }
    err = 0;
    goto out_reclaim;
    
    /*
     * Undo the reference additions here.
     */
    out_undo:
    {
        long long i;
        for(i = 0; i < undo_list_entries; ++i)
        {
            cache_entry = undo_list[i];
            if(ref < 0 && !cache_entry->e_refcnt)
                ++cache->c_stats.s_locked_entries;
            cache_entry->e_refcnt -= ref;
            if(ref > 0 && !cache_entry->e_refcnt)
                --cache->c_stats.s_locked_entries;
        }
    }
    
    out_reclaim:
    pthread_mutex_unlock(&cache->c_active_mutex);
    if(!LIST_EMPTY(&cache_reclaim.r_list))
    {
        /*
         * Reclaim the unused entries back to the inactive list from the reclaim list.
         */
        pthread_mutex_lock(&cache->c_inactive_mutex);
        list_splice(&cache_reclaim.r_list, &cache->c_inactive_list);
        cache->c_stats.s_inactive_entries += cache_reclaim.r_entries;
        pthread_mutex_unlock(&cache->c_inactive_mutex);
    }

    out_free:
    free(undo_list);
    
    out:
    return err;
}

/*
 * Lock the entries in the cache to prevent the aging thread from claiming it.
 */
int io_cache_entry_lock(io_cache_handle_t handle, loff_t offset, long long len)
{
    return __io_cache_entry_lock((io_cache_t*)handle, offset, len, 1);
}

int io_cache_entry_unlock(io_cache_handle_t handle, loff_t offset, long long len)
{
    return __io_cache_entry_lock((io_cache_t*)handle, offset, len, -1);
}

int io_cache_stats_get(io_cache_handle_t handle, io_cache_stats_t *stats)
{
    io_cache_t *cache = handle;
    int err = -1;
    if(!cache || !stats)
        goto out;
    pthread_mutex_lock(&cache->c_active_mutex);
    memcpy(stats, &cache->c_stats, sizeof(*stats));
    pthread_mutex_unlock(&cache->c_active_mutex);
    err = 0;
    out:
    return err;
}

/*
 * Finalize the cache
 */
int io_cache_finalize(io_cache_handle_t *handle)
{
    int err = -1;
    io_cache_t *cache;
    DECLARE_LIST_HEAD(reclaim_list);

    if(!handle || !(cache = *handle))
        goto out;

    pthread_mutex_lock(&cache->c_aging_mutex);
    pthread_mutex_lock(&cache->c_active_mutex);
    if(!cache) 
    {
        pthread_mutex_unlock(&cache->c_active_mutex);
        pthread_mutex_unlock(&cache->c_aging_mutex);
        goto out;
    }
    /*
     * Check for locked references
     */
    if(cache->c_stats.s_locked_entries > 0)
    {
        pthread_mutex_unlock(&cache->c_active_mutex);
        pthread_mutex_unlock(&cache->c_aging_mutex);
        log_error("Cache has [%lld] locked entries. If you have specifically locked them,"
                  "then unlock the entries. "
                  "Otherwise wait for parallel cache reads to finish\n", cache->c_stats.s_locked_entries);
        goto out;
    }
    /*
     * Stop the aging task pool first after deactivating the cache
     */
    cache->c_state &= ~IO_CACHE_ACTIVE;
    *handle = NULL; /* remove reference to the cache, so we could clean up active/inactive entries */
    pthread_cond_signal(&cache->c_aging_cond);
    pthread_mutex_unlock(&cache->c_active_mutex);
    pthread_mutex_unlock(&cache->c_aging_mutex);
    task_pool_delete(cache->c_aging_pool); /* stop the aging cache pool*/
    /*
     * Now clean up the cache: Just reset the inactive list.
     */
    pthread_mutex_lock(&cache->c_inactive_mutex);
    LIST_HEAD_INIT(&cache->c_inactive_list); 
    pthread_mutex_unlock(&cache->c_inactive_mutex);

    pthread_mutex_lock(&cache->c_active_mutex);
    list_splice(&cache->c_active_list, &reclaim_list);
    pthread_mutex_unlock(&cache->c_active_mutex);

    while(!LIST_EMPTY(&reclaim_list))
    {
        io_cache_entry_t *cache_entry = list_entry(reclaim_list.next, io_cache_entry_t, e_active_list);
        assert(cache_entry->e_flags & IO_CACHE_LIST_ACTIVE);
        list_del(&cache_entry->e_active_list);
        free_map(cache, cache_entry);
    }

    task_pool_delete(cache->c_io_pool);
    pthread_mutex_destroy(&cache->c_active_mutex);
    pthread_mutex_destroy(&cache->c_inactive_mutex);
    pthread_mutex_destroy(&cache->c_aging_mutex);
    pthread_cond_destroy(&cache->c_aging_cond);
    free(cache->c_cache);
    free(cache);
    err = 0;

    out:
    return err;
}

void io_cache_log_level_set(int level)
{
    if(level > LOG_DEBUG ||  level < 0)
    {
        log(LOG_WARNING, "Unrecognized log level [%d]. Setting the default log level to DEBUG [%d]\n", 
            level, LOG_DEBUG);
        level = LOG_DEBUG;
    }
    io_cache_default_log_level = level;
}
