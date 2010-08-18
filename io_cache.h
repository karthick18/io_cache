#ifndef _IO_CACHE_H_
#define _IO_CACHE_H_

#include <stdio.h>
#include <sys/syslog.h>

#ifdef __cplusplus
extern "C" {
#endif

#define log(level, fmt, arg...) log_output(stdout, level, fmt, ##arg)
#define log_error(fmt, arg...)  log_output(stderr, LOG_ERR, fmt, ##arg)
#define log_file(file, level, fmt, arg...) log_output(file, level, fmt, ##arg)

typedef void * io_cache_handle_t;

typedef struct io_cache_stats
{
    long long s_size; /*cache size configured*/
    long long s_map_size; /* cache mmap chunk size*/
    long long s_cur_size; /* current size of the cache */
    long long s_entries ; /* entries configured in the cache */
    long long s_cur_entries; /* current entries in the cache */
    long long s_active_entries; /*entries in the active list*/
    long long s_inactive_entries; /*entries in the inactive list*/
    long long s_locked_entries; /*entries locked*/
    long long s_hits;
    long long s_misses;
    time_t s_scan_time; /*last scan timestamp*/
} io_cache_stats_t;

extern int io_cache_initialize(const char *filename, unsigned long long cache_size,
                               void (*read_callback)(char *data, loff_t offset, long long size),
                               io_cache_handle_t *handle);
extern int io_cache_read(io_cache_handle_t handle, char *buffer, loff_t offset, long long *len);
extern int io_cache_read_extended(io_cache_handle_t handle, char *buffer, long long *len);
extern int io_cache_offset_set(io_cache_handle_t handle, loff_t offset);
extern int io_cache_offset_get(io_cache_handle_t handle, loff_t *offset);
extern int io_cache_entry_lock(io_cache_handle_t handle, loff_t offset, long long len);
extern int io_cache_entry_unlock(io_cache_handle_t handle, loff_t offset, long long len);
extern int io_cache_finalize(io_cache_handle_t *handle);
extern int io_cache_stats_get(io_cache_handle_t handle, io_cache_stats_t *stats);
extern void io_cache_log_level_set(int level);
extern void log_output(FILE *fptr, int level, const char *fmt, ...) __attribute__((format(printf, 3, 4)));

#ifdef __cplusplus
}
#endif

#endif
