/*
 * Test file for io-cache. Currently io-cache fails random cache reads coz of a memory corruption in the cache
 * active list.
 * Breaking my head on it as test: test_cache_read_1 fails.
 * Create a 1 GIG file like: dd if=/dev/zero of=test.out bs=1M count=1024
 * And then run the test as: ./test_iocache -f test.out -b 16m -c 256m -l debug
 * The above would run a test on the file test.out using 16 mb reads by configuring the io-cache
 * of 256 mb with log level debug.
 */

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <getopt.h>
#include <sys/param.h>
#include "io_cache.h"

static long long get_size(const char *s, long long default_size)
{
    int shift = 0;
    char *base = NULL;
    long long size;
    size = strtoll(s, &base, 10);
    if(*base)
    {
        int c = tolower(*base);
        if(c == 'k')
            shift = 10;
        else if(c == 'm')
            shift = 20;
        else if(c == 'g')
            shift = 30;
    }
    size <<= shift;
    if(!size) size = default_size;
    return size;
}

static void dump_io_cache_stats(io_cache_handle_t handle)
{
    int err;
    io_cache_stats_t stats = {0};
    err = io_cache_stats_get(handle, &stats);
    assert(err == 0);
    printf("IO cache size [%lld] bytes, cur_size [%lld] bytes, entries [%lld], cur_entries [%lld]\n"
           "IO cache active entries [%lld], inactive entries [%lld], locked entries [%lld]\n"
           "IO cache HITS [%lld], MISSES [%lld], hit rate [%.02f %%]\n",
           stats.s_size, stats.s_cur_size, stats.s_entries, stats.s_cur_entries,
           stats.s_active_entries, stats.s_inactive_entries, stats.s_locked_entries,
           stats.s_hits, stats.s_misses, (double)(stats.s_hits*100.0)/(stats.s_hits+stats.s_misses));
}

static void test_cache_read_extended(io_cache_handle_t handle, long long block_size)
{
    long long size = block_size;
    char *buffer = malloc(block_size);
    static char *hole;
    int err = 0;
    long long count = 0;
    assert(buffer);
    hole = calloc(1, block_size);
    assert(buffer && hole);
    memset(buffer, 0xa5, block_size);
    while( (err = io_cache_read_extended(handle, buffer, &size)) >= 0
           &&
           size > 0)
    {
        err = memcmp(buffer, hole, size);
        assert(err == 0);
        printf("Successly read chunk [%lld] of size [%lld]\n", count++, size);
        memset(buffer, 0xa5, size);
    }
    printf("Sucked in [%lld] chunks %s\n", count, err < 0 ? "after erroring out" : "successfully");
    if(err == 0)
    {
        assert(size == 0);
        dump_io_cache_stats(handle);
    }
    free(buffer);
}

/*
 * Exercise random cache reads from files 
 */
static void test_cache_read_1(io_cache_handle_t handle, long long block_size)
{
    loff_t offsets[] = { 12 << 20LL, 9 << 20LL, 13 << 20LL, /*101LL,*/ 37 << 20LL, 283<<20LL, 489<<20LL, 784<<20LL, 0, -1 } ;
    loff_t offset=0;
    register int i;
    int err;
    long long size = block_size;
    char *buffer = malloc(block_size);
    static char *hole;
    hole = calloc(1, block_size);
    assert(buffer && hole);
    memset(buffer, 0xa5, block_size);
    for( i = 0; (offset=offsets[i]) >= 0; ++i)
    {
        fprintf(stderr, "Testing cache read with offset [%lld] bytes\n", (long long int)offset);
        err = io_cache_read(handle, buffer, offset, &size);
        assert(err == 0);
        assert(size == block_size);
        err = memcmp(buffer, hole, size);
        assert(err == 0);
    }
    fprintf(stderr, "Test cache read 1 success...\n");
}

static char *prog;
static void usage(void)
{
    fprintf(stderr, "%s -f <filename> -b <read_block_size> -c <cache_size> -h | this usage\n",
            prog);
    exit(127);
}

static int str_to_level(const char *level)
{
    if(!strncasecmp(level, "emerg", 5))
        return LOG_EMERG;
    if(!strncasecmp(level, "alert", 5))
        return LOG_ALERT;
    if(!strncasecmp(level, "crit", 4))
        return LOG_CRIT;
    if(!strncasecmp(level, "error", 5))
        return LOG_ERR;
    if(!strncasecmp(level, "warning", 7))
        return LOG_WARNING;
    if(!strncasecmp(level, "notice", 6))
        return LOG_NOTICE;
    if(!strncasecmp(level, "info", 4))
        return LOG_INFO;
    if(!strncasecmp(level, "debug", 5))
        return LOG_DEBUG;
    return LOG_DEBUG;
}

int main(int argc, char **argv)
{
    io_cache_handle_t handle = 0;
    int err;
    int c;
    long long block_size = 16<<20LL;
    long long cache_size = 128<<20LL;
    char filename[PATH_MAX];
    char log_level[40];
    if( (prog = strrchr(argv[0], '/') ) )
        ++prog;
    else prog = argv[0];

    filename[0] = 0;
    log_level[0] = 0;
    strncat(log_level, "info", 4);
    opterr = 0;
    while( ( c = getopt(argc, argv, "f:b:c:l:h") ) != EOF )
        switch(c)
        {
        case 'f':
            strncat(filename, optarg, sizeof(filename)-1);
            break;
        case 'b': /* file read block size */
            block_size = get_size(optarg, block_size);
            break;
        case 'c':
            cache_size = get_size(optarg, cache_size);
            break;
        case 'l':
            log_level[0] = 0;
            strncat(log_level, optarg, sizeof(log_level)-1);
            break;
        case 'h':
        case '?':
        default:
            usage();
        }

    if(optind != argc)
        usage();

    io_cache_log_level_set(str_to_level(log_level));
    printf("IO cache test read buffer block size [%lld] bytes, cache_size [%lld] bytes\n", 
           block_size, cache_size);
    err = io_cache_initialize(filename, cache_size, NULL, &handle);
    assert(err == 0);
    test_cache_read_extended(handle, block_size);
    err = io_cache_offset_set(handle, 0); 
    assert(err == 0);
    test_cache_read_1(handle, block_size);
    err = io_cache_finalize(&handle);
    assert(err == 0);
    return 0;
}
