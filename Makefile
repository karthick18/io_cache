CC := gcc
CFLAGS := -Wall -g
ifeq ($(VALGRIND), 1)
	CFLAGS += -DVALGRIND
endif
LIB_SRCS := io_cache.c task_pool.c
LIB_OBJS := $(LIB_SRCS:%.c=%.o)
TST_SRCS := test_iocache.c
TST_OBJS := $(TST_SRCS:%.c=%.o)
LD_LIBS := -lrt -lpthread -L./. -liocache
TARGET := libiocache.a test_iocache

all: $(TARGET)

libiocache.a: $(LIB_OBJS)
	@ (\
	ar cr $@ $^;\
	ranlib $@;\
	)

test_iocache: $(TST_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LD_LIBS) 

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f *~ $(LIB_OBJS) $(TST_OBJS) $(TARGET)
