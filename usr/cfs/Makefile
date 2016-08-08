
#gcc -DHAVE_CONFIG_H -I.    -D_FILE_OFFSET_BITS=64 -I/usr/include/fuse  -g -O2 -MT bbfs.o -MD -MP -MF .deps/bbfs.Tpo -c -o bbfs.o bbfs.c
#mv -f .deps/bbfs.Tpo .deps/bbfs.Po
#gcc -DHAVE_CONFIG_H -I.    -D_FILE_OFFSET_BITS=64 -I/usr/include/fuse  -g -O2 -MT log.o -MD -MP -MF .deps/log.Tpo -c -o log.o log.c
#mv -f .deps/log.Tpo .deps/log.Po
#gcc -D_FILE_OFFSET_BITS=64 -I/usr/include/fuse  -g -O2   -o bbfs bbfs.o log.o -lfuse -pthread



CC=gcc

LIBS=-lpthread
CFLAGS=-std=gnu99 -Wall -ggdb3 -O0 -march=native
CFLAGS+=-D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE -DSELF_TEST -DDO_SELF_TEST

DEPS = log.h cfs_impl.h commiter.h buffer.h evict.h
OBJ = cfs.o cfs_test.o log.o commiter.o buffer.o chunk_buf.o evict.o blake2/blake2b.o

%.o:     %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

cfs:   $(OBJ) 
	$(CC) -o $@  $^  $(LIBS)

clean:
	rm -rf *.o cfs blake2/*.o

