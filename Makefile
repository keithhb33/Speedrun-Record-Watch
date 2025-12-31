CC := gcc
CFLAGS := -O2 -Wall -Wextra -std=c11
LDLIBS := -lcurl -lcjson

.PHONY: all clean run

all: wr_daily

wr_daily: src/wr_daily.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

run: wr_daily
	./wr_daily > /tmp/wr_sections.md

clean:
	rm -f wr_daily
