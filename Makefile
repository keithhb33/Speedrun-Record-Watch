CC := gcc
CFLAGS := -O2 -Wall -Wextra -std=c11
LDLIBS := -lcurl -lcjson

.PHONY: all clean run

all: wr_weekly

wr_weekly: src/wr_weekly.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

run: wr_weekly
	./wr_weekly > /tmp/wr_sections.md

clean:
	rm -f wr_weekly
