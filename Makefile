CC := gcc
CFLAGS := -O2 -Wall -Wextra -std=c11
LDLIBS := -lcurl -lcjson

.PHONY: all clean run

all: wr_weekly

wr_weekly: src/wr_weekly.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

run: wr_weekly
	./wr_weekly --days 7 --limit 50

clean:
	rm -f wr_weekly
