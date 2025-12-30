#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <curl/curl.h>
#include <cjson/cJSON.h>

typedef struct {
    char *data;
    size_t size;
} Buffer;

static size_t write_cb(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    Buffer *buf = (Buffer *)userp;

    char *ptr = realloc(buf->data, buf->size + realsize + 1);
    if (!ptr) return 0;

    buf->data = ptr;
    memcpy(buf->data + buf->size, contents, realsize);
    buf->size += realsize;
    buf->data[buf->size] = '\0';
    return realsize;
}

static char *fetch_url(CURL *curl, const char *url) {
    Buffer buf = {0};
    buf.data = malloc(1);
    buf.size = 0;
    if (!buf.data) return NULL;
    buf.data[0] = '\0';

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&buf);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "wr-weekly-bot/1.0 (libcurl)");
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, ""); // enable gzip/deflate if available

    for (int attempt = 0; attempt < 6; attempt++) {
        buf.size = 0;
        buf.data[0] = '\0';

        CURLcode res = curl_easy_perform(curl);

        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

        if (res == CURLE_OK && http_code >= 200 && http_code < 300) {
            return buf.data; // caller frees
        }

        // Backoff on 429 / 5xx
        if (http_code == 429 || (http_code >= 500 && http_code < 600)) {
            useconds_t backoff = (useconds_t)(200000 * (attempt + 1)); // 0.2s, 0.4s, ...
            usleep(backoff);
            continue;
        }

        break;
    }

    free(buf.data);
    return NULL;
}

static const char *json_get_string(cJSON *obj, const char *key) {
    cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (cJSON_IsString(v) && v->valuestring) return v->valuestring;
    return NULL;
}

static double json_get_number(cJSON *obj, const char *key, double fallback) {
    cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (cJSON_IsNumber(v)) return v->valuedouble;
    return fallback;
}

static void format_seconds(double sec, char *out, size_t outsz) {
    if (sec < 0) { snprintf(out, outsz, "?"); return; }
    long total = (long)(sec + 0.5);
    long h = total / 3600;
    long m = (total % 3600) / 60;
    long s = total % 60;
    if (h > 0) snprintf(out, outsz, "%ld:%02ld:%02ld", h, m, s);
    else snprintf(out, outsz, "%ld:%02ld", m, s);
}

/*
  Parse ISO8601 UTC like:
    2025-12-29T23:45:12Z
    2025-12-29T23:45:12.123Z
  Returns time_t (UTC) or (time_t)-1 on failure.
*/
static time_t parse_iso8601_utc(const char *s) {
    if (!s) return (time_t)-1;

    char tmp[64];
    memset(tmp, 0, sizeof(tmp));

    // Strip fractional seconds if present
    const char *dot = strchr(s, '.');
    if (dot) {
        size_t n = (size_t)(dot - s);
        if (n >= sizeof(tmp) - 2) return (time_t)-1;
        memcpy(tmp, s, n);
        tmp[n] = 'Z';
        tmp[n+1] = '\0';
        s = tmp;
    }

    struct tm tmv;
    memset(&tmv, 0, sizeof(tmv));

    // Expect trailing Z
    char *r = strptime(s, "%Y-%m-%dT%H:%M:%SZ", &tmv);
    if (!r) return (time_t)-1;

    // timegm converts tm in UTC to time_t
    return timegm(&tmv);
}

/*
  Speedrun.com embeds sometimes appear as:
    "game": "abcd1234"
  or:
    "game": { "data": { ... } }
  This helper returns:
   - id_out: the id (either from string or from embedded data.id)
   - name_out: a human-friendly name if embedded (names.international or name)
*/
static void extract_id_and_name(cJSON *field, const char **id_out, const char **name_out) {
    *id_out = NULL;
    *name_out = NULL;

    if (cJSON_IsString(field)) {
        *id_out = field->valuestring;
        return;
    }

    if (cJSON_IsObject(field)) {
        cJSON *data = cJSON_GetObjectItemCaseSensitive(field, "data");
        if (cJSON_IsObject(data)) {
            const char *id = json_get_string(data, "id");
            if (id) *id_out = id;

            // game names are under names.international
            cJSON *names = cJSON_GetObjectItemCaseSensitive(data, "names");
            if (cJSON_IsObject(names)) {
                const char *intl = json_get_string(names, "international");
                if (intl) *name_out = intl;
            }

            // category/level name is often "name"
            const char *nm = json_get_string(data, "name");
            if (nm) *name_out = nm;
        }
    }
}

static void print_players_compact(cJSON *runObj, char *out, size_t outsz) {
    out[0] = '\0';
    cJSON *players = cJSON_GetObjectItemCaseSensitive(runObj, "players");

    // runs normally have players as array; with some embeds you may see {data:[...]}
    if (cJSON_IsObject(players)) players = cJSON_GetObjectItemCaseSensitive(players, "data");
    if (!cJSON_IsArray(players)) return;

    size_t used = 0;
    int first = 1;

    cJSON *p = NULL;
    cJSON_ArrayForEach(p, players) {
        const char *name = json_get_string(p, "name"); // guests often
        if (!name) {
            // embedded user can have names.international
            cJSON *names = cJSON_GetObjectItemCaseSensitive(p, "names");
            if (cJSON_IsObject(names)) name = json_get_string(names, "international");
        }
        if (!name) {
            // fallback to id
            name = json_get_string(p, "id");
        }
        if (!name) name = "unknown";

        char chunk[256];
        snprintf(chunk, sizeof(chunk), "%s%s", first ? "" : ", ", name);
        first = 0;

        size_t clen = strlen(chunk);
        if (used + clen + 1 >= outsz) break;

        memcpy(out + used, chunk, clen);
        used += clen;
        out[used] = '\0';
    }
}

/*
  Build leaderboard URL with var- filters from run.values.
  If levelId != NULL, use /leaderboards/{game}/level/{level}/{category}
  Else use /leaderboards/{game}/category/{category}
*/
static void build_leaderboard_url(char *out, size_t outsz,
                                 const char *gameId, const char *categoryId, const char *levelId,
                                 cJSON *valuesObj /* may be NULL */) {
    if (levelId && levelId[0]) {
        snprintf(out, outsz,
                 "https://www.speedrun.com/api/v1/leaderboards/%s/level/%s/%s?top=1",
                 gameId, levelId, categoryId);
    } else {
        snprintf(out, outsz,
                 "https://www.speedrun.com/api/v1/leaderboards/%s/category/%s?top=1",
                 gameId, categoryId);
    }

    // Append variable filters: &var-<varId>=<valueId>
    if (cJSON_IsObject(valuesObj)) {
        cJSON *kv = NULL;
        cJSON_ArrayForEach(kv, valuesObj) {
            if (!cJSON_IsString(kv) || !kv->valuestring) continue;
            char tmp[1024];
            snprintf(tmp, sizeof(tmp), "%s&var-%s=%s", out, kv->string, kv->valuestring);
            strncpy(out, tmp, outsz - 1);
            out[outsz - 1] = '\0';
        }
    }
}

/*
  Returns 1 if runId matches the current top run on the leaderboard, else 0.
*/
static int is_current_wr(CURL *curl,
                         const char *runId,
                         const char *gameId,
                         const char *categoryId,
                         const char *levelId,
                         cJSON *valuesObj) {
    char url[2048];
    build_leaderboard_url(url, sizeof(url), gameId, categoryId, levelId, valuesObj);

    char *json = fetch_url(curl, url);
    if (!json) return 0;

    int ok = 0;
    cJSON *root = cJSON_Parse(json);
    free(json);
    if (!root) return 0;

    // leaderboard response is usually: { "data": { "runs": [ {place, run}, ... ] } }
    cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");
    if (!cJSON_IsObject(data)) { cJSON_Delete(root); return 0; }

    cJSON *runs = cJSON_GetObjectItemCaseSensitive(data, "runs");
    if (!cJSON_IsArray(runs) || cJSON_GetArraySize(runs) < 1) { cJSON_Delete(root); return 0; }

    cJSON *first = cJSON_GetArrayItem(runs, 0);
    if (!cJSON_IsObject(first)) { cJSON_Delete(root); return 0; }

    cJSON *runObj = cJSON_GetObjectItemCaseSensitive(first, "run");
    if (!cJSON_IsObject(runObj)) { cJSON_Delete(root); return 0; }

    const char *topId = json_get_string(runObj, "id");
    if (topId && runId && strcmp(topId, runId) == 0) ok = 1;

    cJSON_Delete(root);
    return ok;
}

typedef struct {
    const char *verify_date;   // points into parsed JSON memory (valid until we delete root)
    const char *game_name;
    const char *cat_name;
    const char *level_name;
    const char *weblink;
    double primary_t;
    char players[512];
} Row;

/*
  Collect up to `limit` WR rows from the newest verified runs within `cutoff`.
*/
static int collect_wr_rows(CURL *curl, int days, int limit, Row **rows_out) {
    *rows_out = NULL;
    if (limit <= 0) return 0;

    time_t now = time(NULL);
    time_t cutoff = now - (time_t)days * 24 * 3600;

    const int max = 200; // common max page size
    int offset = 0;

    Row *rows = calloc((size_t)limit, sizeof(Row));
    if (!rows) return 0;

    int count = 0;

    while (count < limit) {
        char url[1024];
        // Embed common fields so we can print names without extra calls.
        // (Seen in real-world bot usage: embed=game,category.variables,players,level)
        snprintf(url, sizeof(url),
                 "https://www.speedrun.com/api/v1/runs"
                 "?status=verified&orderby=verify-date&direction=desc"
                 "&embed=game,category,players,level"
                 "&max=%d&offset=%d",
                 max, offset);

        char *json = fetch_url(curl, url);
        if (!json) break;

        cJSON *root = cJSON_Parse(json);
        free(json);
        if (!root) break;

        cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");
        if (!cJSON_IsArray(data)) { cJSON_Delete(root); break; }

        int page_n = cJSON_GetArraySize(data);
        if (page_n <= 0) { cJSON_Delete(root); break; }

        int stop = 0;

        for (int i = 0; i < page_n && count < limit; i++) {
            cJSON *run = cJSON_GetArrayItem(data, i);
            if (!cJSON_IsObject(run)) continue;

            const char *runId = json_get_string(run, "id");
            const char *weblink = json_get_string(run, "weblink");

            // verify-date lives under status.verify-date typically
            const char *verify_date = NULL;
            cJSON *status = cJSON_GetObjectItemCaseSensitive(run, "status");
            if (cJSON_IsObject(status)) verify_date = json_get_string(status, "verify-date");

            time_t vtime = parse_iso8601_utc(verify_date);
            if (vtime == (time_t)-1) continue;

            if (vtime < cutoff) { stop = 1; break; }

            // game/category/level can be string or embedded object
            const char *gameId = NULL, *gameName = NULL;
            const char *catId = NULL,  *catName = NULL;
            const char *levelId = NULL, *levelName = NULL;

            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "game"), &gameId, &gameName);
            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "category"), &catId, &catName);
            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "level"), &levelId, &levelName);

            if (!gameId || !catId || !runId) continue;

            // times.primary_t
            double primary_t = -1;
            cJSON *times = cJSON_GetObjectItemCaseSensitive(run, "times");
            if (cJSON_IsObject(times)) primary_t = json_get_number(times, "primary_t", -1);

            // subcategory variable values
            cJSON *valuesObj = cJSON_GetObjectItemCaseSensitive(run, "values");

            // check if current WR
            if (!is_current_wr(curl, runId, gameId, catId, levelId, valuesObj)) {
                // throttle lightly anyway
                usleep(50000);
                continue;
            }

            // players
            char players[512];
            print_players_compact(run, players, sizeof(players));

            rows[count].verify_date = verify_date ? strdup(verify_date) : strdup("");
            rows[count].game_name   = gameName ? strdup(gameName) : strdup(gameId);
            rows[count].cat_name    = catName ? strdup(catName) : strdup(catId);
            rows[count].level_name  = (levelId && levelName) ? strdup(levelName) : (levelId ? strdup(levelId) : NULL);
            rows[count].weblink     = weblink ? strdup(weblink) : strdup("");
            rows[count].primary_t   = primary_t;
            strncpy(rows[count].players, players, sizeof(rows[count].players) - 1);

            count++;

            // polite throttle between leaderboard checks
            usleep(120000);
        }

        cJSON_Delete(root);

        if (stop) break;
        offset += page_n;
        if (page_n < max) break;
    }

    *rows_out = rows;
    return count;
}

static void free_rows(Row *rows, int n) {
    for (int i = 0; i < n; i++) {
        free((char*)rows[i].verify_date);
        free((char*)rows[i].game_name);
        free((char*)rows[i].cat_name);
        free((char*)rows[i].level_name);
        free((char*)rows[i].weblink);
    }
    free(rows);
}

static void print_markdown(Row *rows, int n, int days) {
    printf("### Current #1 records verified in the last %d days\n\n", days);

    if (n <= 0) {
        printf("_No current #1 records found in the last %d days (or API throttled)._ \n", days);
        return;
    }

    printf("| Verified (UTC) | Game | Category | Level | Time | Runner(s) | Link |\n");
    printf("|---|---|---|---|---:|---|---|\n");

    for (int i = 0; i < n; i++) {
        char tbuf[64];
        format_seconds(rows[i].primary_t, tbuf, sizeof(tbuf));

        const char *lvl = rows[i].level_name ? rows[i].level_name : "";
        const char *link = rows[i].weblink && rows[i].weblink[0] ? rows[i].weblink : "";

        // Keep table safe-ish (very simple escaping)
        printf("| %s | %s | %s | %s | %s | %s | %s |\n",
               rows[i].verify_date,
               rows[i].game_name,
               rows[i].cat_name,
               lvl,
               tbuf,
               rows[i].players[0] ? rows[i].players : "",
               link);
    }

    printf("\n_Last updated: %s UTC_\n", "via GitHub Actions");
}

static int parse_int_arg(const char *s, int fallback) {
    if (!s || !*s) return fallback;
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (!end || *end != '\0') return fallback;
    if (v < 1) return fallback;
    if (v > 3650) return fallback;
    return (int)v;
}

int main(int argc, char **argv) {
    int days = 7;
    int limit = 50;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--days") == 0 && i + 1 < argc) {
            days = parse_int_arg(argv[++i], days);
        } else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc) {
            limit = parse_int_arg(argv[++i], limit);
        } else {
            fprintf(stderr, "Usage: %s [--days N] [--limit N]\n", argv[0]);
            return 2;
        }
    }

    curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL *curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "curl_easy_init failed\n");
        curl_global_cleanup();
        return 1;
    }

    Row *rows = NULL;
    int n = collect_wr_rows(curl, days, limit, &rows);

    curl_easy_cleanup(curl);
    curl_global_cleanup();

    print_markdown(rows, n, days);
    free_rows(rows, n);
    return 0;
}