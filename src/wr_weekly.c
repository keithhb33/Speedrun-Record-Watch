#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <curl/curl.h>
#include <cjson/cJSON.h>

typedef struct { char *data; size_t size; } Buffer;

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
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "wr-live-readme-bot/1.0 (libcurl)");
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "");

    for (int attempt = 0; attempt < 6; attempt++) {
        buf.size = 0;
        buf.data[0] = '\0';

        CURLcode res = curl_easy_perform(curl);

        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

        if (res == CURLE_OK && http_code >= 200 && http_code < 300) {
            return buf.data;
        }

        if (http_code == 429 || (http_code >= 500 && http_code < 600)) {
            usleep((useconds_t)(200000 * (attempt + 1)));
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

static int json_get_int(cJSON *obj, const char *key, int fallback) {
    cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (cJSON_IsNumber(v)) return v->valueint;
    return fallback;
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

static time_t parse_iso8601_utc(const char *s) {
    if (!s) return (time_t)-1;

    // Strip fractional seconds if present
    char tmp[64];
    memset(tmp, 0, sizeof(tmp));
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
    if (!strptime(s, "%Y-%m-%dT%H:%M:%SZ", &tmv)) return (time_t)-1;
    return timegm(&tmv);
}

/* ---------- category variable cache for subcategory labels ---------- */

typedef struct ValueMap {
    char *value_id;
    char *label;
    struct ValueMap *next;
} ValueMap;

typedef struct VarMap {
    char *var_id;
    char *var_name;
    ValueMap *values;
    struct VarMap *next;
} VarMap;

typedef struct CatVarCache {
    char *cat_id;
    VarMap *vars;
    struct CatVarCache *next;
} CatVarCache;

static ValueMap *valuemap_add(ValueMap *head, const char *id, const char *label) {
    ValueMap *n = calloc(1, sizeof(ValueMap));
    if (!n) return head;
    n->value_id = strdup(id ? id : "");
    n->label = strdup(label ? label : "");
    n->next = head;
    return n;
}

static VarMap *varmap_add(VarMap *head, const char *id, const char *name, ValueMap *values) {
    VarMap *n = calloc(1, sizeof(VarMap));
    if (!n) return head;
    n->var_id = strdup(id ? id : "");
    n->var_name = strdup(name ? name : "");
    n->values = values;
    n->next = head;
    return n;
}

static void free_valuemap(ValueMap *v) {
    while (v) {
        ValueMap *nx = v->next;
        free(v->value_id);
        free(v->label);
        free(v);
        v = nx;
    }
}

static void free_varmap(VarMap *v) {
    while (v) {
        VarMap *nx = v->next;
        free(v->var_id);
        free(v->var_name);
        free_valuemap(v->values);
        free(v);
        v = nx;
    }
}

static void free_cache(CatVarCache *c) {
    while (c) {
        CatVarCache *nx = c->next;
        free(c->cat_id);
        free_varmap(c->vars);
        free(c);
        c = nx;
    }
}

static const char *find_value_label(VarMap *vars, const char *var_id, const char *value_id, const char **var_name_out) {
    for (VarMap *v = vars; v; v = v->next) {
        if (strcmp(v->var_id, var_id) == 0) {
            if (var_name_out) *var_name_out = v->var_name;
            for (ValueMap *vm = v->values; vm; vm = vm->next) {
                if (strcmp(vm->value_id, value_id) == 0) return vm->label;
            }
            return NULL;
        }
    }
    return NULL;
}

static VarMap *load_category_vars(CURL *curl, const char *cat_id) {
    // Endpoint: /api/v1/categories/{id}/variables
    char url[512];
    snprintf(url, sizeof(url),
             "https://www.speedrun.com/api/v1/categories/%s/variables?max=200",
             cat_id);

    char *json = fetch_url(curl, url);
    if (!json) return NULL;

    cJSON *root = cJSON_Parse(json);
    free(json);
    if (!root) return NULL;

    cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");
    if (!cJSON_IsArray(data)) { cJSON_Delete(root); return NULL; }

    VarMap *vars = NULL;

    cJSON *var = NULL;
    cJSON_ArrayForEach(var, data) {
        const char *var_id = json_get_string(var, "id");
        const char *var_name = json_get_string(var, "name");
        if (!var_id) continue;

        ValueMap *values = NULL;

        // values.values is usually an object keyed by value_id with {label: "..."}
        cJSON *valuesObj = cJSON_GetObjectItemCaseSensitive(var, "values");
        cJSON *valuesValues = valuesObj ? cJSON_GetObjectItemCaseSensitive(valuesObj, "values") : NULL;

        if (cJSON_IsObject(valuesValues)) {
            cJSON *entry = NULL;
            cJSON_ArrayForEach(entry, valuesValues) {
                const char *value_id = entry->string;
                const char *label = NULL;
                if (cJSON_IsObject(entry)) label = json_get_string(entry, "label");
                if (value_id) values = valuemap_add(values, value_id, label ? label : value_id);
            }
        }

        vars = varmap_add(vars, var_id, var_name ? var_name : var_id, values);
    }

    cJSON_Delete(root);
    return vars;
}

static VarMap *get_cached_vars(CURL *curl, CatVarCache **cache, const char *cat_id) {
    for (CatVarCache *c = *cache; c; c = c->next) {
        if (strcmp(c->cat_id, cat_id) == 0) return c->vars;
    }
    VarMap *vars = load_category_vars(curl, cat_id);
    CatVarCache *n = calloc(1, sizeof(CatVarCache));
    if (!n) return vars;
    n->cat_id = strdup(cat_id);
    n->vars = vars;
    n->next = *cache;
    *cache = n;
    return vars;
}

static char *format_subcategories(CURL *curl, CatVarCache **cache, const char *cat_id, cJSON *valuesObj) {
    if (!cat_id || !cJSON_IsObject(valuesObj)) return strdup("");

    VarMap *vars = get_cached_vars(curl, cache, cat_id);
    if (!vars) return strdup("");

    // Build "Var: Value, Var2: Value2"
    size_t cap = 256;
    char *out = malloc(cap);
    if (!out) return strdup("");
    out[0] = '\0';
    size_t used = 0;
    int first = 1;

    cJSON *kv = NULL;
    cJSON_ArrayForEach(kv, valuesObj) {
        if (!cJSON_IsString(kv) || !kv->valuestring || !kv->string) continue;

        const char *var_name = NULL;
        const char *val_label = find_value_label(vars, kv->string, kv->valuestring, &var_name);
        if (!var_name) var_name = kv->string;
        if (!val_label) val_label = kv->valuestring;

        char chunk[512];
        snprintf(chunk, sizeof(chunk), "%s%s: %s", first ? "" : ", ", var_name, val_label);
        first = 0;

        size_t clen = strlen(chunk);
        if (used + clen + 1 > cap) {
            while (used + clen + 1 > cap) cap *= 2;
            char *tmp = realloc(out, cap);
            if (!tmp) break;
            out = tmp;
        }
        memcpy(out + used, chunk, clen);
        used += clen;
        out[used] = '\0';
    }

    return out;
}

/* ---------- id/name extraction for embedded fields ---------- */

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

            cJSON *names = cJSON_GetObjectItemCaseSensitive(data, "names");
            if (cJSON_IsObject(names)) {
                const char *intl = json_get_string(names, "international");
                if (intl) *name_out = intl;
            }
            const char *nm = json_get_string(data, "name");
            if (nm) *name_out = nm;
        }
    }
}

static void print_players_compact(cJSON *runObj, char *out, size_t outsz) {
    out[0] = '\0';
    cJSON *players = cJSON_GetObjectItemCaseSensitive(runObj, "players");

    if (cJSON_IsObject(players)) players = cJSON_GetObjectItemCaseSensitive(players, "data");
    if (!cJSON_IsArray(players)) return;

    size_t used = 0;
    int first = 1;

    cJSON *p = NULL;
    cJSON_ArrayForEach(p, players) {
        const char *name = json_get_string(p, "name");
        if (!name) {
            cJSON *names = cJSON_GetObjectItemCaseSensitive(p, "names");
            if (cJSON_IsObject(names)) name = json_get_string(names, "international");
        }
        if (!name) name = json_get_string(p, "id");
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

/* ---------- leaderboard check (#1 run match) ---------- */

static void build_leaderboard_url(char *out, size_t outsz,
                                 const char *gameId, const char *categoryId, const char *levelId,
                                 cJSON *valuesObj) {
    if (levelId && levelId[0]) {
        snprintf(out, outsz,
                 "https://www.speedrun.com/api/v1/leaderboards/%s/level/%s/%s?top=1",
                 gameId, levelId, categoryId);
    } else {
        snprintf(out, outsz,
                 "https://www.speedrun.com/api/v1/leaderboards/%s/category/%s?top=1",
                 gameId, categoryId);
    }

    if (cJSON_IsObject(valuesObj)) {
        cJSON *kv = NULL;
        cJSON_ArrayForEach(kv, valuesObj) {
            if (!cJSON_IsString(kv) || !kv->valuestring || !kv->string) continue;
            char tmp[2048];
            snprintf(tmp, sizeof(tmp), "%s&var-%s=%s", out, kv->string, kv->valuestring);
            strncpy(out, tmp, outsz - 1);
            out[outsz - 1] = '\0';
        }
    }
}

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

    cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");
    if (!cJSON_IsObject(data)) { cJSON_Delete(root); return 0; }

    cJSON *runs = cJSON_GetObjectItemCaseSensitive(data, "runs");
    if (!cJSON_IsArray(runs) || cJSON_GetArraySize(runs) < 1) { cJSON_Delete(root); return 0; }

    cJSON *first = cJSON_GetArrayItem(runs, 0);
    cJSON *runObj = first ? cJSON_GetObjectItemCaseSensitive(first, "run") : NULL;
    if (!cJSON_IsObject(runObj)) { cJSON_Delete(root); return 0; }

    const char *topId = json_get_string(runObj, "id");
    if (topId && runId && strcmp(topId, runId) == 0) ok = 1;

    cJSON_Delete(root);
    return ok;
}

/* ---------- rows + collection ---------- */

typedef struct {
    time_t verified_time;
    char *verified_iso;
    char *game;
    char *category;
    char *level;
    char *subcats;
    char *weblink;
    double primary_t;
    char players[512];
    char *run_id;
} Row;

static void free_row(Row *r) {
    free(r->verified_iso);
    free(r->game);
    free(r->category);
    free(r->level);
    free(r->subcats);
    free(r->weblink);
    free(r->run_id);
}

static int seen_run_id(char **seen, size_t seen_n, const char *id) {
    for (size_t i = 0; i < seen_n; i++) {
        if (seen[i] && id && strcmp(seen[i], id) == 0) return 1;
    }
    return 0;
}

static void add_row(Row **rows, size_t *n, size_t *cap, Row *r) {
    if (*n + 1 > *cap) {
        size_t ncap = (*cap == 0) ? 128 : (*cap * 2);
        Row *tmp = realloc(*rows, ncap * sizeof(Row));
        if (!tmp) return;
        *rows = tmp;
        *cap = ncap;
    }
    (*rows)[*n] = *r;
    (*n)++;
}

static void collect_current_wrs_since(CURL *curl, CatVarCache **cache,
                                      time_t cutoff, Row **rows_out, size_t *rows_n_out) {
    const int max = 200;
    int offset = 0;

    Row *rows = NULL;
    size_t n = 0, cap = 0;

    // de-dupe by run id (runs feed can contain repeats across pages in edge cases)
    char **seen = NULL;
    size_t seen_n = 0, seen_cap = 0;

    while (1) {
        char url[1024];
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

        for (int i = 0; i < page_n; i++) {
            cJSON *run = cJSON_GetArrayItem(data, i);
            if (!cJSON_IsObject(run)) continue;

            const char *runId = json_get_string(run, "id");
            if (!runId) continue;

            if (seen_run_id(seen, seen_n, runId)) continue;

            // add to seen
            if (seen_n + 1 > seen_cap) {
                size_t ncap = (seen_cap == 0) ? 512 : (seen_cap * 2);
                char **tmp = realloc(seen, ncap * sizeof(char*));
                if (!tmp) break;
                seen = tmp;
                seen_cap = ncap;
            }
            seen[seen_n++] = strdup(runId);

            const char *weblink = json_get_string(run, "weblink");

            const char *verify_date = NULL;
            cJSON *status = cJSON_GetObjectItemCaseSensitive(run, "status");
            if (cJSON_IsObject(status)) verify_date = json_get_string(status, "verify-date");

            time_t vtime = parse_iso8601_utc(verify_date);
            if (vtime == (time_t)-1) continue;

            if (vtime < cutoff) { stop = 1; break; }

            const char *gameId = NULL, *gameName = NULL;
            const char *catId  = NULL, *catName  = NULL;
            const char *levelId = NULL, *levelName = NULL;

            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "game"), &gameId, &gameName);
            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "category"), &catId, &catName);
            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "level"), &levelId, &levelName);

            if (!gameId || !catId) continue;

            // times.primary_t
            double primary_t = -1;
            cJSON *times = cJSON_GetObjectItemCaseSensitive(run, "times");
            if (cJSON_IsObject(times)) primary_t = json_get_number(times, "primary_t", -1);

            cJSON *valuesObj = cJSON_GetObjectItemCaseSensitive(run, "values");

            // only keep if current #1 on its exact leaderboard (including vars)
            if (!is_current_wr(curl, runId, gameId, catId, levelId, valuesObj)) {
                usleep(40000);
                continue;
            }

            char players[512];
            print_players_compact(run, players, sizeof(players));

            char *subcats = format_subcategories(curl, cache, catId, valuesObj);

            Row rr = {0};
            rr.verified_time = vtime;
            rr.verified_iso = strdup(verify_date ? verify_date : "");
            rr.game = strdup(gameName ? gameName : gameId);
            rr.category = strdup(catName ? catName : catId);
            rr.level = (levelId ? strdup(levelName ? levelName : levelId) : NULL);
            rr.subcats = subcats; // already allocated
            rr.weblink = strdup(weblink ? weblink : "");
            rr.primary_t = primary_t;
            strncpy(rr.players, players, sizeof(rr.players) - 1);
            rr.run_id = strdup(runId);

            add_row(&rows, &n, &cap, &rr);

            usleep(120000);
        }

        cJSON_Delete(root);

        if (stop) break;

        offset += page_n;
        if (page_n < max) break;
    }

    // free seen
    for (size_t i = 0; i < seen_n; i++) free(seen[i]);
    free(seen);

    *rows_out = rows;
    *rows_n_out = n;
}

static int cmp_row_newest_first(const void *a, const void *b) {
    const Row *ra = (const Row*)a;
    const Row *rb = (const Row*)b;
    if (ra->verified_time > rb->verified_time) return -1;
    if (ra->verified_time < rb->verified_time) return 1;
    return 0;
}

static void print_section(const char *title, Row *rows, size_t n) {
    printf("### %s\n\n", title);

    if (n == 0) {
        printf("_No current #1 records found in this window._\n\n");
        return;
    }

    printf("| Verified (UTC) | Game | Category | Subcategory | Level | Time | Runner(s) | Link |\n");
    printf("|---|---|---|---|---|---:|---|---|\n");

    for (size_t i = 0; i < n; i++) {
        char tbuf[64];
        format_seconds(rows[i].primary_t, tbuf, sizeof(tbuf));

        const char *lvl = rows[i].level ? rows[i].level : "";
        const char *sub = (rows[i].subcats && rows[i].subcats[0]) ? rows[i].subcats : "";

        printf("| %s | %s | %s | %s | %s | %s | %s | %s |\n",
               rows[i].verified_iso,
               rows[i].game,
               rows[i].category,
               sub,
               lvl,
               tbuf,
               rows[i].players[0] ? rows[i].players : "",
               rows[i].weblink && rows[i].weblink[0] ? rows[i].weblink : "");
    }
    printf("\n");
}

int main(void) {
    time_t now = time(NULL);

    time_t cutoff_1h  = now - 1 * 3600;
    time_t cutoff_24h = now - 24 * 3600;
    time_t cutoff_7d  = now - 7 * 24 * 3600;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL *curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "curl_easy_init failed\n");
        curl_global_cleanup();
        return 1;
    }

    CatVarCache *cache = NULL;

    // Collect once for 7 days (largest window), then split into 24h / 1h
    Row *all = NULL;
    size_t all_n = 0;
    collect_current_wrs_since(curl, &cache, cutoff_7d, &all, &all_n);

    // sort newest first
    qsort(all, all_n, sizeof(Row), cmp_row_newest_first);

    // partition counts
    size_t n_1h = 0, n_24h = 0, n_7d = all_n;
    for (size_t i = 0; i < all_n; i++) {
        if (all[i].verified_time >= cutoff_24h) n_24h++;
        if (all[i].verified_time >= cutoff_1h) n_1h++;
    }

    // Create slices (no copying deep strings; just shallow copy rows)
    Row *rows_1h = NULL, *rows_24h = NULL, *rows_7d = NULL;
    if (n_1h) rows_1h = malloc(n_1h * sizeof(Row));
    if (n_24h) rows_24h = malloc(n_24h * sizeof(Row));
    if (n_7d) rows_7d = malloc(n_7d * sizeof(Row));

    // Fill (since sorted newest-first, first n_1h are in 1h, first n_24h in 24h)
    for (size_t i = 0; i < n_1h; i++) rows_1h[i] = all[i];
    for (size_t i = 0; i < n_24h; i++) rows_24h[i] = all[i];
    for (size_t i = 0; i < n_7d; i++) rows_7d[i] = all[i];

    // Print combined markdown
    printf("## 🏁 Live #1 Records\n\n");
    printf("_Updated hourly via GitHub Actions._\n\n");

    print_section("Past hour", rows_1h, n_1h);
    print_section("Past 24 hours", rows_24h, n_24h);
    print_section("Past 7 days", rows_7d, n_7d);

    // cleanup slices
    free(rows_1h);
    free(rows_24h);
    free(rows_7d);

    // cleanup deep rows
    for (size_t i = 0; i < all_n; i++) free_row(&all[i]);
    free(all);

    free_cache(cache);

    curl_easy_cleanup(curl);
    curl_global_cleanup();
    return 0;
}