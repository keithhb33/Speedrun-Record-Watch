#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <stdint.h>
#include <math.h>

#include <curl/curl.h>
#include <cjson/cJSON.h>

typedef struct { char *data; size_t size; } Buffer;

/* ----------------- debug / logging ----------------- */

static int g_debug = 1; // set DEBUG=0 in env to silence

static void log_ts(FILE *fp) {
    time_t t = time(NULL);
    struct tm tmv;
    gmtime_r(&t, &tmv);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tmv);
    fprintf(fp, "%sZ ", buf);
}

#define LOG(fmt, ...) do { \
    if (g_debug) { \
        log_ts(stderr); \
        fprintf(stderr, "[dbg] " fmt "\n", ##__VA_ARGS__); \
        fflush(stderr); \
    } \
} while (0)

static void init_debug_from_env(void) {
    const char *v = getenv("DEBUG");
    if (!v) return;
    if (strcmp(v, "0") == 0 || strcasecmp(v, "false") == 0 || strcasecmp(v, "no") == 0) g_debug = 0;
}

/* ----------------- http helpers ----------------- */

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
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "wr-live-readme-bot/2.1 (libcurl)");
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "");

    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 20L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);

    for (int attempt = 0; attempt < 6; attempt++) {
        buf.size = 0;
        buf.data[0] = '\0';

        clock_t c0 = clock();
        CURLcode res = curl_easy_perform(curl);
        clock_t c1 = clock();
        double elapsed = (double)(c1 - c0) / (double)CLOCKS_PER_SEC;

        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

        if (res == CURLE_OK && http_code >= 200 && http_code < 300) {
            LOG("HTTP %ld in %.2fs (%zu bytes): %s", http_code, elapsed, buf.size, url);
            return buf.data;
        }

        LOG("HTTP FAIL attempt=%d res=%d (%s) code=%ld in %.2fs: %s",
            attempt + 1, (int)res, curl_easy_strerror(res), http_code, elapsed, url);

        if (http_code == 429 || (http_code >= 500 && http_code < 600)) {
            usleep((useconds_t)(200000 * (attempt + 1)));
            continue;
        }
        break;
    }

    free(buf.data);
    return NULL;
}

/* ----------------- json helpers ----------------- */

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

static long json_get_long(cJSON *obj, const char *key, long fallback) {
    cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (cJSON_IsNumber(v)) return (long)v->valuedouble;
    return fallback;
}

/* ----------------- time helpers ----------------- */

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

static void format_iso_utc(time_t t, char *out, size_t outsz) {
    if (!out || outsz == 0) return;
    struct tm tmv;
    gmtime_r(&t, &tmv);
    strftime(out, outsz, "%Y-%m-%dT%H:%M:%SZ", &tmv);
}

/* --- README timestamp formatting in Eastern Time (ET; shows EST/EDT) --- */
static void init_tz_eastern(void) {
    setenv("TZ", "America/New_York", 1);
    tzset();
}

static void format_pretty_et(time_t t, char *out, size_t outsz) {
    if (!out || outsz == 0) return;
    struct tm tmv;
    localtime_r(&t, &tmv);
    // Example: "Dec 31, 2025 11:03 PM EST"
    strftime(out, outsz, "%b %d, %Y %I:%M %p %Z", &tmv);
}

/* ----------------- fs helpers ----------------- */

static int ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) return 1;
    if (mkdir(path, 0755) == 0) return 1;
    return 0;
}

static char *read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return NULL; }
    long n = ftell(f);
    if (n < 0) { fclose(f); return NULL; }
    rewind(f);

    char *buf = malloc((size_t)n + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t r = fread(buf, 1, (size_t)n, f);
    fclose(f);
    buf[r] = '\0';
    return buf;
}

static int write_file(const char *path, const char *data) {
    FILE *f = fopen(path, "wb");
    if (!f) return 0;
    size_t n = strlen(data);
    if (fwrite(data, 1, n, f) != n) { fclose(f); return 0; }
    fclose(f);
    return 1;
}

/* ----------------- fast string hash set (run_id + processed keys) ----------------- */

typedef struct StrSet {
    char **keys;
    size_t cap;
    size_t len;
} StrSet;

static uint64_t fnv1a_64(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    for (const unsigned char *p = (const unsigned char*)s; *p; p++) {
        h ^= (uint64_t)(*p);
        h *= 1099511628211ULL;
    }
    return h;
}

static int strset_init(StrSet *s, size_t initial_cap) {
    if (!s) return 0;
    size_t cap = 1;
    while (cap < initial_cap) cap <<= 1;
    s->keys = calloc(cap, sizeof(char*));
    if (!s->keys) return 0;
    s->cap = cap;
    s->len = 0;
    return 1;
}

static void strset_free(StrSet *s) {
    if (!s || !s->keys) return;
    for (size_t i = 0; i < s->cap; i++) free(s->keys[i]);
    free(s->keys);
    s->keys = NULL;
    s->cap = 0;
    s->len = 0;
}

static int strset_rehash(StrSet *s, size_t newcap) {
    StrSet ns = {0};
    if (!strset_init(&ns, newcap)) return 0;

    for (size_t i = 0; i < s->cap; i++) {
        char *k = s->keys[i];
        if (!k) continue;
        uint64_t h = fnv1a_64(k);
        size_t mask = ns.cap - 1;
        size_t idx = (size_t)h & mask;
        while (ns.keys[idx]) idx = (idx + 1) & mask;
        ns.keys[idx] = k; // move ownership
        ns.len++;
        s->keys[i] = NULL;
    }

    free(s->keys);
    *s = ns;
    return 1;
}

static int strset_has(const StrSet *s, const char *key) {
    if (!s || !s->keys || !key) return 0;
    uint64_t h = fnv1a_64(key);
    size_t mask = s->cap - 1;
    size_t idx = (size_t)h & mask;
    for (size_t probe = 0; probe < s->cap; probe++) {
        char *k = s->keys[idx];
        if (!k) return 0;
        if (strcmp(k, key) == 0) return 1;
        idx = (idx + 1) & mask;
    }
    return 0;
}

static int strset_add(StrSet *s, const char *key) {
    if (!s || !s->keys || !key) return 0;
    if (s->len * 10 >= s->cap * 7) { // > 0.7 load
        if (!strset_rehash(s, s->cap * 2)) return 0;
    }
    uint64_t h = fnv1a_64(key);
    size_t mask = s->cap - 1;
    size_t idx = (size_t)h & mask;
    while (s->keys[idx]) {
        if (strcmp(s->keys[idx], key) == 0) return 1; // already
        idx = (idx + 1) & mask;
    }
    s->keys[idx] = strdup(key);
    if (!s->keys[idx]) return 0;
    s->len++;
    return 1;
}

/* ----------------- category variable cache for subcategory labels ----------------- */

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
    LOG("Fetch category variables: cat_id=%s", cat_id ? cat_id : "(null)");

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
        if (strcmp(c->cat_id, cat_id) == 0) {
            return c->vars;
        }
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

/* ----------------- embedded id/name extraction ----------------- */

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

/* ----------------- leaderboard top-1 cache (in-memory) ----------------- */

typedef struct LbCache {
    char *key;
    char *top_run_id;
    struct LbCache *next;
} LbCache;

static void free_lb_cache(LbCache *c) {
    while (c) {
        LbCache *nx = c->next;
        free(c->key);
        free(c->top_run_id);
        free(c);
        c = nx;
    }
}

static void build_leaderboard_url_top(char *out, size_t outsz,
                                     const char *gameId, const char *categoryId, const char *levelId,
                                     cJSON *valuesObj, int topN) {
    if (outsz == 0) return;
    out[0] = '\0';

    int written = 0;
    if (levelId && levelId[0]) {
        written = snprintf(out, outsz,
                           "https://www.speedrun.com/api/v1/leaderboards/%s/level/%s/%s?top=%d",
                           gameId, levelId, categoryId, topN);
    } else {
        written = snprintf(out, outsz,
                           "https://www.speedrun.com/api/v1/leaderboards/%s/category/%s?top=%d",
                           gameId, categoryId, topN);
    }
    if (written < 0 || (size_t)written >= outsz) { out[outsz - 1] = '\0'; return; }

    size_t used = (size_t)written;

    if (cJSON_IsObject(valuesObj)) {
        cJSON *kv = NULL;
        cJSON_ArrayForEach(kv, valuesObj) {
            if (!cJSON_IsString(kv) || !kv->valuestring || !kv->string) continue;

            int add = snprintf(out + used, outsz - used, "&var-%s=%s", kv->string, kv->valuestring);
            if (add < 0) break;
            if ((size_t)add >= outsz - used) { out[outsz - 1] = '\0'; break; }
            used += (size_t)add;
        }
    }
}

typedef struct KVPair { const char *k; const char *v; } KVPair;

static int kv_cmp(const void *a, const void *b) {
    const KVPair *ka = (const KVPair*)a;
    const KVPair *kb = (const KVPair*)b;
    return strcmp(ka->k, kb->k);
}

// Canonical leaderboard key: game|cat|level|k=v&k=v...
static char *make_lb_key(const char *gameId, const char *catId, const char *levelId, cJSON *valuesObj) {
    size_t cap = 2048;
    char *buf = malloc(cap);
    if (!buf) return NULL;

    snprintf(buf, cap, "%s|%s|%s|", gameId ? gameId : "", catId ? catId : "", levelId ? levelId : "");

    int n = 0;
    if (cJSON_IsObject(valuesObj)) {
        cJSON *kv = NULL;
        cJSON_ArrayForEach(kv, valuesObj) {
            if (kv->string && cJSON_IsString(kv) && kv->valuestring) n++;
        }
    }

    KVPair *pairs = NULL;
    if (n > 0) {
        pairs = calloc((size_t)n, sizeof(KVPair));
        if (!pairs) { free(buf); return NULL; }

        int i = 0;
        cJSON *kv = NULL;
        cJSON_ArrayForEach(kv, valuesObj) {
            if (kv->string && cJSON_IsString(kv) && kv->valuestring) {
                pairs[i].k = kv->string;
                pairs[i].v = kv->valuestring;
                i++;
            }
        }
        qsort(pairs, (size_t)n, sizeof(KVPair), kv_cmp);
    }

    size_t used = strlen(buf);
    for (int i = 0; i < n; i++) {
        char chunk[256];
        snprintf(chunk, sizeof(chunk), "%s=%s&", pairs[i].k, pairs[i].v);
        size_t clen = strlen(chunk);
        if (used + clen + 1 > cap) {
            while (used + clen + 1 > cap) cap *= 2;
            char *tmp = realloc(buf, cap);
            if (!tmp) break;
            buf = tmp;
        }
        memcpy(buf + used, chunk, clen);
        used += clen;
        buf[used] = '\0';
    }

    free(pairs);
    return buf;
}

static const char *lb_cache_get(LbCache *cache, const char *key) {
    for (LbCache *c = cache; c; c = c->next) {
        if (strcmp(c->key, key) == 0) return c->top_run_id;
    }
    return NULL;
}

static void lb_cache_put(LbCache **cache, const char *key, const char *top_run_id) {
    LbCache *n = calloc(1, sizeof(LbCache));
    if (!n) return;
    n->key = strdup(key ? key : "");
    n->top_run_id = strdup(top_run_id ? top_run_id : "");
    n->next = *cache;
    *cache = n;
}

static const char *fetch_top1_run_id(CURL *curl,
                                     LbCache **cache,
                                     const char *gameId,
                                     const char *catId,
                                     const char *levelId,
                                     cJSON *valuesObj) {
    static long s_lb_calls = 0;
    s_lb_calls++;

    char *key = make_lb_key(gameId, catId, levelId, valuesObj);
    if (!key) return NULL;

    const char *cached = lb_cache_get(*cache, key);
    if (cached) {
        free(key);
        return cached;
    }

    char url[2048];
    build_leaderboard_url_top(url, sizeof(url), gameId, catId, levelId, valuesObj, 1);

    char *json = fetch_url(curl, url);
    if (!json) { free(key); return NULL; }

    cJSON *root = cJSON_Parse(json);
    free(json);
    if (!root) { free(key); return NULL; }

    const char *topId = NULL;
    cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");
    cJSON *runs = data ? cJSON_GetObjectItemCaseSensitive(data, "runs") : NULL;
    if (cJSON_IsArray(runs) && cJSON_GetArraySize(runs) > 0) {
        cJSON *first = cJSON_GetArrayItem(runs, 0);
        cJSON *runObj = first ? cJSON_GetObjectItemCaseSensitive(first, "run") : NULL;
        if (cJSON_IsObject(runObj)) topId = json_get_string(runObj, "id");
    }

    if (topId) {
        lb_cache_put(cache, key, topId);
        const char *ret = lb_cache_get(*cache, key);
        cJSON_Delete(root);
        free(key);
        return ret;
    }

    cJSON_Delete(root);
    free(key);
    return NULL;
}

static int is_current_wr(CURL *curl, LbCache **cache,
                         const char *runId,
                         const char *gameId,
                         const char *catId,
                         const char *levelId,
                         cJSON *valuesObj) {
    const char *topId = fetch_top1_run_id(curl, cache, gameId, catId, levelId, valuesObj);
    if (!topId || !runId) return 0;
    return strcmp(topId, runId) == 0;
}

/* ----------------- persistence ----------------- */

static long load_last_seen_epoch(void) {
    char *txt = read_file("data/state.json");
    if (!txt) return 0;
    cJSON *root = cJSON_Parse(txt);
    free(txt);
    if (!root) return 0;
    long v = json_get_long(root, "last_seen_epoch", 0);
    cJSON_Delete(root);
    if (v < 0) v = 0;
    return v;
}

static void save_last_seen_epoch(long last_seen_epoch) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "last_seen_epoch", (double)last_seen_epoch);

    char *out = cJSON_Print(root);
    cJSON_Delete(root);
    if (!out) return;

    write_file("data/state.json", out);
    free(out);
}

static cJSON *load_wrs_array(void) {
    char *txt = read_file("data/wrs.json");
    if (!txt) return cJSON_CreateArray();

    cJSON *arr = cJSON_Parse(txt);
    free(txt);
    if (!arr || !cJSON_IsArray(arr)) {
        if (arr) cJSON_Delete(arr);
        return cJSON_CreateArray();
    }
    return arr;
}

static void save_wrs_array(cJSON *arr) {
    char *out = cJSON_Print(arr);
    if (!out) return;
    write_file("data/wrs.json", out);
    free(out);
}

static void prune_old_wrs(cJSON *arr, time_t cutoff_epoch) {
    if (!cJSON_IsArray(arr)) return;

    for (int i = cJSON_GetArraySize(arr) - 1; i >= 0; i--) {
        cJSON *it = cJSON_GetArrayItem(arr, i);
        if (!cJSON_IsObject(it)) { cJSON_DeleteItemFromArray(arr, i); continue; }
        long v = json_get_long(it, "verified_epoch", 0);
        if (v < (long)cutoff_epoch) cJSON_DeleteItemFromArray(arr, i);
    }
}

static int wr_cmp_newest_first(const void *a, const void *b) {
    const cJSON *oa = *(const cJSON* const*)a;
    const cJSON *ob = *(const cJSON* const*)b;
    long ta = json_get_long((cJSON*)oa, "verified_epoch", 0);
    long tb = json_get_long((cJSON*)ob, "verified_epoch", 0);
    if (ta > tb) return -1;
    if (ta < tb) return 1;
    return 0;
}

static cJSON *sorted_wrs_dup(cJSON *arr) {
    if (!cJSON_IsArray(arr)) return cJSON_CreateArray();
    int n = cJSON_GetArraySize(arr);
    if (n <= 1) return cJSON_Duplicate(arr, 1);

    cJSON **items = calloc((size_t)n, sizeof(cJSON*));
    if (!items) return cJSON_Duplicate(arr, 1);

    for (int i = 0; i < n; i++) items[i] = cJSON_GetArrayItem(arr, i);
    qsort(items, (size_t)n, sizeof(cJSON*), wr_cmp_newest_first);

    cJSON *out = cJSON_CreateArray();
    for (int i = 0; i < n; i++) cJSON_AddItemToArray(out, cJSON_Duplicate(items[i], 1));

    free(items);
    return out;
}

/* ----------------- add WR entry (no current-WR requirement) ----------------- */

static void add_wr_entry_from_run(CURL *curl, CatVarCache **catCache,
                                 cJSON *wrs, StrSet *runIds,
                                 cJSON *run,
                                 long verified_epoch,
                                 const char *verify_date) {
    const char *runId = json_get_string(run, "id");
    if (!runId) return;
    if (strset_has(runIds, runId)) return;

    const char *weblink = json_get_string(run, "weblink");

    const char *gameId = NULL, *gameName = NULL;
    const char *catId  = NULL, *catName  = NULL;
    const char *levelId = NULL, *levelName = NULL;

    extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "game"), &gameId, &gameName);
    extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "category"), &catId, &catName);
    extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "level"), &levelId, &levelName);

    if (!gameId || !catId) return;

    double primary_t = -1;
    cJSON *times = cJSON_GetObjectItemCaseSensitive(run, "times");
    if (cJSON_IsObject(times)) primary_t = json_get_number(times, "primary_t", -1);

    cJSON *valuesObj = cJSON_GetObjectItemCaseSensitive(run, "values");

    char players[512];
    print_players_compact(run, players, sizeof(players));

    char *subcats = format_subcategories(curl, catCache, catId, valuesObj);

    cJSON *obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "run_id", runId);
    cJSON_AddNumberToObject(obj, "verified_epoch", (double)verified_epoch);
    cJSON_AddStringToObject(obj, "verified_iso", verify_date ? verify_date : "");
    cJSON_AddStringToObject(obj, "game", gameName ? gameName : gameId);
    cJSON_AddStringToObject(obj, "category", catName ? catName : catId);
    cJSON_AddStringToObject(obj, "level", (levelId ? (levelName ? levelName : levelId) : ""));
    cJSON_AddStringToObject(obj, "subcats", subcats ? subcats : "");
    cJSON_AddNumberToObject(obj, "primary_t", primary_t);
    cJSON_AddStringToObject(obj, "players", players);
    cJSON_AddStringToObject(obj, "weblink", weblink ? weblink : "");

    free(subcats);

    cJSON_AddItemToArray(wrs, obj);
    strset_add(runIds, runId);
}

/* ----------------- fetch run details by id ----------------- */

static cJSON *fetch_run_details(CURL *curl, const char *run_id, int embed) {
    if (!run_id || !run_id[0]) return NULL;

    char url[512];
    if (embed) {
        snprintf(url, sizeof(url),
                 "https://www.speedrun.com/api/v1/runs/%s?embed=game,category,players,level",
                 run_id);
    } else {
        snprintf(url, sizeof(url),
                 "https://www.speedrun.com/api/v1/runs/%s",
                 run_id);
    }

    char *json = fetch_url(curl, url);
    if (!json) return NULL;

    cJSON *root = cJSON_Parse(json);
    free(json);
    if (!root) return NULL;

    cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");
    if (!cJSON_IsObject(data)) { cJSON_Delete(root); return NULL; }

    cJSON *dup = cJSON_Duplicate(data, 1);
    cJSON_Delete(root);
    return dup;
}

static int get_run_verify_epoch_and_iso(cJSON *runObj, long *epoch_out, const char **iso_out) {
    if (epoch_out) *epoch_out = 0;
    if (iso_out) *iso_out = NULL;

    cJSON *status = cJSON_GetObjectItemCaseSensitive(runObj, "status");
    const char *verify_date = NULL;
    if (cJSON_IsObject(status)) verify_date = json_get_string(status, "verify-date");
    if (!verify_date) return 0;

    time_t vtime = parse_iso8601_utc(verify_date);
    if (vtime == (time_t)-1) return 0;

    if (epoch_out) *epoch_out = (long)vtime;
    if (iso_out) *iso_out = verify_date;
    return 1;
}

/* ----------------- record history reconstruction per leaderboard key ----------------- */

typedef struct LbRunInfo {
    char *run_id;
    double primary_t;
    long verified_epoch;     // 0 if unknown
} LbRunInfo;

static void free_lbruninfos(LbRunInfo *a, int n) {
    if (!a) return;
    for (int i = 0; i < n; i++) free(a[i].run_id);
    free(a);
}

static int lbrun_cmp_epoch_asc(const void *a, const void *b) {
    const LbRunInfo *ra = (const LbRunInfo*)a;
    const LbRunInfo *rb = (const LbRunInfo*)b;
    if (ra->verified_epoch < rb->verified_epoch) return -1;
    if (ra->verified_epoch > rb->verified_epoch) return 1;
    return 0;
}

static void track_leaderboard_history(CURL *curl, CatVarCache **catCache,
                                      cJSON *wrs, StrSet *runIds,
                                      const char *gameId, const char *catId, const char *levelId, cJSON *valuesObj,
                                      time_t cutoff_epoch) {
    const int TOPN = 200;
    char url[2048];
    build_leaderboard_url_top(url, sizeof(url), gameId, catId, levelId, valuesObj, TOPN);

    char *json = fetch_url(curl, url);
    if (!json) return;

    cJSON *root = cJSON_Parse(json);
    free(json);
    if (!root) return;

    cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");
    cJSON *runs = data ? cJSON_GetObjectItemCaseSensitive(data, "runs") : NULL;
    if (!cJSON_IsArray(runs)) { cJSON_Delete(root); return; }

    int n_entries = cJSON_GetArraySize(runs);
    if (n_entries <= 0) { cJSON_Delete(root); return; }

    LbRunInfo *infos = calloc((size_t)n_entries, sizeof(LbRunInfo));
    if (!infos) { cJSON_Delete(root); return; }

    int n = 0;
    for (int i = 0; i < n_entries; i++) {
        cJSON *entry = cJSON_GetArrayItem(runs, i);
        if (!cJSON_IsObject(entry)) continue;

        cJSON *runObj = cJSON_GetObjectItemCaseSensitive(entry, "run");
        if (!cJSON_IsObject(runObj)) continue;

        const char *rid = json_get_string(runObj, "id");
        if (!rid) continue;

        double pt = -1;
        cJSON *times = cJSON_GetObjectItemCaseSensitive(runObj, "times");
        if (cJSON_IsObject(times)) pt = json_get_number(times, "primary_t", -1);
        if (pt < 0) continue;

        long ve = 0;
        const char *iso = NULL;
        if (get_run_verify_epoch_and_iso(runObj, &ve, &iso)) {
            // ok
        } else {
            // Some leaderboard payloads omit verify-date. We'll fill selectively later.
            ve = 0;
        }

        infos[n].run_id = strdup(rid);
        infos[n].primary_t = pt;
        infos[n].verified_epoch = ve;
        n++;
    }

    cJSON_Delete(root);

    if (n == 0) { free(infos); return; }

    // Fill missing verify_epoch using /runs/{id} (no embed) for only those missing.
    for (int i = 0; i < n; i++) {
        if (infos[i].verified_epoch != 0) continue;
        cJSON *runBare = fetch_run_details(curl, infos[i].run_id, 0);
        if (!runBare) continue;

        long ve = 0;
        const char *iso = NULL;
        if (get_run_verify_epoch_and_iso(runBare, &ve, &iso)) {
            infos[i].verified_epoch = ve;
        }
        cJSON_Delete(runBare);

        // tiny politeness: these calls are rare (only when leaderboard payload lacks verify-date)
        usleep(2000);
    }

    // Determine baseline best time before cutoff, if available in our TOPN slice.
    double baseline_best = INFINITY;
    for (int i = 0; i < n; i++) {
        if (infos[i].verified_epoch > 0 && (time_t)infos[i].verified_epoch < cutoff_epoch) {
            if (infos[i].primary_t < baseline_best) baseline_best = infos[i].primary_t;
        }
    }

    // Collect candidates within window (must have verify_epoch known).
    LbRunInfo *cand = calloc((size_t)n, sizeof(LbRunInfo));
    if (!cand) { free_lbruninfos(infos, n); return; }
    int cN = 0;
    for (int i = 0; i < n; i++) {
        if (infos[i].verified_epoch <= 0) continue;
        if ((time_t)infos[i].verified_epoch < cutoff_epoch) continue;
        cand[cN].run_id = strdup(infos[i].run_id);
        cand[cN].primary_t = infos[i].primary_t;
        cand[cN].verified_epoch = infos[i].verified_epoch;
        cN++;
    }

    free_lbruninfos(infos, n);

    if (cN == 0) { free_lbruninfos(cand, cN); return; }

    qsort(cand, (size_t)cN, sizeof(LbRunInfo), lbrun_cmp_epoch_asc);

    const double EPS = 1e-6;
    double best = baseline_best;

    // If we couldn't find a baseline (TOPN slice didn't include any pre-window verified run),
    // we still track monotonic improvements/ties within the window.
    int have_baseline = isfinite(best);

    for (int i = 0; i < cN; i++) {
        int include = 0;
        if (!have_baseline) {
            // First event becomes the initial within-window "record chain" seed.
            include = 1;
            best = cand[i].primary_t;
            have_baseline = 1;
        } else {
            double t = cand[i].primary_t;
            if (t < best - EPS) {
                include = 1;
                best = t;
            } else if (fabs(t - best) <= EPS) {
                // tie WR: include as a new co-#1
                include = 1;
            }
        }

        if (!include) continue;

        // Add full run details (with embeds) to wrs.json, if not present.
        if (strset_has(runIds, cand[i].run_id)) continue;

        cJSON *runFull = fetch_run_details(curl, cand[i].run_id, 1);
        if (!runFull) continue;

        long ve = 0;
        const char *iso = NULL;
        if (!get_run_verify_epoch_and_iso(runFull, &ve, &iso)) {
            cJSON_Delete(runFull);
            continue;
        }

        // Ensure it's still in window (defensive)
        if ((time_t)ve >= cutoff_epoch) {
            add_wr_entry_from_run(curl, catCache, wrs, runIds, runFull, ve, iso);
        }

        cJSON_Delete(runFull);

        // very light politeness delay
        usleep(3000);
    }

    free_lbruninfos(cand, cN);
}

/* ----------------- scan runs feed, detect new current-WR keys, then backfill history ----------------- */

static long scan_new_runs_and_update(CURL *curl, CatVarCache **catCache, LbCache **lbCache,
                                     cJSON *wrs, StrSet *runIds,
                                     long last_seen_epoch,
                                     time_t prune_cutoff_epoch) {
    const int max = 200;
    int offset = 0;

    long new_last_seen = last_seen_epoch;

    long scan_floor;
    const long overlap_sec = 1 * 3600;

    if (last_seen_epoch > 0) {
        scan_floor = last_seen_epoch - overlap_sec;
    } else {
        scan_floor = (long)prune_cutoff_epoch - overlap_sec;
    }
    if (scan_floor < 0) scan_floor = 0;

    StrSet processedKeys = {0};
    strset_init(&processedKeys, 1024);

    long pages = 0;
    long runs_seen = 0;
    long runs_checked = 0;
    long keys_processed = 0;
    time_t newest = 0;
    time_t oldest = 0;

    while (1) {
        pages++;
        LOG("Runs page: offset=%d max=%d scan_floor=%ld prune_cutoff=%ld last_seen=%ld",
            offset, max, scan_floor, (long)prune_cutoff_epoch, last_seen_epoch);

        char url[1024];
        snprintf(url, sizeof(url),
                 "https://www.speedrun.com/api/v1/runs"
                 "?status=verified&orderby=verify-date&direction=desc"
                 "&embed=game,category,players,level"
                 "&max=%d&offset=%d",
                 max, offset);

        char *json = fetch_url(curl, url);
        if (!json) {
            LOG("Failed to fetch runs page (offset=%d). Stopping.", offset);
            break;
        }

        cJSON *root = cJSON_Parse(json);
        free(json);
        if (!root) {
            LOG("Failed to parse runs JSON (offset=%d). Stopping.", offset);
            break;
        }

        cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");
        if (!cJSON_IsArray(data)) {
            LOG("Runs JSON missing data[] (offset=%d). Stopping.", offset);
            cJSON_Delete(root);
            break;
        }

        int page_n = cJSON_GetArraySize(data);
        if (page_n <= 0) {
            LOG("Runs page empty (offset=%d). Stopping.", offset);
            cJSON_Delete(root);
            break;
        }

        int stop = 0;

        for (int i = 0; i < page_n; i++) {
            cJSON *run = cJSON_GetArrayItem(data, i);
            if (!cJSON_IsObject(run)) continue;

            const char *verify_date = NULL;
            cJSON *status = cJSON_GetObjectItemCaseSensitive(run, "status");
            if (cJSON_IsObject(status)) verify_date = json_get_string(status, "verify-date");

            time_t vtime = parse_iso8601_utc(verify_date);
            if (vtime == (time_t)-1) continue;

            runs_seen++;
            if (newest == 0) newest = vtime;
            oldest = vtime;

            if ((long)vtime > new_last_seen) new_last_seen = (long)vtime;

            if ((long)vtime < scan_floor) { stop = 1; break; }

            if (vtime < prune_cutoff_epoch) continue;

            runs_checked++;

            const char *runId = json_get_string(run, "id");
            if (!runId) continue;

            // Quick skip: if we already stored this run, it won't create a "new current WR key" event.
            if (strset_has(runIds, runId)) continue;

            const char *gameId = NULL, *gameName = NULL;
            const char *catId  = NULL, *catName  = NULL;
            const char *levelId = NULL, *levelName = NULL;

            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "game"), &gameId, &gameName);
            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "category"), &catId, &catName);
            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "level"), &levelId, &levelName);
            if (!gameId || !catId) continue;

            cJSON *valuesObj = cJSON_GetObjectItemCaseSensitive(run, "values");

            // If this run is the CURRENT #1, then this leaderboard had a record change chain inside our window.
            // We process the leaderboard key once and reconstruct all record-change events (incl. intermediates).
            if (is_current_wr(curl, lbCache, runId, gameId, catId, levelId, valuesObj)) {
                char *key = make_lb_key(gameId, catId, levelId, valuesObj);
                if (key) {
                    if (!strset_has(&processedKeys, key)) {
                        strset_add(&processedKeys, key);
                        keys_processed++;

                        LOG("New current WR detected; backfilling history for key: %s", key);
                        track_leaderboard_history(curl, catCache, wrs, runIds, gameId, catId, levelId, valuesObj, prune_cutoff_epoch);
                    }
                    free(key);
                }
            }

            // heartbeat log
            if (g_debug && (runs_seen % 500 == 0)) {
                LOG("Progress: pages=%ld seen=%ld checked=%ld keys_processed=%ld (offset=%d)",
                    pages, runs_seen, runs_checked, keys_processed, offset);
            }

            // tiny politeness delay
            if ((runs_checked % 40) == 0) usleep(2000);
        }

        // ---- human-friendly progress estimate ----
        if (g_debug && newest != 0 && oldest != 0) {
            double total = difftime(newest, (time_t)scan_floor);   // seconds from newest down to scan_floor
            double done  = difftime(newest, oldest);              // seconds from newest down to oldest on this page
            double pct = 0.0;
            if (total > 0.0) {
                pct = (done / total) * 100.0;
                if (pct < 0.0) pct = 0.0;
                if (pct > 100.0) pct = 100.0;
            }

            char oldest_iso[32], floor_iso[32];
            format_iso_utc(oldest, oldest_iso, sizeof(oldest_iso));
            format_iso_utc((time_t)scan_floor, floor_iso, sizeof(floor_iso));

            double remaining = difftime(oldest, (time_t)scan_floor); // seconds remaining until scan_floor
            if (remaining < 0) remaining = 0;

            LOG("Scan progress: oldest=%s scan_floor=%s done=%.1f%% remaining≈%.2f hours",
                oldest_iso, floor_iso, pct, remaining / 3600.0);
        }

        LOG("Page done: pages=%ld seen=%ld checked=%ld keys_processed=%ld newest=%ld oldest=%ld",
            pages, runs_seen, runs_checked, keys_processed, (long)newest, (long)oldest);

        cJSON_Delete(root);

        if (stop) {
            LOG("Stopping scan: reached scan_floor (oldest run < scan_floor)");
            break;
        }

        offset += page_n;
        if (page_n < max) break;
    }

    strset_free(&processedKeys);

    LOG("Scan complete: pages=%ld seen=%ld checked=%ld keys_processed=%ld new_last_seen=%ld",
        pages, runs_seen, runs_checked, keys_processed, new_last_seen);

    return new_last_seen;
}

/* ----------------- README rendering ----------------- */

static void fputs_html_escaped(FILE *fp, const char *s) {
    if (!s) return;
    for (const unsigned char *p = (const unsigned char*)s; *p; p++) {
        switch (*p) {
            case '&': fputs("&amp;", fp); break;
            case '<': fputs("&lt;", fp); break;
            case '>': fputs("&gt;", fp); break;
            case '"': fputs("&quot;", fp); break;
            case '\'': fputs("&#39;", fp); break;
            default: fputc(*p, fp); break;
        }
    }
}

static void print_section_from_wrs(const char *title, cJSON *wrs, time_t cutoff_epoch) {
    printf("### %s\n\n", title);

    // Scrollable table container; full-width; wraps text to avoid horizontal scrolling.
    printf("<div style=\"max-height: 520px; overflow-y: auto; border: 1px solid #d0d7de; border-radius: 8px; padding: 6px;\">\n");
    printf("<table style=\"width: 100%%; border-collapse: collapse; table-layout: fixed;\">\n");

    printf("<thead>\n<tr>\n");
    printf("<th align=\"left\" style=\"white-space: nowrap;\">Verified (ET)</th>\n");
    printf("<th align=\"left\">Game</th>\n");
    printf("<th align=\"left\">Category</th>\n");
    printf("<th align=\"left\">Subcategory</th>\n");
    printf("<th align=\"left\">Level</th>\n");
    printf("<th align=\"right\" style=\"white-space: nowrap;\">Time</th>\n");
    printf("<th align=\"left\">Runner(s)</th>\n");
    printf("<th align=\"left\" style=\"white-space: nowrap;\">Link</th>\n");
    printf("</tr>\n</thead>\n");

    printf("<tbody>\n");

    int printed = 0;
    cJSON *it = NULL;
    cJSON_ArrayForEach(it, wrs) {
        long v = json_get_long(it, "verified_epoch", 0);
        if ((time_t)v < cutoff_epoch) continue;

        const char *game = json_get_string(it, "game");
        const char *cat = json_get_string(it, "category");
        const char *sub = json_get_string(it, "subcats");
        const char *lvl = json_get_string(it, "level");
        const char *players = json_get_string(it, "players");
        const char *link = json_get_string(it, "weblink");
        double t = json_get_number(it, "primary_t", -1);

        char tbuf[64];
        format_seconds(t, tbuf, sizeof(tbuf));

        char vbuf[64];
        format_pretty_et((time_t)v, vbuf, sizeof(vbuf));

        printf("<tr>\n");

        printf("<td style=\"white-space: nowrap;\">");
        fputs_html_escaped(stdout, vbuf);
        printf("</td>\n");

        printf("<td style=\"word-break: break-word;\">");
        fputs_html_escaped(stdout, game ? game : "");
        printf("</td>\n");

        printf("<td style=\"word-break: break-word;\">");
        fputs_html_escaped(stdout, cat ? cat : "");
        printf("</td>\n");

        printf("<td style=\"word-break: break-word;\">");
        fputs_html_escaped(stdout, (sub && sub[0]) ? sub : "");
        printf("</td>\n");

        printf("<td style=\"word-break: break-word;\">");
        fputs_html_escaped(stdout, (lvl && lvl[0]) ? lvl : "");
        printf("</td>\n");

        printf("<td align=\"right\" style=\"white-space: nowrap;\">");
        fputs_html_escaped(stdout, tbuf);
        printf("</td>\n");

        printf("<td style=\"word-break: break-word;\">");
        fputs_html_escaped(stdout, players ? players : "");
        printf("</td>\n");

        printf("<td style=\"white-space: nowrap;\">");
        if (link && link[0]) {
            printf("<a href=\"");
            fputs_html_escaped(stdout, link);
            printf("\">🔗</a>");
        }
        printf("</td>\n");

        printf("</tr>\n");

        printed++;
    }

    if (printed == 0) {
        printf("<tr><td colspan=\"8\"><em>None</em></td></tr>\n");
    }

    printf("</tbody>\n");
    printf("</table>\n");
    printf("</div>\n\n");
}

/* ----------------- main ----------------- */

int main(void) {
    init_debug_from_env();
    init_tz_eastern(); // affects README time formatting via localtime()

    if (!ensure_dir("data")) {
        fprintf(stderr, "Failed to ensure ./data directory\n");
        return 1;
    }

    time_t now = time(NULL);
    time_t cutoff_1h  = now - 1 * 3600;
    time_t cutoff_24h = now - 24 * 3600;

    LOG("Start. now=%ld cutoff_1h=%ld cutoff_24h=%ld",
        (long)now, (long)cutoff_1h, (long)cutoff_24h);

    curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL *curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "curl_easy_init failed\n");
        curl_global_cleanup();
        return 1;
    }

    long last_seen_epoch = load_last_seen_epoch();
    cJSON *wrs = load_wrs_array();

    // Always prune first (keeps file small) — keep only last 24 hours
    prune_old_wrs(wrs, cutoff_24h);

    // Build run-id set from pruned cache
    StrSet runIds = {0};
    strset_init(&runIds, 2048);

    int existing = cJSON_GetArraySize(wrs);
    for (int i = 0; i < existing; i++) {
        cJSON *it = cJSON_GetArrayItem(wrs, i);
        if (!cJSON_IsObject(it)) continue;
        const char *rid = json_get_string(it, "run_id");
        if (rid) strset_add(&runIds, rid);
    }

    LOG("Loaded state: last_seen_epoch=%ld", last_seen_epoch);
    LOG("Loaded wrs.json (post-prune): %d entries", cJSON_GetArraySize(wrs));

    CatVarCache *catCache = NULL;
    LbCache *lbCache = NULL;

    long new_last_seen = scan_new_runs_and_update(curl, &catCache, &lbCache, wrs, &runIds, last_seen_epoch, cutoff_24h);

    // Sort newest-first for rendering and persistence
    cJSON *sorted = sorted_wrs_dup(wrs);
    cJSON_Delete(wrs);
    wrs = sorted;

    // Persist
    save_wrs_array(wrs);
    save_last_seen_epoch(new_last_seen);

    LOG("After scan: wrs.json entries=%d new_last_seen=%ld", cJSON_GetArraySize(wrs), new_last_seen);
    LOG("Saved state+wrs.");

    // Output markdown/HTML (GitHub README supports inline HTML)
    printf("## 🏁 Live #1 Records\n\n");
    printf("_Updated hourly via GitHub Actions._\n\n");

    print_section_from_wrs("Past hour", wrs, cutoff_1h);
    print_section_from_wrs("Past 24 hours", wrs, cutoff_24h);

    cJSON_Delete(wrs);
    free_cache(catCache);
    free_lb_cache(lbCache);
    strset_free(&runIds);

    curl_easy_cleanup(curl);
    curl_global_cleanup();
    return 0;
}
