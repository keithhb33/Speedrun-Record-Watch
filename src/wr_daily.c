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

/* --- README timestamp formatting in Eastern Time (ET; shows EST/EDT) --- */
static void init_tz_eastern(void) {
    setenv("TZ", "America/New_York", 1);
    tzset();
}

static void format_pretty_et(time_t t, char *out, size_t outsz) {
    if (!out || outsz == 0) return;
    struct tm tmv;
    localtime_r(&t, &tmv);
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
        ns.keys[idx] = k;
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
    if (s->len * 10 >= s->cap * 7) {
        if (!strset_rehash(s, s->cap * 2)) return 0;
    }
    uint64_t h = fnv1a_64(key);
    size_t mask = s->cap - 1;
    size_t idx = (size_t)h & mask;
    while (s->keys[idx]) {
        if (strcmp(s->keys[idx], key) == 0) return 1;
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

/* Get game asset URI from an embedded run object (run.game.data.assets.<key>.uri) */
static const char *get_game_asset_uri_from_run(cJSON *runObj, const char *asset_key) {
    if (!cJSON_IsObject(runObj) || !asset_key) return NULL;

    cJSON *game = cJSON_GetObjectItemCaseSensitive(runObj, "game");
    if (!cJSON_IsObject(game)) return NULL;

    cJSON *gdata = cJSON_GetObjectItemCaseSensitive(game, "data");
    if (!cJSON_IsObject(gdata)) return NULL;

    cJSON *assets = cJSON_GetObjectItemCaseSensitive(gdata, "assets");
    if (!cJSON_IsObject(assets)) return NULL;

    cJSON *asset = cJSON_GetObjectItemCaseSensitive(assets, asset_key);
    if (!cJSON_IsObject(asset)) return NULL;

    const char *uri = json_get_string(asset, "uri");
    return (uri && uri[0]) ? uri : NULL;
}

/* Normalize cover URLs:
   - force https
   - change ".../cover?..." to ".../cover.png?..." (or ".../cover.png" if no query)
*/
static void normalize_cover_uri(const char *in, char *out, size_t outsz) {
    if (!out || outsz == 0) return;
    out[0] = '\0';
    if (!in || !in[0]) return;

    char tmp[1024];
    tmp[0] = '\0';

    if (strncmp(in, "http://", 7) == 0) {
        snprintf(tmp, sizeof(tmp), "https://%s", in + 7);
    } else {
        snprintf(tmp, sizeof(tmp), "%s", in);
    }

    const char *p = strstr(tmp, "/cover");
    if (!p) {
        snprintf(out, outsz, "%s", tmp);
        return;
    }

    if (strncmp(p, "/cover.png", 9) == 0) {
        snprintf(out, outsz, "%s", tmp);
        return;
    }

    size_t prefix_len = (size_t)(p - tmp) + strlen("/cover");
    if (prefix_len >= sizeof(tmp)) {
        snprintf(out, outsz, "%s", tmp);
        return;
    }

    char prefix[1024];
    if (prefix_len >= sizeof(prefix)) {
        snprintf(out, outsz, "%s", tmp);
        return;
    }
    memcpy(prefix, tmp, prefix_len);
    prefix[prefix_len] = '\0';

    const char *suffix = tmp + prefix_len;
    snprintf(out, outsz, "%s.png%s", prefix, suffix);
}

/* Normalize any URI:
   - force https if it starts with http://
*/
static void normalize_uri_https(const char *in, char *out, size_t outsz) {
    if (!out || outsz == 0) return;
    out[0] = '\0';
    if (!in || !in[0]) return;

    if (strncmp(in, "http://", 7) == 0) {
        snprintf(out, outsz, "https://%s", in + 7);
    } else {
        snprintf(out, outsz, "%s", in);
    }
}

/* Normalize speedrun.com user image URLs:
   - force https
   - change ".../image?..." to ".../image.png?..." (or ".../image.png" if no query)
   Examples:
     https://www.speedrun.com/static/user/abc/image?v=123  -> https://www.speedrun.com/static/user/abc/image.png?v=123
     http://www.speedrun.com/static/user/abc/image         -> https://www.speedrun.com/static/user/abc/image.png
*/
static void normalize_user_image_uri(const char *in, char *out, size_t outsz) {
    if (!out || outsz == 0) return;
    out[0] = '\0';
    if (!in || !in[0]) return;

    char tmp[1024];
    tmp[0] = '\0';

    /* https */
    if (strncmp(in, "http://", 7) == 0) {
        snprintf(tmp, sizeof(tmp), "https://%s", in + 7);
    } else {
        snprintf(tmp, sizeof(tmp), "%s", in);
    }

    /* find last "/image" occurrence */
    const char *p = NULL;
    const char *q = tmp;
    while ((q = strstr(q, "/image")) != NULL) {
        p = q;
        q += 6; /* strlen("/image") */
    }

    if (!p) {
        snprintf(out, outsz, "%s", tmp);
        return;
    }

    /* already has .png */
    if (strncmp(p, "/image.png", 10) == 0) {
        snprintf(out, outsz, "%s", tmp);
        return;
    }

    /* only rewrite if it's exactly "/image" followed by end or query/fragment */
    if (strncmp(p, "/image", 6) == 0) {
        char after = p[6];
        if (after == '\0' || after == '?' || after == '#') {
            size_t prelen = (size_t)(p - tmp) + 6; /* include "/image" */
            if (prelen >= sizeof(tmp)) {
                snprintf(out, outsz, "%s", tmp);
                return;
            }

            /* out = tmp[0:prelen] + ".png" + tmp[prelen:] */
            /* tmp[prelen:] starts at '?' or '\0' or '#' */
            int written = snprintf(out, outsz, "%.*s.png%s", (int)prelen, tmp, tmp + prelen);
            if (written < 0) out[0] = '\0';
            return;
        }
    }

    snprintf(out, outsz, "%s", tmp);
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

/* Build players array with avatar + profile link from embedded run.players.data */
static cJSON *build_players_array(cJSON *runObj) {
    cJSON *players = cJSON_GetObjectItemCaseSensitive(runObj, "players");
    if (cJSON_IsObject(players)) players = cJSON_GetObjectItemCaseSensitive(players, "data");
    if (!cJSON_IsArray(players)) return NULL;

    cJSON *out = cJSON_CreateArray();
    if (!out) return NULL;

    cJSON *p = NULL;
    cJSON_ArrayForEach(p, players) {
        const char *name = json_get_string(p, "name");
        if (!name) {
            cJSON *names = cJSON_GetObjectItemCaseSensitive(p, "names");
            if (cJSON_IsObject(names)) name = json_get_string(names, "international");
        }
        if (!name) name = json_get_string(p, "id");
        if (!name) name = "unknown";

        const char *weblink = json_get_string(p, "weblink");

        const char *img_raw = NULL;
        cJSON *assets = cJSON_GetObjectItemCaseSensitive(p, "assets");
        if (cJSON_IsObject(assets)) {
            cJSON *imgObj = cJSON_GetObjectItemCaseSensitive(assets, "image");
            if (cJSON_IsObject(imgObj)) img_raw = json_get_string(imgObj, "uri");

            if (!img_raw || !img_raw[0]) {
                cJSON *iconObj = cJSON_GetObjectItemCaseSensitive(assets, "icon");
                if (cJSON_IsObject(iconObj)) img_raw = json_get_string(iconObj, "uri");
            }
        }

        char img[1024];
        img[0] = '\0';
        if (img_raw && img_raw[0]) {
            /* IMPORTANT: user avatars require ".png" inserted after "/image" */
            normalize_user_image_uri(img_raw, img, sizeof(img));
        }

        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "name", name);
        cJSON_AddStringToObject(o, "weblink", (weblink && weblink[0]) ? weblink : "");
        cJSON_AddStringToObject(o, "image", img[0] ? img : "");
        cJSON_AddItemToArray(out, o);
    }

    if (cJSON_GetArraySize(out) == 0) {
        cJSON_Delete(out);
        return NULL;
    }

    return out;
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

/* ----------------- add WR entry (store game cover + players_data) ----------------- */

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

    const char *cover_uri_raw = get_game_asset_uri_from_run(run, "cover-tiny");
    if (!cover_uri_raw) cover_uri_raw = get_game_asset_uri_from_run(run, "cover-small");
    if (!cover_uri_raw) cover_uri_raw = get_game_asset_uri_from_run(run, "cover-medium");
    if (!cover_uri_raw) cover_uri_raw = get_game_asset_uri_from_run(run, "cover-large");
    if (!cover_uri_raw) cover_uri_raw = get_game_asset_uri_from_run(run, "icon");

    char cover_uri[1024];
    cover_uri[0] = '\0';
    if (cover_uri_raw && cover_uri_raw[0]) {
        normalize_cover_uri(cover_uri_raw, cover_uri, sizeof(cover_uri));
    }

    double primary_t = -1;
    cJSON *times = cJSON_GetObjectItemCaseSensitive(run, "times");
    if (cJSON_IsObject(times)) primary_t = json_get_number(times, "primary_t", -1);

    cJSON *valuesObj = cJSON_GetObjectItemCaseSensitive(run, "values");

    char players[512];
    print_players_compact(run, players, sizeof(players));

    cJSON *players_data = build_players_array(run);

    char *subcats = format_subcategories(curl, catCache, catId, valuesObj);

    cJSON *obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "run_id", runId);
    cJSON_AddNumberToObject(obj, "verified_epoch", (double)verified_epoch);
    cJSON_AddStringToObject(obj, "verified_iso", verify_date ? verify_date : "");
    cJSON_AddStringToObject(obj, "game", gameName ? gameName : gameId);
    cJSON_AddStringToObject(obj, "game_cover", cover_uri[0] ? cover_uri : "");
    cJSON_AddStringToObject(obj, "category", catName ? catName : catId);
    cJSON_AddStringToObject(obj, "level", (levelId ? (levelName ? levelName : levelId) : ""));
    cJSON_AddStringToObject(obj, "subcats", subcats ? subcats : "");
    cJSON_AddNumberToObject(obj, "primary_t", primary_t);
    cJSON_AddStringToObject(obj, "players", players);
    if (players_data) {
        cJSON_AddItemToObject(obj, "players_data", players_data);
    }
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
    long verified_epoch;
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
        } else {
            ve = 0;
        }

        infos[n].run_id = strdup(rid);
        infos[n].primary_t = pt;
        infos[n].verified_epoch = ve;
        n++;
    }

    cJSON_Delete(root);

    if (n == 0) { free(infos); return; }

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
        usleep(2000);
    }

    double baseline_best = INFINITY;
    for (int i = 0; i < n; i++) {
        if (infos[i].verified_epoch > 0 && (time_t)infos[i].verified_epoch < cutoff_epoch) {
            if (infos[i].primary_t < baseline_best) baseline_best = infos[i].primary_t;
        }
    }

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
    int have_baseline = isfinite(best);

    for (int i = 0; i < cN; i++) {
        int include = 0;
        if (!have_baseline) {
            include = 1;
            best = cand[i].primary_t;
            have_baseline = 1;
        } else {
            double tt = cand[i].primary_t;
            if (tt < best - EPS) {
                include = 1;
                best = tt;
            } else if (fabs(tt - best) <= EPS) {
                include = 1;
            }
        }

        if (!include) continue;
        if (strset_has(runIds, cand[i].run_id)) continue;

        cJSON *runFull = fetch_run_details(curl, cand[i].run_id, 1);
        if (!runFull) continue;

        long ve = 0;
        const char *iso = NULL;
        if (!get_run_verify_epoch_and_iso(runFull, &ve, &iso)) {
            cJSON_Delete(runFull);
            continue;
        }

        if ((time_t)ve >= cutoff_epoch) {
            add_wr_entry_from_run(curl, catCache, wrs, runIds, runFull, ve, iso);
        }

        cJSON_Delete(runFull);
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
    const long overlap_sec = 24 * 3600;

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
            if ((long)vtime > new_last_seen) new_last_seen = (long)vtime;

            if ((long)vtime < scan_floor) { stop = 1; break; }
            if (vtime < prune_cutoff_epoch) continue;

            runs_checked++;

            const char *runId = json_get_string(run, "id");
            if (!runId) continue;

            if (strset_has(runIds, runId)) continue;

            const char *gameId = NULL, *gameName = NULL;
            const char *catId  = NULL, *catName  = NULL;
            const char *levelId = NULL, *levelName = NULL;

            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "game"), &gameId, &gameName);
            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "category"), &catId, &catName);
            extract_id_and_name(cJSON_GetObjectItemCaseSensitive(run, "level"), &levelId, &levelName);
            if (!gameId || !catId) continue;

            cJSON *valuesObj = cJSON_GetObjectItemCaseSensitive(run, "values");

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

            if ((runs_checked % 40) == 0) usleep(2000);
        }

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

/* ----------------- README rendering (only Subcategories truncated) ----------------- */

static void fputs_html_escaped(FILE *fp, const char *s) {
    if (!s) return;
    for (const unsigned char *p = (const unsigned char*)s; *p; p++) {
        switch (*p) {
            case '&': fputs("&amp;", fp); break;
            case '<': fputs("&lt;", fp); break;
            case '>': fputs("&gt;", fp); break;
            case '"': fputs("&quot;", fp); break;
            case '\'': fputs("&#39;", fp); break;
            case '|': fputs("&#124;", fp); break;
            case '\n': case '\r': case '\t': fputc(' ', fp); break;
            default: fputc(*p, fp); break;
        }
    }
}

static void print_cell_plain_sub(const char *s) {
    printf("<sub>");
    fputs_html_escaped(stdout, s ? s : "");
    printf("</sub>");
}

static void print_cell_subcat_trunc(const char *s, int max_chars) {
    if (!s) s = "";
    int n = (int)strlen(s);
    int trunc = (max_chars > 0 && n > max_chars);

    printf("<sub><span title=\"");
    fputs_html_escaped(stdout, s);
    printf("\">");

    if (!trunc || max_chars <= 0) {
        fputs_html_escaped(stdout, s);
    } else {
        int take = max_chars - 1;
        if (take < 0) take = 0;

        int end = take;
        while (end > 0) {
            unsigned char c = (unsigned char)s[end];
            if ((c & 0xC0) != 0x80) break;
            end--;
        }
        if (end <= 0) end = take;

        for (int i = 0; i < end && s[i]; i++) {
            char tmp[2] = { s[i], 0 };
            fputs_html_escaped(stdout, tmp);
        }
        printf("…");
    }

    printf("</span></sub>");
}

/* Game cell: image + <br/> + title */
static void print_game_cell_with_cover(const char *game_name, const char *cover_uri_maybe) {
    if (!game_name) game_name = "";

    char cover_norm[1024];
    cover_norm[0] = '\0';
    if (cover_uri_maybe && cover_uri_maybe[0]) {
        normalize_cover_uri(cover_uri_maybe, cover_norm, sizeof(cover_norm));
    }

    printf("<div style=\"text-align:center;\">");

    if (cover_norm[0]) {
        printf("<img src=\"");
        fputs_html_escaped(stdout, cover_norm);
        printf("\" alt=\"\" width=\"60\" style=\"display:block; margin:0 auto 4px auto;\"/>");
        printf("<br/>");
    } else {
        printf("<br/>");
    }

    printf("<sub>");
    fputs_html_escaped(stdout, game_name);
    printf("</sub>");

    printf("</div>");
}

/* Runner(s) cell: avatars + <br/> + names */
static void print_runners_cell_with_avatars(cJSON *players_data, const char *fallback_names) {
    if (!cJSON_IsArray(players_data) || cJSON_GetArraySize(players_data) <= 0) {
        print_cell_plain_sub(fallback_names ? fallback_names : "");
        return;
    }

    printf("<div style=\"display:flex; gap:6px; justify-content:center; align-items:flex-start;\">");

    int n = cJSON_GetArraySize(players_data);
    for (int i = 0; i < n; i++) {
        cJSON *p = cJSON_GetArrayItem(players_data, i);
        if (!cJSON_IsObject(p)) continue;

        const char *name = json_get_string(p, "name");
        const char *img  = json_get_string(p, "image");
        const char *link = json_get_string(p, "weblink");

        if (!name) name = "unknown";
        if (!img) img = "";
        if (!link) link = "";

        printf("<div style=\"text-align:center;\">");

        if (img[0]) {
            if (link[0]) {
                printf("<a href=\"");
                fputs_html_escaped(stdout, link);
                printf("\">");
            }

            printf("<img src=\"");
            fputs_html_escaped(stdout, img);
            printf("\" alt=\"\" width=\"40\" style=\"display:block; margin:0 auto 4px auto; border-radius:50%%;\"/>");

            if (link[0]) printf("</a>");
            printf("<br/>");
        } else {
            printf("<br/>");
        }

        printf("<sub>");
        fputs_html_escaped(stdout, name);
        printf("</sub>");

        printf("</div>");
    }

    printf("</div>");
}

static void print_section_from_wrs(const char *title, cJSON *wrs, time_t cutoff_epoch) {
    printf("### %s\n\n", title);

    printf("| <sub>When (ET)</sub> | <sub>Game</sub> | <sub>Category</sub> | <sub>Subcategory</sub> | <sub>Level</sub> | <sub>Time</sub> | <sub>Runner(s)</sub> | <sub>Link</sub> |\n");
    printf("|---|---|---|---|---|---:|---|---|\n");

    int printed = 0;
    cJSON *it = NULL;
    cJSON_ArrayForEach(it, wrs) {
        long v = json_get_long(it, "verified_epoch", 0);
        if ((time_t)v < cutoff_epoch) continue;

        const char *game = json_get_string(it, "game");
        const char *game_cover = json_get_string(it, "game_cover");
        const char *cat = json_get_string(it, "category");
        const char *sub = json_get_string(it, "subcats");
        const char *lvl = json_get_string(it, "level");
        const char *players = json_get_string(it, "players");
        cJSON *players_data = cJSON_GetObjectItemCaseSensitive(it, "players_data");
        const char *link = json_get_string(it, "weblink");
        double t = json_get_number(it, "primary_t", -1);

        char tbuf[64];
        format_seconds(t, tbuf, sizeof(tbuf));

        char when_buf[64];
        format_pretty_et((time_t)v, when_buf, sizeof(when_buf));

        printf("| ");
        print_cell_plain_sub(when_buf);
        printf(" | ");
        print_game_cell_with_cover(game ? game : "", (game_cover && game_cover[0]) ? game_cover : NULL);
        printf(" | ");
        print_cell_plain_sub(cat ? cat : "");
        printf(" | ");
        print_cell_subcat_trunc((sub && sub[0]) ? sub : "", 20);
        printf(" | ");
        print_cell_plain_sub((lvl && lvl[0]) ? lvl : "");
        printf(" | <sub>");
        fputs_html_escaped(stdout, tbuf);
        printf("</sub> | ");

        print_runners_cell_with_avatars(players_data, players ? players : "");

        printf(" | ");

        if (link && link[0]) {
            printf("<sub><a href=\"");
            fputs_html_escaped(stdout, link);
            printf("\">link</a></sub>");
        } else {
            printf("<sub>&nbsp;</sub>");
        }

        printf(" |\n");
        printed++;
    }

    if (printed == 0) {
        printf("| <sub>—</sub> | <em>None</em> |  |  |  |  |  |  |\n");
    }

    printf("\n");
}

/* Upgrade existing recent wrs.json entries (within cutoff) with players_data so avatars show immediately */
static void enrich_recent_entries_with_players_data(CURL *curl, cJSON *wrs, time_t cutoff_epoch) {
    if (!cJSON_IsArray(wrs)) return;

    cJSON *it = NULL;
    cJSON_ArrayForEach(it, wrs) {
        if (!cJSON_IsObject(it)) continue;

        long v = json_get_long(it, "verified_epoch", 0);
        if ((time_t)v < cutoff_epoch) continue;

        if (cJSON_GetObjectItemCaseSensitive(it, "players_data")) continue;

        const char *rid = json_get_string(it, "run_id");
        if (!rid || !rid[0]) continue;

        cJSON *runFull = fetch_run_details(curl, rid, 1);
        if (!runFull) continue;

        cJSON *arr = build_players_array(runFull);
        cJSON_Delete(runFull);

        if (arr) {
            cJSON_AddItemToObject(it, "players_data", arr);
        }

        usleep(2000);
    }
}

/* ----------------- main ----------------- */

int main(void) {
    init_debug_from_env();
    init_tz_eastern();

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

    prune_old_wrs(wrs, cutoff_24h);

    StrSet runIds = {0};
    strset_init(&runIds, 2048);

    int existing = cJSON_GetArraySize(wrs);
    for (int i = 0; i < existing; i++) {
        cJSON *it = cJSON_GetArrayItem(wrs, i);
        if (!cJSON_IsObject(it)) continue;
        const char *rid = json_get_string(it, "run_id");
        if (rid) strset_add(&runIds, rid);
    }

    /* Ensure avatars show for already-saved recent entries */
    enrich_recent_entries_with_players_data(curl, wrs, cutoff_24h);

    LOG("Loaded state: last_seen_epoch=%ld", last_seen_epoch);
    LOG("Loaded wrs.json (post-prune): %d entries", cJSON_GetArraySize(wrs));

    CatVarCache *catCache = NULL;
    LbCache *lbCache = NULL;

    long new_last_seen = scan_new_runs_and_update(
        curl, &catCache, &lbCache, wrs, &runIds, last_seen_epoch, cutoff_24h
    );

    cJSON *sorted = sorted_wrs_dup(wrs);
    cJSON_Delete(wrs);
    wrs = sorted;

    save_wrs_array(wrs);
    save_last_seen_epoch(new_last_seen);

    LOG("After scan: wrs.json entries=%d new_last_seen=%ld", cJSON_GetArraySize(wrs), new_last_seen);

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
