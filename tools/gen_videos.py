"""Daily builder for the Drift shared library.

Writes:  v/<topic>.tsv  (one shard per category)   series.tsv   data/channels.tsv   data/state.json
Quota: search = 100 units, everything else = 1 unit per call (50 items). Keys are read from the secrets
YT_KEY, YT_KEY2, YT_KEY3 (all optional except the first); the script rotates when one is exhausted.
Tune from the workflow (env): BUDGET_PER_KEY (default 9000), SHARD_BASE (2200), SHARD_MAX (6000), PER_CHANNEL (8).
"""
import json, os, sys, time, datetime, re, random, math, urllib.request, urllib.parse, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE) if os.path.basename(HERE) == "tools" else HERE
KEYS = [os.environ.get(k, "").strip() for k in ("YT_KEY", "YT_KEY2", "YT_KEY3")]
KEYS = [k for k in KEYS if k]
if not KEYS:
    sys.exit("YT_KEY secret is missing")
PER_KEY = int(os.environ.get("BUDGET_PER_KEY", "9000"))
SHARD_BASE = int(os.environ.get("SHARD_BASE", "2200"))
SHARD_MAX = int(os.environ.get("SHARD_MAX", "6000"))
PER_CHANNEL = int(os.environ.get("PER_CHANNEL", "8"))
TOTAL = PER_KEY * len(KEYS)
random.seed()
TODAY = datetime.date.today().toordinal()

class Out(Exception):
    pass

dead = set()
spent = 0
fails = 0

def api(path, cost=1):
    """Calls the API, rotating keys; raises Out when every key is exhausted."""
    global spent, fails
    if fails >= 15:
        raise Out()
    for _ in range(len(KEYS) + 1):
        live = [k for k in KEYS if k not in dead]
        if not live:
            raise Out()
        k = random.choice(live)
        url = "https://www.googleapis.com/youtube/v3/" + path + "&key=" + k
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                spent += cost
                fails = 0
                return json.load(r)
        except urllib.error.HTTPError as e:
            body = e.read()[:300].decode("utf-8", "ignore")
            if e.code == 403 and ("quota" in body.lower() or "rateLimit" in body):
                print("key exhausted", file=sys.stderr)
                dead.add(k)
                continue
            if e.code in (400, 404):
                fails += 1
                return None
            fails += 1
            print("HTTP", e.code, body[:120], file=sys.stderr)
            return None
        except Exception as e:
            print("ERR", e, file=sys.stderr)
            fails += 1
            time.sleep(1)
    return None

def secs(d):
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", d or "")
    if not m: return 0
    h, mi, s = (int(x or 0) for x in m.groups())
    return h * 3600 + mi * 60 + s

def age(s):
    try:
        t = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        return max(0, (datetime.datetime.utcnow() - t).days)
    except Exception:
        return 365

def clean(x):
    return (x or "").replace("\t", " ").replace("\n", " ").replace("\r", " ")

# ---------------- state ----------------
topics = json.load(open(os.path.join(HERE, "topics.json"), encoding="utf-8"))
T = {t["id"]: t for t in topics}
os.makedirs(os.path.join(ROOT, "v"), exist_ok=True)
os.makedirs(os.path.join(ROOT, "data"), exist_ok=True)

shards = {}   # topic id -> list of rows (list of 11 columns: id,dur,user,title,topic,channelId,views,likes,cat,age,day_added)
for t in T:
    p = os.path.join(ROOT, "v", t + ".tsv")
    rows = []
    if os.path.exists(p):
        for l in open(p, encoding="utf-8"):
            c = l.rstrip("\n").split("\t")
            if len(c) >= 10 and c[0]:
                rows.append((c + [""])[:11])
    shards[t] = rows
have = {r[0] for rows in shards.values() for r in rows}

chp = os.path.join(ROOT, "data", "channels.tsv")
channels = {}   # channelId -> [topic, lastCrawl, yield, crawls]
if os.path.exists(chp):
    for l in open(chp, encoding="utf-8"):
        c = l.rstrip("\n").split("\t")
        if len(c) >= 4: channels[c[0]] = [c[1], int(c[2]), int(c[3]), int(c[4]) if len(c) > 4 else 1]

sp = os.path.join(ROOT, "data", "state.json")
state = json.load(open(sp)) if os.path.exists(sp) else {}

manual = {}
wp = os.path.join(ROOT, "weights.json")
if os.path.exists(wp):
    try: manual = json.load(open(wp))
    except Exception: manual = {}

# ---------------- weights: more room for the categories that perform best / that you boost ----------------
def avg_views(t):
    rows = shards[t]
    if not rows: return 1.0
    v = sorted(int(r[6] or 0) for r in rows)
    return math.log10(max(10, v[len(v) // 2]))
base = {t: avg_views(t) for t in T}
mx = max(base.values()) or 1.0
weight = {t: (0.6 + base[t] / mx) * float(manual.get(t, 1.0)) for t in T}
def cap(t):
    return int(min(SHARD_MAX, SHARD_BASE * max(0.6, weight[t] / 1.4 * float(1.0))))

def pick_topic():
    ts = list(T)
    ws = [weight[t] * (1.4 if len(shards[t]) < cap(t) else 0.3) for t in ts]
    return random.choices(ts, ws)[0]

def row_ok(o):
    st = o.get("status", {})
    if not st.get("embeddable", True) or st.get("privacyStatus", "public") != "public": return None
    dur = secs(o.get("contentDetails", {}).get("duration"))
    if dur < 3 or dur > 75: return None
    return dur

BAD = re.compile(r"hindi|desi|bhojpuri|bollywood|punjabi|tamil|telugu|t-series|tseries|zee |bhai|yaar|\bkya\b|\bnahi\b|india", re.I)
def bad_text(title, channel):
    if BAD.search(title + " " + channel): return True
    letters = [c for c in title + channel if c.isalpha()]
    if letters:
        n = sum(1 for c in letters if 0x0900 <= ord(c) <= 0x0DFF or 0x0E00 <= ord(c) <= 0x0E7F or 0x0A00 <= ord(c) <= 0x0A7F)
        if n * 4 >= len(letters) or n >= 3: return True
    return False

added_total = 0
chan_count = {}
for t, rows in shards.items():
    for r in rows:
        chan_count[(t, r[5])] = chan_count.get((t, r[5]), 0) + 1

def add_videos(ids, topic):
    """details for ids (1 unit / 50) -> rows into the shard of `topic`; also registers the channels."""
    global added_total
    n = 0
    ids = [i for i in dict.fromkeys(ids) if i not in have]
    for k in range(0, len(ids), 50):
        d = api("videos?part=snippet,contentDetails,statistics,status&fields=items(id,snippet(title,channelTitle,channelId,publishedAt,categoryId,defaultAudioLanguage,defaultLanguage),contentDetails/duration,statistics(viewCount,likeCount),status(embeddable,privacyStatus))&id=" + ",".join(ids[k:k + 50]))
        if not d: continue
        for o in d.get("items", []):
            dur = row_ok(o)
            if dur is None: continue
            sn = o.get("snippet", {}); s = o.get("statistics", {})
            title, ch = clean(sn.get("title")), clean(sn.get("channelTitle"))
            if bad_text(title, ch): continue
            aud = (sn.get("defaultAudioLanguage") or sn.get("defaultLanguage") or "").lower().split("-")[0]
            if aud in ("hi", "bn", "ta", "te", "mr", "gu", "kn", "ml", "pa", "ne", "ur", "th", "vi", "id", "tl"): continue
            cid = sn.get("channelId", "")
            vid = o["id"]
            if vid in have or len(shards[topic]) >= cap(topic) + 400: continue
            if chan_count.get((topic, cid), 0) >= PER_CHANNEL: continue
            have.add(vid)
            chan_count[(topic, cid)] = chan_count.get((topic, cid), 0) + 1
            shards[topic].append([vid, str(dur), ch, title, T[topic]["ar"], cid, s.get("viewCount", "0"), s.get("likeCount", "0"), str(sn.get("categoryId", "0")), str(age(sn.get("publishedAt", ""))), aud[:2]])
            n += 1
            if cid and cid not in channels:
                channels[cid] = [topic, 0, 0, 0]
    added_total += n
    return n

try:
    # ---------- 1) discovery by search: random order, random time window, random region -> different results each day ----------
    for _ in range(100000):
        if spent >= TOTAL * 0.30: break
        t = pick_topic()
        qs_ = T[t]["q"]
        arq_ = [x for x in qs_ if re.search(r"[\u0600-\u06FF]", x)]
        q = random.choice(arq_) if arq_ and random.random() < 0.6 else random.choice(qs_)
        ar = bool(re.search(r"[؀-ۿ]", q))
        order = random.choice(["relevance", "viewCount", "date", "rating"])
        after = datetime.datetime.utcnow() - datetime.timedelta(days=random.choice([30, 90, 180, 365, 730, 1500]))
        win = ""
        if random.random() < 0.6:
            a = after.strftime("%Y-%m-%dT00:00:00Z")
            b = (after + datetime.timedelta(days=random.choice([60, 120, 365]))).strftime("%Y-%m-%dT00:00:00Z")
            win = "&publishedAfter=" + a + "&publishedBefore=" + b
        region = random.choice(["SA", "EG", "DZ", "AE", "MA", "IQ"]) if ar else random.choice(["US", "GB", "CA", "AU"])
        lang = "&relevanceLanguage=" + ("ar" if ar else "en") + "&regionCode=" + region
        r = api("search?part=id&type=video&videoDuration=short&videoEmbeddable=true&safeSearch=strict&fields=items(id/videoId)&maxResults=50&order=" + order + lang + win + "&q=" + urllib.parse.quote(q), 100)
        if not r: continue
        add_videos([i["id"]["videoId"] for i in r.get("items", []) if i.get("id", {}).get("videoId")], t)

    # ---------- 2) most popular per category / region (1 unit each) ----------
    CAT = {"20": ["game", "realgame", "parkour"], "23": ["meme", "skit", "fail"], "15": ["pets", "wild"], "10": ["music", "dance"],
           "17": ["sport", "fit", "martial"], "24": ["challenge", "movie", "cartoon"], "26": ["diy", "cook", "art"], "27": ["sci", "hist", "space"],
           "28": ["tech", "ai"], "19": ["travel", "city", "camp"], "1": ["anime", "cartoon", "movie"], "2": ["cars"], "22": ["story", "brain", "calm"]}
    for _ in range(100000):
        if spent >= TOTAL * 0.34: break
        c = random.choice(list(CAT))
        reg = random.choice(["US", "SA", "EG", "DZ", "GB", "FR", "DE", "BR", "TR", "AE", "MA", "CA"])
        r = api("videos?part=id&chart=mostPopular&videoCategoryId=" + c + "&regionCode=" + reg + "&maxResults=50&fields=items(id)")
        if r: add_videos([i["id"] for i in r.get("items", [])], random.choice([x for x in CAT[c] if x in T] or list(T)))

    # ---------- 3) crawl channel uploads (1 unit per 50 videos) ----------
    order = sorted(channels, key=lambda c: (channels[c][1], -channels[c][2]))
    done = 0
    for cid in order:
        if spent >= TOTAL * 0.62: break
        ch = channels[cid]
        if ch[3] >= 3 and ch[2] == 0: continue          # channel never gave a usable clip
        if TODAY - ch[1] < 3: continue
        pl = "UU" + cid[2:] if cid.startswith("UC") else None
        if not pl: continue
        r = api("playlistItems?part=contentDetails&maxResults=50&fields=items(contentDetails/videoId)&playlistId=" + pl)
        done += 1
        ch[1] = TODAY; ch[3] += 1
        if not r: continue
        ids = [i["contentDetails"]["videoId"] for i in r.get("items", [])]
        n = add_videos(ids, ch[0] if ch[0] in T else pick_topic())
        ch[2] += n

    # ---------- 4) re-check a slice of old clips (drop the ones that stopped being embeddable) ----------
    allrows = [(t, r) for t, rows in shards.items() for r in rows]
    cur = state.get("cursor", 0)
    slice_ = allrows[cur:cur + max(0, int((TOTAL * 0.70 - spent))) * 50]
    state["cursor"] = 0 if cur + len(slice_) >= len(allrows) else cur + len(slice_)
    alive = set()
    ids = [r[0] for _, r in slice_]
    for k in range(0, len(ids), 50):
        d = api("videos?part=contentDetails,status&fields=items(id,contentDetails/duration,status(embeddable,privacyStatus))&id=" + ",".join(ids[k:k + 50]))
        if d is None: alive.update(ids[k:k + 50]); continue
        for o in d.get("items", []):
            if row_ok(o) is not None: alive.add(o["id"])
    dropped = set(ids) - alive
    if dropped:
        for t in shards: shards[t] = [r for r in shards[t] if r[0] not in dropped]
        print("dropped", len(dropped))

    # ---------- 5) series (playlists whose clips are numbered episodes) ----------
    def epnum(raw):
        title = "".join(chr(ord("0") + ord(c) - 0x660) if "٠" <= c <= "٩" else c for c in raw)
        m = re.search(r"(?:الحلقة|حلقة|الجزء|جزء|episode|ep\.?|part|pt\.?|#|(?<![a-z])e)\s*[-:.]?\s*(\d{1,3})", title, re.I)
        if m: return int(m.group(1))
        m = re.search(r"(\d{1,3})\s*\W*$", title.strip())
        return int(m.group(1)) if m else None
    def ar_title(t):
        letters = [c for c in t if c.isalpha()]
        if not letters: return False
        a = sum(1 for c in letters if 0x600 <= ord(c) <= 0x6FF)
        low = t.lower()
        return a * 10 >= len(letters) * 3 or "arabic" in low or "arsub" in low or "مترجم" in t
    RANGE = re.compile(r"\d{1,3}\s*[-~–—]\s*\d{1,3}|full (episode|movie|series|story)|compilation|all episodes|complete", re.I)
    os.makedirs(os.path.join(ROOT, "s"), exist_ok=True)
    AVG = {}
    def build_eps(pid):
        """Writes s/<pid>.tsv (id, seconds, title) in watching order. Returns the number of usable episodes."""
        ids, tok = [], ""
        for _ in range(3):
            r = api("playlistItems?part=contentDetails&maxResults=50&fields=nextPageToken,items(contentDetails/videoId)&playlistId=" + pid + ("&pageToken=" + tok if tok else ""))
            if not r: break
            ids += [i["contentDetails"]["videoId"] for i in r.get("items", [])]
            tok = r.get("nextPageToken", "")
            if not tok: break
        rows = []
        for k in range(0, len(ids), 50):
            d = api("videos?part=snippet,contentDetails,status&fields=items(id,snippet/title,contentDetails/duration,status(embeddable,privacyStatus))&id=" + ",".join(ids[k:k + 50]))
            byid = {o["id"]: o for o in (d or {}).get("items", [])}
            for vid in ids[k:k + 50]:
                o = byid.get(vid)
                if not o: continue
                st = o.get("status", {})
                dur = secs(o.get("contentDetails", {}).get("duration"))
                if not st.get("embeddable", True) or st.get("privacyStatus", "public") != "public" or dur < 3 or dur > 900: continue
                rows.append([vid, str(dur), clean(o["snippet"]["title"])])
        if len(rows) < 5: return 0
        nums = [epnum(r[2]) for r in rows]
        have_n = [x for x in nums if x is not None]
        if len(have_n) >= len(rows) * 0.7 and len(set(have_n)) >= len(have_n) * 0.8:
            prev, keys = 0.0, []
            for n_ in nums:
                kk = float(n_) if n_ is not None else prev + 0.5
                prev = kk; keys.append(kk)
            rows = [rows[i] for i in sorted(range(len(rows)), key=lambda i: keys[i])]
        AVG[pid] = sum(int(r[1]) for r in rows) // len(rows)
        open(os.path.join(ROOT, "s", pid + ".tsv"), "w", encoding="utf-8").write("".join("\t".join(r) + "\n" for r in rows))
        return len(rows)
    series = []
    sp2 = os.path.join(ROOT, "series.tsv")
    if os.path.exists(sp2):
        for l in open(sp2, encoding="utf-8"):
            c = l.rstrip("\n").split("\t")
            if len(c) >= 4: series.append((c + [""])[:5] + c[5:6])
    known_s = {s[0] for s in series}
    sq = json.load(open(os.path.join(HERE, "series_queries.json"), encoding="utf-8"))
    for _ in range(100000):
        if spent >= TOTAL * 0.95: break
        q = random.choice(sq)
        r = api("search?part=snippet&type=playlist&maxResults=25&safeSearch=strict&relevanceLanguage=ar&regionCode=" + random.choice(["SA", "EG", "DZ", "AE"]) + "&fields=items(id/playlistId,snippet(title,channelTitle,thumbnails/medium/url))&q=" + urllib.parse.quote(q), 100)
        if not r: continue
        cand = {}
        for o in r.get("items", []):
            pid = o.get("id", {}).get("playlistId"); sn = o.get("snippet", {})
            ttl = clean(sn.get("title"))
            if not pid or pid in known_s or bad_text(ttl, sn.get("channelTitle", "")) or not ar_title(ttl) or RANGE.search(ttl): continue
            cand[pid] = [pid, ttl, clean(sn.get("channelTitle")), "0", sn.get("thumbnails", {}).get("medium", {}).get("url", "")]
        if not cand: continue
        cnt = api("playlists?part=contentDetails&fields=items(id,contentDetails/itemCount)&id=" + ",".join(cand))
        for o in (cnt or {}).get("items", []):
            n = o.get("contentDetails", {}).get("itemCount", 0)
            if 6 <= n <= 120 and o["id"] in cand:
                cand[o["id"]][3] = str(n)
        for pid, row in cand.items():
            if row[3] == "0": continue
            it = api("playlistItems?part=contentDetails&maxResults=50&fields=items(contentDetails/videoId)&playlistId=" + pid)
            ids = [i["contentDetails"]["videoId"] for i in (it or {}).get("items", [])]
            if len(ids) < 5: continue
            d = api("videos?part=snippet,status&fields=items(id,snippet/title,status/embeddable)&id=" + ",".join(ids))
            items = (d or {}).get("items", [])
            if len(items) < 5: continue
            emb = sum(1 for o in items if o.get("status", {}).get("embeddable", True))
            nums = [epnum(o["snippet"]["title"]) for o in items]
            have_n = [x for x in nums if x is not None]
            if emb >= len(items) * 0.8 and len(have_n) >= len(items) * 0.7 and len(set(have_n)) >= len(have_n) * 0.8:
                if build_eps(pid) >= 5:
                    avg = AVG.get(pid, 0)
                    # short serial clips are the goal: long episodes are kept only occasionally
                    if avg > 240 and random.random() > 0.2:
                        try: os.remove(os.path.join(ROOT, "s", pid + ".tsv"))
                        except OSError: pass
                        known_s.add(pid)
                        continue
                    series.append(row + [str(avg)]); known_s.add(pid)
    # backfill: older series that still have no episode file
    fixed = []
    for row in series:
        if spent >= TOTAL * 0.97: fixed.append(row); continue
        fp = os.path.join(ROOT, "s", row[0] + ".tsv")
        if os.path.exists(fp) or build_eps(row[0]) >= 5:
            if len(row) < 6 or not row[5].isdigit():
                try:
                    ds = [int(l.split("\t")[1]) for l in open(fp, encoding="utf-8") if l.count("\t") >= 2]
                    row = row[:5] + [str(sum(ds) // max(1, len(ds)))]
                except Exception:
                    row = row[:5] + ["999"]
            fixed.append(row)
    series = fixed
    def _avg(r): return int(r[5]) if len(r) > 5 and r[5].isdigit() else 999
    short_s = [r for r in series if _avg(r) <= 240]
    long_s = [r for r in series if _avg(r) > 240]
    # once enough short serials exist, long series shrink to a small minority (their episode files are removed)
    if len(short_s) >= 30:
        keep = max(12, len(short_s) // 4)
        for r in long_s[keep:]:
            try: os.remove(os.path.join(ROOT, "s", r[0] + ".tsv"))
            except OSError: pass
        long_s = long_s[:keep]
    # AI-generated story series come first, then the rest (each group keeps its order)
    AIW = ("ذكاء اصطناعي", "الذكاء الاصطناعي", "بالذكاء", " ai ", "ai-", "(ai)", "ai generated", "ai story", "مولد")
    def _ai(r): 
        t = (" " + r[1] + " ").lower()
        return 0 if any(w in t for w in AIW) else 1
    short_s.sort(key=_ai)
    long_s.sort(key=_ai)
    series = (short_s + long_s)[:600]
    open(sp2, "w", encoding="utf-8").write("\n".join("\t".join(s) for s in series) + "\n")
except Out:
    print("all keys exhausted: saving what we have")

# ---------------- write ----------------
for t, rows in shards.items():
    rows = rows[-(cap(t) + 400):]   # stable order (small git diffs); oldest clips fall off first
    open(os.path.join(ROOT, "v", t + ".tsv"), "w", encoding="utf-8").write("".join("\t".join(r) + "\n" for r in rows))
open(chp, "w", encoding="utf-8").write("".join("\t".join([c, v[0], str(v[1]), str(v[2]), str(v[3])]) + "\n" for c, v in channels.items()))
json.dump(state, open(sp, "w"))
tot = sum(len(r) for r in shards.values())
print("units", spent, "added", added_total, "total videos", tot, "channels", len(channels))
