# Task: Chunked Vector Index mit Sparse+Dense Retrieval

## Problem

Die aktuelle Embedding-Pipeline hat drei Schwächen:

1. **Trunkierung statt Chunking.** `getEmbedding` schneidet bei 4000 Bytes ab.
   Alles danach existiert für die Suche nicht. Ein zwölfseitiger Bescheid ist
   durch seine ersten anderthalb Seiten repräsentiert — Entscheidungssatz,
   Rechtsbehelfsbelehrung, Anlagenverzeichnis sind unauffindbar.

2. **Byte-Slice statt Rune-Slice.** `text[:4000]` schneidet Bytes. Bei
   deutschen Texten mit Umlauten und ß landet der Schnitt regelmäßig mitten
   in einem UTF-8-Zeichen → U+FFFD.

3. **Englischzentrisches Modell.** nomic-embed-text-v1.5 für einen deutschen
   Verwaltungsbestand. Das Modell kann 8192 Token, genutzt werden ~1400.

Zusätzlich: ein Qdrant-Punkt pro Dokument. Verwaltungstexte bestehen zu
großen Teilen aus wortgleichen Textbausteinen (Rechtsbehelfsbelehrung,
Anschriftenkopf, Standardformulierungen). Ein Dokumentvektor mittelt den
einen unterscheidenden Absatz mit neunzehn identischen weg.

## Ziel

Ein Qdrant-Punkt pro Chunk. Strukturbasiertes Splitting. Dokumentkopf in
jedem Chunk. Dense + Sparse Vektoren im selben Punkt. Multilinguales Modell.
Inkrementeller Reindex über Chunk-Hashes.


## Architektur

### Qdrant-Schema (Named Vectors)

```
Collection: opencloud

Punkt:
  id:       UUID
  vectors:
    dense:  float[1024]        (Qwen3-Embedding-0.6B)
    sparse: SparseVector       (BM25/SPLADE, optional Phase 2)
  payload:
    doc_id:        string      (ResourceID, z.B. "storageid$spaceid!nodeid")
    chunk_index:   int
    chunk_text:    string      (für Snippet-Anzeige)
    chunk_hash:    string      (SHA-256, für inkrementellen Reindex)
    offset_start:  int         (Zeichenposition im Gesamttext)
    offset_end:    int
    page:          int         (wenn erkennbar, sonst 0)

    # Filter-Felder (auf jedem Chunk dupliziert)
    space_id:      string
    name:          string      (Dateiname)
    mime:          string
    mtime:         string      (RFC3339)
    path:          string

    # Optionale Metadaten (aus Taki DocMeta / xattrs)
    doc_type:      string      (letter, invoice, notice, ...)
    file_reference: string     (oy.fileReference / Aktenzeichen)
    subject:       string
    summary:       string      (LLM-Summary des Gesamtdokuments, nur chunk_index=0)
```

### Suche: query_points_groups

```
POST /collections/opencloud/points/query/groups
{
  "query": <dense_vector>,
  "using": "dense",
  "group_by": "doc_id",
  "group_size": 3,       // beste 3 Chunks pro Dokument
  "limit": 20,           // 20 Dokumente
  "score_threshold": 0.5,
  "with_payload": true,
  "filter": { ... }      // space_id, mime, etc.
}
```

Ergebnis: 20 Dokument-Gruppen mit je bis zu 3 Chunk-Treffern.
Kein clientseitiges Dedup nötig.

### Phase 2: Hybrid (Dense + Sparse)

```
POST /collections/opencloud/points/query/groups
{
  "prefetch": [
    { "query": <dense_vector>, "using": "dense", "limit": 100 },
    { "query": <sparse_vector>, "using": "sparse", "limit": 100 }
  ],
  "query": { "fusion": "rrf" },
  "group_by": "doc_id",
  "group_size": 3,
  "limit": 20
}
```

Dense findet semantisch ähnliche Texte, Sparse findet Aktenzeichen,
Paragraphen und Eigennamen exakt. RRF fusioniert die Rankings.


## Änderungen pro Repo

### 1. open_taki

#### Chunking (neu)

```go
type Chunk struct {
    Text        string  `json:"text"`
    Index       int     `json:"index"`
    OffsetStart int     `json:"offset_start"`
    OffsetEnd   int     `json:"offset_end"`
    Page        int     `json:"page"`
    Hash        string  `json:"hash"`
    Embedding   []float64 `json:"embedding,omitempty"`
}
```

**Splitting-Strategie (Priorität absteigend):**

1. Seitenumbruch (`\f`, Form Feed)
2. Doppel-Newline (Absatzgrenze)
3. Einzelner Newline nach Satzende (`. \n`)
4. Fallback: Token-Fenster (1000 Token, 100 Token Overlap)

**Ziel-Chunkgröße:** 800–1200 Token. Nicht unter 300 Token (zu wenig Kontext).
Zu kleine Reste an den vorherigen Chunk anhängen.

**Dokumentkopf:** Jedem Chunk werden 2-3 Zeilen Kontext vorangestellt,
extrahiert aus den ersten Zeilen des Dokuments oder aus DocMeta:

```
[Bescheid | AZ 11.12.345 | 2024-03-15 | Baugenehmigung Hauptstr. 7]
```

Damit ist ein Chunk von Seite 7 mit dem Satz „dem Antrag wird nicht
entsprochen" per Embedding auffindbar.

#### getEmbedding → getChunkEmbeddings

```go
func (s *Server) getChunkEmbeddings(text string, meta *takiDocMeta) []Chunk {
    // 1. Strukturbasiert splitten
    // 2. Dokumentkopf erzeugen (aus meta oder ersten Zeilen)
    // 3. Kopf + Chunk-Text → Embedding-Call pro Chunk
    // 4. SHA-256 Hash pro Chunk-Text (ohne Kopf, für Änderungserkennung)
    // 5. []Chunk zurückgeben
}
```

UTF-8-sauber: Alle Schnitte über `[]rune`, nie `text[:n]` auf Bytes.

#### Response-Format (v2)

Bisher:
```json
{ "X-TAKI:embedding": [0.1, 0.2, ...] }
```

Neu:
```json
{
  "X-TAKI:chunks": [
    {
      "text": "chunk text (mit Kopf)",
      "index": 0,
      "offset_start": 0,
      "offset_end": 1847,
      "page": 1,
      "hash": "a3f2...",
      "embedding": [0.1, 0.2, ...]
    },
    { ... }
  ]
}
```

`X-TAKI:embedding` bleibt für Abwärtskompatibilität (Embedding des ersten Chunks).

#### /embed Endpoint (Query-Embedding)

Kein Chunking. Nur Modell-spezifisches Prefix (z.B. `search_query:` bei nomic,
bei Qwen3-Embedding kein Prefix nötig). Einmaliger Embedding-Call.

#### Config-Erweiterung

```yaml
embedding:
  api_base: "http://microllm:8012/v1"
  model: "local-embed"
  max_chunk_tokens: 1000    # Ziel-Chunkgröße (default: 1000)
  chunk_overlap_tokens: 100 # Overlap (default: 100)
  doc_prefix: ""            # z.B. "search_document: " für nomic
  query_prefix: ""          # z.B. "search_query: " für nomic
```

### 2. opencloud (search service)

#### Qdrant-Speicherung (service.go)

Bisher: ein `Upsert` pro Dokument mit einem Vektor.

Neu:
```
1. Alle Punkte mit doc_id löschen (Filter-Delete)
2. Pro Chunk: Punkt mit Payload erzeugen
3. Batch-Upsert aller Chunk-Punkte
```

Das Delete-vor-Insert ist Pflicht — ohne sammeln sich Waisen-Chunks,
und gelöschte Dokumente tauchen weiter in der Suche auf (Löschfristen).

#### Qdrant-Suche (service.go)

Bisher: `search_points` → Liste von Dokumenten.

Neu: `query_points_groups` mit `group_by: "doc_id"`.

Response-Mapping:
- Gruppe = Dokument
- Punkte in Gruppe = Chunk-Treffer (mit chunk_text für Snippets)
- Score = bester Chunk-Score der Gruppe

#### Collection-Init

Beim Start prüfen ob Collection das richtige Schema hat.
Wenn nicht (alter Single-Vector-Index): Collection droppen, neu anlegen.

```go
vectors_config: {
    "dense": { size: 1024, distance: "Cosine" }
}
// Phase 2:
// sparse_vectors_config: {
//     "sparse": {}
// }
```

#### Taki-Response parsen

`doc.Taki.Embed []float64` → `doc.Taki.Chunks []Chunk`

Fallback: wenn `Embed` gesetzt aber `Chunks` leer → alter Taki,
Single-Vektor wie bisher.

### 3. Infrastruktur

#### Modellwechsel: Qwen3-Embedding-0.6B

**Entscheidung:** Qwen3-Embedding-0.6B (nicht 4B).

| Modell | Dims | Token | Größe | Anmerkung |
|--------|------|-------|-------|-----------|
| **Qwen3-Embedding-0.6B** | 1024 | 8192 | 0.6B | **gewählt** |
| Qwen3-Embedding-4B | 2560 | 8192 | 4B | zu groß für Iterationsgeschwindigkeit |
| bge-m3 | 1024 | 8192 | 568M | Sparse/ColBERT-Köpfe nicht über vLLM nutzbar |
| multilingual-e5-large | 1024 | 512 | 560M | nur 512 Token Kontext |
| nomic-embed-text-v2-moe | 768 | 8192 | 475M | englischzentriert |

**Begründung:**

1. **Passt ohne Umbau in den Call.** vLLM serviert es nativ über die
   OpenAI-kompatible /embeddings-Route. `getEmbedding` ändert sich nur
   um den Instruct-Prefix auf der Query-Seite. bge-m3 wäre eine Falle:
   die Sparse- und ColBERT-Köpfe (das eigentliche Verkaufsargument)
   brauchen FlagEmbedding oder Infinity als Server — über vLLMs
   Standard-Endpunkt bekommt man sie nicht. Man würde entweder ein
   zweites Serving-Setup betreiben oder das Feature bezahlen, ohne es
   zu bekommen.

2. **1024 Dimensionen statt 2560.** Bei ~100k Dokumenten × 8 Chunks
   sind das 3,2 GB statt 8 GB — mit Scalar-Quantisierung (int8) nochmal
   ein Viertel. Der Unterschied ist nicht existenziell, aber die 4B
   kauft ihn nur mit ein paar MTEB-Punkten ab.

3. **Reindex-Durchsatz.** Das Chunking ist ungetestet, die Dokumentkopf-
   Idee ist ungetestet, die Chunk-Größe ist geraten. Der Index wird in
   den nächsten Wochen drei- bis viermal neu gebaut. 0.6B macht das über
   Nacht, 4B lässt zögern — und ein Setup, bei dem man das Experiment
   scheut, wird nicht gut. Der Modellsprung von 0.6B auf 4B bringt
   vielleicht zwei bis drei MTEB-Punkte; das richtige Chunking bringt
   zweistellig.

4. **Instruct-Effekt ist kleiner als gedacht.** Qwen-Tests zeigen
   typischerweise 1–5% Verbesserung durch Instruktionen gegenüber ohne.
   Mitnehmen, aber nicht daran optimieren.

**Gegenkandidat:** bge-m3, wenn Lizenzklarheit wichtiger ist als alles
andere. MIT-lizenziert, für deutschsprachige RAG-Stacks als robusteste
Wahl dokumentiert, 8192 Token. Aber: Sparse/ColBERT nur mit eigenem
Serving-Stack (FlagEmbedding/Infinity), nicht über vLLM.

#### Vorgehen

1. Modell auf einer GPU deployen (vLLM)
2. microllm-Alias `local-embed` umbiegen
3. Taki-Config: `embedding.model` + Prefixes anpassen
4. Qdrant Collection droppen (Dimensionsänderung = Schema-Bruch)
5. Reindex

#### Testset

Vor dem Umbau 30 echte Suchanfragen mit erwarteten Treffern dokumentieren.
Nach dem Reindex gegen den neuen Index laufen lassen. Ohne Testset ist
nicht messbar, welche der Änderungen etwas gebracht hat.

```
# testset.jsonl
{"query": "Baugenehmigung Hauptstraße 7", "expected": ["doc_id_1", "doc_id_2"]}
{"query": "Gewerbeabmeldung Stritzke", "expected": ["doc_id_3"]}
{"query": "11.12.345", "expected": ["doc_id_4", "doc_id_5"]}
...
```


## Enrich-Event (Entkopplung Upload → Taki)

### Problem

Upload-Events (`FileUploaded`/`UploadReady`) triggern nur `doFastIndex`
(Bleve, Metadaten). Taki-Extraktion (LLM, Embedding) passiert erst beim
manuellen Reindex oder Re-Enrich. Neue Dateien sind sofort per Name
suchbar, aber ohne semantischen Content und ohne LLM-Metadaten.

Der direkte Weg (UpsertItem im Upload-Handler) überlastet bei Massen-
Uploads GPU + Jetstream — das gleiche Problem wie beim Re-Enrich.

### Lösung: Internes Enrich-Event

```
Upload → FileUploaded Event
  → Search Consumer:
      1. doFastIndex (Bleve, sofort)
      2. publish EnrichNeeded{Ref, Priority} auf eigene Queue
  → Enrich Consumer (separater Worker-Pool):
      1. pick EnrichNeeded
      2. doUpsertItem (Taki + Qdrant)
      3. rate-limited (max N parallel, konfigurierbar)
```

Zwei getrennte Queues:
- `main-queue` (bestehend): Events → Bleve-Index (schnell)
- `enrich-queue` (neu): EnrichNeeded → Taki-Extraktion (langsam, throttled)

### Vorteile

- Upload sofort suchbar (Name, Metadaten, Tags)
- Taki-Extraktion asynchron, überlastet nichts
- Bei Massen-Upload staut sich nur die Enrich-Queue
- Jetstream-Überlauf betrifft nur Enrich-Queue, nicht Hauptqueue
- Re-Enrich CLI wird obsolet (jede Datei enriched sich selbst)

### Konfiguration

```
SEARCH_ENRICH_WORKERS=4          # parallele Taki-Calls (default: 4)
SEARCH_ENRICH_QUEUE=enrich-queue # eigener NATS-Stream
```

### Priorität

EnrichNeeded kann Priority tragen:
- `high`: UI-Reindex-Button (User wartet)
- `normal`: Upload (Hintergrund)
- `low`: Re-Enrich Batch

### Was sich ändert

| Trigger | Heute | Mit Enrich-Event |
|---------|-------|------------------|
| Upload | doFastIndex (kein Taki) | doFastIndex + EnrichNeeded |
| UI Reindex | UpsertItem (Taki) | EnrichNeeded{priority=high} |
| CLI --force-rescan | ReEnrichSpace | EnrichNeeded pro File |
| Favorit setzen | UpsertItem (Taki!) | UpsertItem (kein Taki nötig, nur Bleve) |

Favorit/Tag-Events brauchen kein Taki — nur Bleve-Update. Heute
machen sie trotzdem einen vollen UpsertItem mit Taki-Call. Das
sollte auf doFastIndex reduziert werden.


## Reihenfolge

```
1. Testset anlegen (30 Anfragen, erwartete Treffer notieren)
2. Modell wählen + auf GPU deployen  ← ERLEDIGT (Qwen3-Embedding-0.6B)
3. open_taki: Chunking + neues Response-Format + /embed Prefix
4. opencloud: Qdrant auf Chunks + Groups umstellen, Delete-Pipeline
5. opencloud: Enrich-Event (entkoppelt Upload → Taki)
6. Qdrant Collection droppen + Reindex
7. Testset gegen neuen Index laufen lassen
8. Phase 2: Sparse Vektoren (BM25/SPLADE)
```

Schritt 3, 4 und 5 sind parallelisierbar (verschiedene Repos/Subsysteme).
Schritt 6 braucht 3+4.


## Nicht-Ziele (bewusst ausgeklammert)

- Bleve-Index bleibt wie er ist (Metadaten, Tags, Favorites)
- Kein Austausch der Bleve-Engine
- Kein clientseitiges Chunking (alles serverseitig in Taki)
- Kein Multi-Vector/MaxSim (keine Chunk-Rückverfolgung möglich)
- Kein Mean-Pooling (Textbaustein-Problem bei Verwaltungstexten)
