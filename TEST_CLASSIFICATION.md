# Klassifikationstest

Testet die Dokumenttyp-Erkennung (`doc.type`) gegen einen realen
OpenCloud-Dokumentenbestand. Das Script `taki-test.py` lädt Dateien
per Graph API / WebDAV, schickt sie an taki und zeigt das Ergebnis.

## Voraussetzungen

- SSH-Zugang zu cloud.brandis.eu
- `taki-test.py` liegt auf dem Host unter `/nu/container/cloud_brandis/compose/taki-test.py`
  (liegt im Compose-Dir, wird von nu-compose nicht verarbeitet, aber persistent)
- tika-Service läuft im Pod `cloud_brandis`

## Aufruf

```bash
# Vom Entwicklungsrechner:
ssh root@cloud.brandis.eu '
  PID=$(podman inspect --format "{{.State.Pid}}" systemd-cloud_brandis-opencloud)
  nsenter -t $PID -n -- python3 /nu/container/cloud_brandis/compose/taki-test.py \
    --drive "Archikart DMS" --limit 10
'
```

`nsenter` ist nötig, weil tika (Port 9998) nur im Pod-Netzwerk erreichbar ist.

## Optionen

| Option | Default | Beschreibung |
|--------|---------|-------------|
| `--drive NAME` | Posteingang | Name des OpenCloud-Drives |
| `--limit N` | 10 | Max. Dateien testen |
| `--depth N` | 3 | Max. Ordnertiefe beim Scannen |
| `--list-drives` | | Nur Drives auflisten |
| `--list-files` | | Nur Dateien auflisten (kein taki-Call) |

## Beispiele

```bash
# Drives anzeigen
nsenter ... -- python3 taki-test.py --list-drives

# Dateien eines Drives auflisten (ohne Extraktion)
nsenter ... -- python3 taki-test.py --drive "Archikart DMS" --list-files

# 20 Dateien testen
nsenter ... -- python3 taki-test.py --drive "Archikart DMS" --limit 20

# Mehrere Drives testen
for drive in "Archikart DMS" "Innere Verwaltung" "Sicherheit und Ordnung" "Bibliothek"; do
  echo "=== $drive ==="
  nsenter ... -- python3 taki-test.py --drive "$drive" --limit 5
done
```

## Ausgabe

```
  Dateiname.pdf (application/pdf, 937923 bytes)...  type=vertrag  date=2000-11-08  sender=Stadt Brandis  source=vision  uncertain=0

============================================================
SUMMARY: 10 files tested
  vertrag               6
  no-docmeta            4
```

Jede Zeile zeigt: `doc.type`, `doc.date`, `sender.company`, Extraktionsquelle (`vision`/`vision_p2`/`merged`), Anzahl unsicherer Felder.

## Script aktualisieren

Bei Änderungen am Script:
```bash
scp taki-test.py root@cloud.brandis.eu:/nu/container/cloud_brandis/compose/taki-test.py
```

## Testergebnisse (qwen3-doctype-1.0, 2026-07-29)

| Drive | Dateien | Ergebnis |
|-------|---------|----------|
| Archikart DMS | 10 | 6x `vertrag`, 1x `no-docmeta` (MSG), 3x Timeout (LLM unter Last) |
| Innere Verwaltung | 1 | 1x `mitteilung` |
| Sicherheit und Ordnung | 1 | 1x `brief` (source=vision_p2) |
| Bibliothek | 5 | 1x `foto` (JPEG), 4x kein DocMeta (HTML, Markdown, drawio) |
| Rescues | 1 | kein DocMeta (Shell-Script) |
| Magic | 4 | kein DocMeta (Textdateien) |

Alle visuellen Dokumente korrekt klassifiziert. Nicht-visuelle Formate
(Text, Markdown, Scripts) korrekt ohne DocMeta.
