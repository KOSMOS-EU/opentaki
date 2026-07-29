# Task: Dokumentenklassifikation mit typ-spezifischer Extraktion

## Problem

Aktuell prüft `is_letterhead` (bool) ob DocMeta extrahiert wird.
Das ist zu grob — ein Scanner-Beleg, eine Rechnung oder ein Foto
brauchen andere Metadaten als ein Brief.

## Ziel

1. **Dokumenttyp-Klassifikation** statt `is_letterhead` bool
2. **Typ-spezifische Prompts** für die Metadaten-Extraktion
3. **Erweiterbare Typ-Liste** ohne Code-Änderung

## Dokumenttypen (Entwurf)

```
letter          - Brief (mit Briefkopf, Absender, Empfänger)
invoice         - Rechnung / Lieferschein
receipt         - Beleg / Quittung
notice          - Mahnung / Bescheid
contract        - Vertrag
form            - Formular / Antrag
report          - Bericht / Protokoll
certificate     - Bescheinigung / Zeugnis / Ausweis
sketch          - Skizze / technische Zeichnung
photo_person    - Foto (Person/Porträt)
photo_landscape - Foto (Landschaft/Gebäude)
photo_document  - Foto eines Dokuments (Scan-Ersatz)
spreadsheet     - Tabelle / Kalkulation
presentation    - Präsentation
email           - E-Mail (.msg/.eml)
other           - Sonstiges
```

## Fragen

- Wie granular? `invoice` vs. `invoice_incoming` / `invoice_outgoing`?
- Soll der Typ vom LLM erkannt werden oder regelbasiert (MIME + Heuristik)?

## Etablierte Standards (Recherche)

- **Schema.org** — `schema:DigitalDocument`, `schema:Invoice`, `schema:Letter` etc.
  Standardisiert, erweiterbar, als Basis für Typ-Liste geeignet.
- **Dublin Core (ISO 15836)** — 15 Basis-Metadaten (title, creator, subject, date, type).
  Sehr generisch, eher für Metadaten-Keys als für Typ-Klassifikation.
- **CMIS (OASIS)** — `cmis:document`, `cmis:folder` mit Properties.
  OpenYard nutzt CMIS 1.1 schon. Dokumenttypen aber nicht granular.
- **DIN 6763** — Aktenplan-Systematik für deutsche Kommunalverwaltung.
  Relevant für Brandis (Bescheid, Verfügung, Satzung, Niederschrift).
- **DCAT** — Datenkatalog-Vokabular, eher für Open Data als für DMS.

**Empfehlung**: Schema.org als Basis + DIN 6763 für Verwaltungs-Typen + Custom für Scanner/DMS.
Recherche der konkreten Schema.org Document-Types und Mapping auf Verwaltungs-Typen steht aus.

## Architektur-Idee

```
1. Klassifikation (LLM oder Heuristik)
   Input:  Dateiname, MIME-Type, erste 500 Zeichen Content
   Output: document_type (string)

2. Typ-spezifischer Prompt
   prompts/
     letter.txt        - Absender, Empfänger, Datum, Aktenzeichen, Betreff
     invoice.txt       - Rechnungsnummer, Betrag, Fällig, Lieferant
     receipt.txt       - Betrag, Datum, Zahlungsart
     photo_person.txt  - Name, Kontext
     default.txt       - Fallback für unbekannte Typen

3. Schema pro Typ
   schemas/
     letter.json       - erwartete Metadaten-Keys
     invoice.json
     ...

4. Extraktion
   Taki wählt Prompt + Schema basierend auf document_type
   Ergebnis: docmeta mit typ-spezifischen Feldern
```

## Konfiguration (config.yaml)

```yaml
classification:
  method: llm           # llm | rules | hybrid
  model: local-ocr      # welches LLM für Klassifikation
  types_file: /config/document_types.yaml

extraction:
  prompts_dir: /config/prompts/
  schemas_dir: /config/schemas/
```

## Vorteile

- Kein `is_letterhead` mehr — jedes Dokument bekommt seinen Typ
- Neue Typen ohne Code-Änderung (nur YAML + Prompt + Schema)
- Prompt-Qualität pro Typ optimierbar
- Schema pro Typ → saubere Metadaten, keine irrelevanten Felder
- Typ wird als Metadatum gespeichert → filterbar in der UI

## Abhängigkeiten

- open_taki: Klassifikation + typ-spezifische Extraktion
- opencloud: `document_type` als Metadatum im Index
- opencloud_web/folderviews: Typ-Icon/Filter in der UI (optional)

## Aufwand

- Phase 1: Klassifikation + document_type Metadatum (open_taki)
- Phase 2: Typ-spezifische Prompts/Schemas (open_taki config)
- Phase 3: UI-Integration (opencloud_web)
