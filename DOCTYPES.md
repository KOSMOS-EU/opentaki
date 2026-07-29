# Dokumenttypen (doc.type)

open_taki klassifiziert jedes Dokument anhand seines Inhalts in einen der
folgenden Typen. Der Typ wird als `doc.type` im DocMeta-Ergebnis zurückgegeben
und bestimmt, welche Metadaten-Felder extrahiert werden.

## Verwaltung (DIN 6763)

| Typ | Beschreibung | Extrahierte Felder |
|-----|-------------|-------------------|
| `brief` | Brief mit Briefkopf | sender.*, recipient.*, doc.subject, doc.date, doc.reference |
| `bescheid` | Bescheid / Verwaltungsakt | sender.*, recipient.*, doc.subject, doc.date, doc.reference |
| `verfuegung` | Verfügung / Anordnung | sender.*, recipient.*, doc.subject, doc.date, doc.reference |
| `satzung` | Satzung / Verordnung | sender.*, doc.subject, doc.date, doc.reference |
| `niederschrift` | Niederschrift / Protokoll | sender.*, doc.subject, doc.date, doc.reference |
| `antrag` | Antrag / Formular | sender.*, recipient.*, doc.subject, doc.date, doc.reference |
| `vertrag` | Vertrag | sender.*, recipient.*, doc.subject, doc.date, doc.reference |
| `mitteilung` | Mitteilung / Information | sender.*, recipient.*, doc.subject, doc.date, doc.reference |
| `einladung` | Einladung | sender.*, recipient.*, doc.subject, doc.date |
| `kuendigung` | Kündigung | sender.*, recipient.*, doc.subject, doc.date, doc.reference |
| `angebot` | Angebot / Kostenvoranschlag | sender.*, doc.subject, doc.date, doc.reference, amounts.* |

## Buchhaltung

| Typ | Beschreibung | Extrahierte Felder |
|-----|-------------|-------------------|
| `rechnung` | Rechnung | sender.company, amounts.*, doc.reference (Rechnungsnr.), doc.date |
| `lieferschein` | Lieferschein | sender.company, doc.reference, doc.date |
| `kontoauszug` | Kontoauszug | sender.company, amounts.total, amounts.currency, doc.date |
| `kassenbon` | Kassenbon / Quittung | sender.company, amounts.total, amounts.currency, doc.date |
| `ec_bon` | EC-Beleg / Kartenzahlung | sender.company, amounts.total, amounts.currency, doc.date |
| `mahnung` | Mahnung / Zahlungserinnerung | sender.*, amounts.*, doc.reference, doc.date |
| `gutschrift` | Gutschrift | sender.company, amounts.*, doc.reference, doc.date |

## Urkunden / Bescheinigungen

| Typ | Beschreibung | Extrahierte Felder |
|-----|-------------|-------------------|
| `urkunde` | Urkunde / Bescheinigung | sender.*, doc.subject, doc.date, doc.reference |
| `zeugnis` | Zeugnis / Zertifikat | sender.*, doc.subject, doc.date, doc.reference |

## Medien / Sonstiges

| Typ | Beschreibung | Extrahierte Felder |
|-----|-------------|-------------------|
| `foto` | Foto / Bild | doc.subject (Bildbeschreibung) |
| `skizze` | Skizze / technische Zeichnung | doc.subject (Bildbeschreibung) |
| `tabelle` | Tabelle / Kalkulation | doc.subject |
| `praesentation` | Präsentation | doc.subject |
| `email` | E-Mail | sender.email, sender.given_name, sender.family_name, doc.subject, doc.date |
| `sonstiges` | Nicht zuordenbar | was erkennbar ist |

## Feldgruppen

### doc.*
| Feld | Typ | Beschreibung |
|------|-----|-------------|
| `doc.subject` | string/null | Betreffzeile (oder zusammengefasst, dann subject_inferred=true) |
| `doc.subject_inferred` | boolean | true wenn Betreff vom LLM zusammengefasst wurde |
| `doc.type` | string/null | Dokumenttyp (siehe Tabellen oben) |
| `doc.date` | string/null | Dokumentdatum (ISO 8601: YYYY-MM-DD) |
| `doc.reference` | string/null | Aktenzeichen / Rechnungsnummer des Absenders |

### sender.*
| Feld | Typ | Beschreibung |
|------|-----|-------------|
| `sender.company` | string/null | Organisation / Firma / Behörde |
| `sender.given_name` | string/null | Vorname (natürliche Person) |
| `sender.family_name` | string/null | Nachname |
| `sender.street` | string/null | Straße |
| `sender.house_number` | string/null | Hausnummer |
| `sender.postal_code` | string/null | PLZ |
| `sender.sub_locality` | string/null | Ortsteil |
| `sender.city` | string/null | Stadt |
| `sender.country` | string/null | Land (ISO 3166-1 alpha-2, z.B. "DE") |
| `sender.email` | string/null | E-Mail |
| `sender.phone` | string/null | Telefon |

### recipient.*
| Feld | Typ | Beschreibung |
|------|-----|-------------|
| `recipient.company` | string/null | Empfänger-Organisation |
| `recipient.given_name` | string/null | Empfänger-Vorname |
| `recipient.family_name` | string/null | Empfänger-Nachname |

### amounts.*
| Feld | Typ | Beschreibung |
|------|-----|-------------|
| `amounts.total` | string/null | Bruttobetrag |
| `amounts.tax` | string/null | MwSt-Betrag |
| `amounts.currency` | string/null | Währung (ISO 4217, z.B. "EUR") |
| `amounts.payment_due` | string/null | Fälligkeitsdatum (YYYY-MM-DD) |

## Prinzip

Felder die nicht zum Dokumenttyp gehören, werden auf `null` gesetzt und
nicht im DMS gespeichert. So bekommt jeder Dokumenttyp nur seine
relevanten Metadaten.

## Erweiterung

Neue Typen werden im `doc.type` Enum in `docmeta_schema.json` ergänzt.
Die Extraktionsregeln stehen im `docmeta_prompt.txt`. Beides ohne
Code-Änderung anpassbar.
