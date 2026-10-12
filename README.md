# Microsoft-Fabric
# End-to-End Data Pipeline: Medallion Architecture (Databricks retail_org `--->` Power BI)

##  Projektkontext & Datenherkunft
Dieses Projekt nutzt die Datenstrukturen aus der Databricks-Beispielorganisation **`retail_org`** (einschließlich transaktionaler Verkaufsdaten und Marketing-Promotions). Die Pipeline bildet den gesamten Lebenszyklus dieser Daten ab: Vom unbereinigten Daten-Ingest im **Bronze Layer** über die differenzierte Aufbereitung im **Silver Layer** bis hin zur performanten Modellierung im **Gold Layer** für **Power BI** und **Machine Learning (ML)**.

---

##  Ziel des Projektes
Das Hauptziel dieses Projekts ist es, die operationalen, verstreuten Rohdaten der Verkaufsstellen (`retail_org`) in strategische Unternehmenswerte zu verwandeln. Dabei löst das Projekt ein Kernproblem moderner Datenarchitekturen: **Die gleichzeitige, widerspruchsfreie Bereitstellung derselben Datenbasis für zwei unterschiedliche Business-Zwecke:**

1. **Umsatz- und Kampagnenanalyse (Business Intelligence):**
   * **Ziel:** Erstellung eines performanten, fehlerfreien **Power BI Dashboards** im Sternschema.
   * **Business-Value:** Das Management erhält sofortigen Einblick in die Profitabilität, den Brutto- vs. Nettoumsatz sowie den Erfolg von Marketing-Promotions, ohne durch Systemfehler (`NaN`) oder leere Filterzeilen (`Null`) behindert zu werden.

2. **Kaufverhaltens- und Nachfrageprognose (Machine Learning):**
   * **Ziel:** Bereitstellung mathematisch unverfälschter Datensätze für Data Scientists.
   * **Business-Value:** Vorhersagemodelle (z. B. für Absatzprognosen oder Churn-Analysen) können exakt lernen, wie Kunden auf Rabatte reagieren. Da fehlende Promotionswerte (`Null`) als wichtiges statistisches Signal erhalten bleiben, wird eine künstliche Verzerrung des Modells (Bias) verhindert.

---

##  End-to-End Architektur & Datenfluss

Der Datenfluss ist so konzipiert, dass er Rohdaten sicher speichert, sie zentral bereinigt und anschließend maßgeschneidert an die Endkonsumenten (Business Intelligence und Data Science) verteilt.

```
[  BRONZE LAYER ]  --> Rohdaten-Ingest (Append-only, Parquet/Delta, inkl. Fehlern & Nulls)
│
▼ (Deduplizierung, Type-Casting & Join über sales_order_id + product_id)
[  SILVER LAYER ]  --> Bereinigter Datenkern (Single Source of Truth, strukturierte Schemas)
│
├──► [  ML-PFAD ] ──────► Echte Null/NaN-Werte bleiben als mathematisches Signal intakt.
│
▼ (Imputation & Sternschema-Transformation)
[  GOLD LAYER ]    --> Datenmodellierung bereit für BI-Konsum (Delta / Fact & Dim Tables)
│
▼ (DirectQuery / Import)
[  POWER BI ]      --> Sternschema (Star Schema), DAX-Measures & optimiertes Reporting
```
---

##  Die Pipeline-Phasen im Detail

### 1.  Bronze Layer (Raw Ingest)
* **Charakteristik:** Unveränderte Rohdaten direkt aus den Quellsystemen (ERP, CRM).
* **Technologie:** Delta Lake (Append-only).
* **Zustand:** Enthält duplizierte Einträge, unformatierte Datumsangaben sowie technische `NaN`- und `Null`-Werte. Spaltennamen wie `sales_order_id` existieren redundant in den Quelltabellen.

### 2.  Silver Layer (Cleaned & Enriched)
Hier findet die technische Konsolidierung statt. Um den Konflikt zwischen den Anforderungen von **BI** (benötigt geschlossene Datenzeilen) und **ML** (benötigt rohe Fehlwerte als Signal) zu lösen, gilt folgende Pipeline-Logik:
* **Spalten-Bereinigung:** Doppelte IDs aus den Quelltabellen werden über explizite Objektreferenzen (`sales_base["sales_order_id"]`) aufgelöst, um `Ambiguous-Column`-Fehler in PySpark zu verhindern.
* **Finanzmathematische Präzision:** Der Bruttoumsatz (`gross_revenue`) wird als `quantity_ordered * unit_price` berechnet, kaufmännisch gerundet und strikt als `decimal(18,2)` gecastet, um Rundungsfehler zu vermeiden.
* **Trennlinie für ML:** Data Scientists greifen direkt auf die Ausfahrt des Silver Layers zu. Lücken in den Spalten `promotion_id` oder `promotion_discount` bleiben echte `Null`-Werte, da das Fehlen einer Promotion eine statistisch relevante Information für ML-Modelle ist.

### 3.  Gold Layer &  Power BI Modellierung (Business Ready)
Für das Reporting werden die Daten im Gold Layer final in ein performantes **Sternschema (Star Schema)** überführt. Hier werden alle verbliebenen `Null`-Werte für BI-Konsumenten restlos beseitigt.

####  Datenmodellierung im Power BI / Gold-Standard:
* **Fakten-Tabelle (`Fact_Sales`):** Enthält Kennzahlen wie Mengen, Preise und den errechneten Umsatz. Alle `Null`-Werte in den Discount-Spalten werden per `coalesce(promotion_discount, 0.00)` standardisiert.
* **Dimensions-Tabellen (`Dim_Customer`, `Dim_Product`, `Dim_Promotion`):** 
  * Verwaiste oder fehlende IDs werden mit Standard-Einträgen abgefangen. 
  * Wenn ein Kauf ohne Promotion stattfand, wird die `promotion_id` zu `"Keine Promotion"` transformiert. Dadurch entstehen in Power BI saubere Slicer (Filter), ohne dass leere Zeilen die Optik stören.
* **Datumsdimension (`Dim_Date`):** Das Feld `order_date` wird mit einer zentralen Kalendertabelle verknüpft, um Time-Intelligence-Funktionen (YoY, YTD) in Power BI stabil zu halten.
----




