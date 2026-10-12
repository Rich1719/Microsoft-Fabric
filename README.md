# Microsoft-Fabric
# End-to-End Data Pipeline: Medallion Architecture (Bronze  Power BI)

Dieses Repository dokumentiert unsere skalierbare Daten-Pipeline basierend auf dem **Medallion-Lakehouse-Design**. Das Projekt bildet den gesamten Lebenszyklus der Daten ab: Vom unbereinigten Daten-Ingest im **Bronze Layer** über die differenzierte Aufbereitung im **Silver Layer** bis hin zur performanten Modellierung im **Gold Layer** für **Power BI** und **Machine Learning (ML)**.

---

##  End-to-End Architektur & Datenfluss

Der Datenfluss ist so konzipiert, dass er Rohdaten sicher speichert, sie zentral bereinigt und anschließend maßgeschneidert an die Endkonsumenten (Business Intelligence und Data Science) verteilt.


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

---

## 🛠️ Die Pipeline-Phasen im Detail

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

---

##  PySpark Transformations-Code (Kernlogik)

```python
from pyspark.sql.functions import col, round, when, coalesce

# 1. Auflösung doppelter Join-Spalten & Selektion
cleaned_df = sales_with_promos.select(
    sales_base["sales_order_id"].alias("sales_order_id"),
    col("product_id"),
    col("product_name"),
    col("unit_price"),
    col("quantity_ordered"),
    col("promotion_id"),
    col("promotion_discount")
)

# 2. Basis-Berechnung (Silver Standard)
final_df = cleaned_df.withColumn(
    "gross_revenue", 
    round(col("quantity_ordered") * col("unit_price"), 2).cast("decimal(18,2)")
)

# --- PFAD A: MACHINE LEARNING (Rohzustand bleibt für Features erhalten) ---
ml_df = final_df.withColumn(
    "net_revenue",
    round(col("gross_revenue") - (col("quantity_ordered") * coalesce(col("promotion_discount"), col("gross_revenue") * 0)), 2).cast("decimal(18,2)")
)

# --- PFAD B: GOLD LAYER / POWER BI (Bereitstellung des Sternschemas) ---
bi_gold_df = ml_df.withColumn(
    "promotion_id", 
    when(col("promotion_id").isNull(), "Keine Promotion").otherwise(col("promotion_id"))
).withColumn(
    "promotion_discount", 
    coalesce(col("promotion_discount"), col("unit_price") * 0)
)

 Best Practices für das Power BI Frontend

1. Beziehungen: Nutze ausschließlich 1:n (One-to-Many) Beziehungen von den Dimensionstabellen zur Faktentabelle. Vermeide bidirektionale Filter, um die Performance zu maximieren.
2. DAX statt Spalten: Berechne hochaggregierte Kennzahlen (wie den Gesamten Nettoumsatz über alle Filialen) nicht als berechnete Spalte, sondern dynamisch als DAX-Measure:dax
Total Net Revenue = SUM(Fact_Sales[net_revenue])
Verwende Code mit Vorsicht.
3. Ausblenden von Schlüsseln: Blende technische IDs (z. B. product_key, customer_key) in der Berichtsansicht für Endanwender aus. Für Filter und Zeilenbeschriftungen werden ausschließlich die Attribute aus den Dimensionstabellen (Dim_Product[product_name]) genutzt.




