// =========================================
// Smart Agriculture Big Data Project
// Spark SQL Queries
// =========================================


// =========================================
// Show Databases
// =========================================

spark.sql("SHOW DATABASES").show(false)


// =========================================
// Use Database
// =========================================

spark.sql("USE smart_agriculture")


// =========================================
// Show Tables
// =========================================

spark.sql("SHOW TABLES").show(false)


// =========================================
// Count Sensor Records
// =========================================

spark.sql("""
SELECT COUNT(*) AS total_records
FROM plant_sensors_500k
""").show()


// =========================================
// Plant Health Distribution
// =========================================

spark.sql("""
SELECT
Plant_Health_Status,
COUNT(*) AS total
FROM plant_sensors_500k
GROUP BY Plant_Health_Status
""").show(false)


// =========================================
// View Plant Health Summary
// =========================================

spark.sql("""
SELECT *
FROM plant_health_summary_500k
""").show(false)


// =========================================
// View Daily Summary
// =========================================

spark.sql("""
SELECT *
FROM daily_summary_500k
""").show(false)


// =========================================
// View Plant Summary
// =========================================

spark.sql("""
SELECT *
FROM plant_summary_500k
LIMIT 10
""").show(false)


// =========================================
// Top Plants by Number of Readings
// =========================================

spark.sql("""
SELECT *
FROM plant_summary_500k
ORDER BY readings DESC
LIMIT 10
""").show(false)


// =========================================
// Moisture Alerts
// =========================================

spark.sql("""
SELECT *
FROM moisture_alerts_500k
LIMIT 20
""").show(false)


// =========================================
// Temperature Summary
// =========================================

spark.sql("""
SELECT *
FROM temperature_summary_500k
""").show(false)


// =========================================
// Nutrient Summary
// =========================================

spark.sql("""
SELECT *
FROM nutrient_summary_500k
""").show(false)


// =========================================
// pH Summary
// =========================================

spark.sql("""
SELECT *
FROM ph_summary_500k
""").show(false)


// =========================================
// Light Summary
// =========================================

spark.sql("""
SELECT *
FROM light_summary_500k
""").show(false)


// =========================================
// Chlorophyll Summary
// =========================================

spark.sql("""
SELECT *
FROM chlorophyll_summary_500k
""").show(false)


// =========================================
// Electrochemical Signal Summary
// =========================================

spark.sql("""
SELECT *
FROM signal_summary_500k
""").show(false)


// =========================================
// Plant Health Count
// =========================================

spark.sql("""
SELECT *
FROM plant_health_count_500k
""").show(false)


// =========================================
// Average Soil Moisture
// =========================================

spark.sql("""
SELECT
ROUND(AVG(Soil_Moisture),2) AS avg_soil_moisture
FROM plant_sensors_500k
""").show()


// =========================================
// Average Temperature
// =========================================

spark.sql("""
SELECT
ROUND(AVG(Ambient_Temperature),2) AS avg_temperature
FROM plant_sensors_500k
""").show()


// =========================================
// Average Humidity
// =========================================

spark.sql("""
SELECT
ROUND(AVG(Humidity),2) AS avg_humidity
FROM plant_sensors_500k
""").show()


// =========================================
// Number of Moisture Alerts
// =========================================

spark.sql("""
SELECT COUNT(*)
FROM moisture_alerts_500k
""").show()


// =========================================
// Verify Export Tables
// =========================================

spark.sql("SHOW TABLES").show(false)
