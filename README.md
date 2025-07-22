# YouTube Performance 📊 

This dbt project transforms raw YouTube API data from a central analytics data source into clean, tested, and documented data models for analyzing video performance, content types, and viewer engagement across multiple channels.

---

## 📦 Project Overview

This warehouse powers reporting, dashboards, and analysis that answer key questions like:

- Which videos or content types drive the most engagement?
- How do tags, promos, or highlights perform across channels?
- What videos are performing best by upload day, time, or topic?

---

## 🔧 Data Sources

This project is built on top of a single data source:

- **`raw_youtube`** (via `stage_youtube_api_metrics`)
  - Powered by the YouTube Data API
  - Contains raw video, channel, and metric-level fields

---

## 🧱 Model Layers

The project follows modern dimensional modeling and dbt best practices:

models/
├── staging/
├── dimensions/
├── bridge/
├── facts/
└── reports/

---

### ✅ Staging Models
- `stage_youtube_api_metrics`: Prepares raw YouTube API data

### ✅ Dimension Models
- `dim_video`: Video metadata and flags like `is_short`, `is_full_match`
- `dim_channel`: Channel-level info (name, subscribers)
- `dim_date`: Standard date dimension for joining with fact tables
- `dim_tag`: Unique video tags extracted from metadata

### ✅ Bridge Table
- `bridge_video_tag`: Many-to-many relationship between videos and tags

### ✅ Fact Table
- `fact_video_daily_performance`: Core metrics like views, likes, and engagement rate

### ✅ Reporting Models
- `report_video_daily_performance`: Flattened report-friendly view of daily video metrics
- `report_tag_daily_performance`: Tag-level attribution of engagement metrics

---

## 🧪 Data Testing

Data quality is validated using dbt tests, including:

- Unique & not null constraints on surrogate keys
- Relationship tests between fact & dimension models
- Optional accepted values or derived content-type classifications

---

## 📄 Documentation

Generate interactive documentation using:

---


Preview details about each model, column, tests, and relationships via the dbt Docs UI.

---

## 🚀 Usage

Typical workflow:

dbt run # Build all models
dbt test # Run all data quality tests
dbt docs generate # Build documentation site
dbt docs serve # Launch doc site at http://localhost:8080/

---

## 🏷️ Tags & Materialization

Models are organized using tags like:
- `dimension`, `fact`, `bridge`, `report`, `stage`

Materializations are configured in `dbt_project.yml`:
- Dimensions: `table`
- Fact/Report Tables: `table`
- Staging Models: `view`

---

## 🤝 Contributing

To contribute:
1. Fork the repo or work in a feature branch
2. Add your model or update existing logic
3. Run `dbt test` and `dbt docs generate` before submitting a PR

---

## 📬 Questions?

Feel free to reach out to Grapple Insights.

---

Made with ❤️ and dbt. Powered by the YouTube Data API.

---

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
