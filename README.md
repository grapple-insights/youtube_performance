# Pro Wrestling Events Analytics dbt Project

## Overview

This dbt project models professional wrestling event data for analytics and reporting. It currently focuses on **WWE and AEW events from 2019-2025**. It implements a classic star schema, with clear separation between staging, dimension, fact, and reporting layers. The project is designed for flexibility, scalability, and ease of use in BI tools.

---

## Project Structure 

This project is organized into logical layers, following best practices for modular, maintainable analytics engineering. Each layer serves a distinct purpose in the data pipeline:

1. Staging Layer (models/stage/)
Purpose: Prepares raw source data for downstream modeling.

Example:

stage_pro_wrestling_events.sql: Raw wrestling events data.

2. Dimension Layer (models/dimensions/)
Purpose: Contains dimension tables that describe the core business entities and their attributes.

Examples:

dim_event.sql: Event details (brand, event name, show number, etc.)

dim_arena.sql: Unique arenas.

dim_location.sql: Unique locations (city, state, country).

dim_date.sql: Calendar dates.

3. Fact Layer (models/facts/)
Purpose: Contains event-level records for attendance, referencing dimension tables via surrogate keys and storing business measures.

Example:

fact_pro_wrestling_events.sql: Stores each wrestling event, with links to all dimensions.

4. Reporting Layer (models/reporting/)
Purpose: Joins fact and dimension tables into a single, denormalized table for efficient BI reporting and dashboarding.

Example:

report_pro_wrestling_events.sql: Combines all relevant fields for easy analysis and visualization.

5. Schema & Documentation (models/schema.yml)
Purpose: Documents all models and columns, and defines dbt tests for data quality and integrity.

## Data Flow

1. **Staging Layer**
   - Cleans and standardizes raw event data (`stage_pro_wrestling_events.sql`).

2. **Dimension Layer**
   - `dim_event`: Parsed event details (brand, event name, show number, show name, event type, broadcast type, broadcast network).
   - `dim_arena`: Unique list of arenas.
   - `dim_location`: Unique locations (city, state, country).
   - `dim_date`: Calendar date dimension.

3. **Fact Layer**
   - `fact_pro_wrestling_events`: Contains foreign keys to all dimensions and measures such as attendance.

4. **Reporting Layer**
   - `report_pro_wrestling_events.sql`: Joins the fact table with all dimensions, providing an analytics-ready table for BI and dashboards.

---

## Setup & Usage

### 1. **Install dbt and Dependencies**

- pip install dbt
- dbt deps

2. **Configure Your Connection**

Edit your `profiles.yml` with your data warehouse credentials.

3. **Build All Models**

- dbt run

4. **Test Your Models**

- dbt test 

5. **Generate and View Documentation**

- dbt docs generate
- dbt docs serve

## Customization

- **Add new dimensions or facts:**  
Place new models in the appropriate folder (`dimensions/`, `facts/`, etc.).
- **Update documentation and tests:**  
Edit `schema.yml` to add model and column descriptions, and to define dbt tests.
- **Extend reporting:**  
Add calculated fields or additional joins to `reporting.sql` as needed for your BI requirements.

## Best Practices

- Keep your dimension tables unique on their business keys.
- Validate your fact table grain to avoid duplicates.
- Document your models using `schema.yml` for maintainability.
- Add dbt tests for uniqueness, not null, and referential integrity.

## Support & Contributions

For questions, suggestions, or contributions, please contact [Bryan Sauka] or open an issue in this repository.

---