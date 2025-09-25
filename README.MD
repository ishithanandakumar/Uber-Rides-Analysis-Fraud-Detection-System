# Uber Ride Analysis Dashboard Project

## Overview
This project analyzes Uber ride booking data to create comprehensive dashboards focusing on ride cancellations, fraud detection, and operational insights. The data pipeline is built using Snowflake as the data warehouse with AWS S3 as the data source. Data is analyzed using SQL queries and visualized in Snowflake dashboards and Tableau Public.

## Project Structure
```
uber-ride-analysis/
├── sql/
│   ├── 01_setup/
│   │   ├── warehouse_setup.sql
│   │   ├── database_schema_setup.sql
│   │   └── integration_setup.sql
│   ├── 02_data_loading/
│   │   ├── file_format_setup.sql
│   │   ├── stage_setup.sql
│   │   └── table_creation.sql
│   ├── 03_data_transformation/
│   │   ├── refined_layer.sql
│   │   └── enriched_layer.sql
│   ├── 04_analytics/
│   │   └── dashboard_queries.sql
│   └── 04_data_export/
│       └── export_for_tableau.sql
├── dashboards/
│   ├── snowflake_dashboard_results.png
│   ├── uber_ride_analysis.csv
│   └── tableau_dashboards/
├── docs/
│   └── data_dictionary.md
└── README.md
```

## Data Architecture

### Layer 1: Raw Data (UberRideBooking_Schema)
- **Purpose**: Store raw data as loaded from S3
- **Table**: `Rides`
- **Description**: Contains all original ride booking data with minimal transformations

### Layer 2: Refined Data (Refined_Schema)  
- **Purpose**: Clean and standardized data with selected columns
- **Table**: `Refined_Rides`
- **Description**: Filtered dataset with relevant columns for analysis

### Layer 3: Enriched Data (Enriched_Schema)
- **Purpose**: Business-ready data with additional transformations
- **View**: `Enriched_Rides`  
- **Description**: Clean data with string trimming and column renaming - **THIS IS THE MAIN DATA SOURCE FOR DASHBOARDS**

## Key Findings from Data Analysis

### 🎯 **Ride Performance Overview**
- **Total Completed Rides**: 186,000 (majority of bookings)
- **Driver Cancellations**: 54,000 rides  
- **Customer Cancellations**: 21,000 rides
- **No Driver Found**: 21,000 rides
- **Incomplete Rides**: 18,000 rides

### 📊 **Top Customer Cancellation Reasons**
1. **Wrong Address** - 4,724 incidents
2. **Change of Plans** - 4,706 incidents  
3. **Driver Not Moving Towards Pickup** - 4,670 incidents
4. **Driver Asked to Cancel** - 4,590 incidents
5. **AC Not Working** - 2,310 incidents

### 🚗 **Top Driver Cancellation Reasons**
1. **Customer Related Issues** - 13,674 incidents
2. **Customer Coughing/Sick** - 13,502 incidents
3. **Personal & Car Related Issues** - 13,452 incidents
4. **More Than Permitted People** - 13,372 incidents

### 💰 **Booking Value Analysis**
- **Very High Value (>$50)**: 203,758 bookings
- **High Value ($25-$50)**: 242 bookings
- Higher value rides show different cancellation patterns

### 🛣️ **Distance Impact on Completion**
- **Long Rides (15-30km)**: 93.28% completion rate
- **Short Rides (<5km)**: 75.15% completion rate  
- **Medium Rides (5-15km)**: 80.00% completion rate
- **Very Long Rides (>30km)**: 100% completion rate

### 🔍 **Fraud Detection Results**
- Identified **8 high-risk customers** with multiple cancellations
- All flagged customers show 100% cancellation rate (4 bookings each)
- Key fraud indicators: Multiple same-day cancellations, repetitive patterns

### 💳 **Payment Method Preferences**
1. **UPI**: 91,818 bookings (most popular)
2. **Cash**: 50,734 bookings
3. **Uber Wallet**: 24,552 bookings
4. **Credit Card**: 20,418 bookings
5. **Debit Card**: 16,478 bookings

### ⭐ **Driver Performance by Rating**
- **5-Star Drivers**: 56,156 total rides, 100% completion
- **4-Star Drivers**: 116,456 total rides, 100% completion  
- **3-Star Drivers**: 13,388 total rides, 100% completion
- No driver cancellations across all rating categories

### 🚨 **Combined Fraud Analysis**
- Top fraud pattern: "Driver not moving towards pickup location" (multiple customers)
- Average 2 cancellations per day for high-risk customers
- Consistent "Low Risk" classification for most patterns

## Dashboard Requirements

### 1. Ride Cancellation Analysis with Fraud Detection
- **Metrics**: Cancellation rates by customer and driver
- **Fraud Indicators**: Multiple cancellations by same customer, unusual booking patterns
- **Visualizations**: Cancellation trends, fraud score distribution

### 2. Cancellation Reasons Analysis
- **Customer Cancellations**: Analysis of `Reason_for_cancelling_by_Customer`
- **Driver Cancellations**: Analysis of `Reason_for_cancelling_by_Driver`
- **Impact Analysis**: Relationship between cancellation reasons and booking values

### 3. Operational Performance
- **Distance Impact**: Effect of `Ride_Distance` on ride completion
- **Payment Method Analysis**: Cancellation patterns by `Payment_Method`
- **Driver Performance**: Impact of `Driver_Ratings` on cancellations

## Prerequisites

### Software Requirements
- Snowflake account with appropriate privileges
- AWS S3 bucket access
- Tableau Public (free) for additional dashboard creation

### AWS Setup
- S3 bucket: `myuberridesanalysis`
- IAM Role: `UberRideRole` with S3 access permissions
- ARN: `arn:aws:iam::586206155985:role/UberRideRole`

## Installation and Setup

### Step 1: Clone Repository
```bash
git clone https://github.com/yourusername/uber-ride-analysis.git
cd uber-ride-analysis
```

### Step 2: Snowflake Setup
Run the SQL files in the following order:

1. **Infrastructure Setup**
   ```sql
   -- Run these files in order:
   sql/01_setup/warehouse_setup.sql
   sql/01_setup/database_schema_setup.sql  
   sql/01_setup/integration_setup.sql
   ```

2. **Data Loading Setup**
   ```sql
   sql/02_data_loading/file_format_setup.sql
   sql/02_data_loading/stage_setup.sql
   sql/02_data_loading/table_creation.sql
   ```

3. **Data Transformation**
   ```sql
   sql/03_data_transformation/refined_layer.sql
   sql/03_data_transformation/enriched_layer.sql
   ```

### Step 3: Run Analytics Queries
Execute the dashboard queries to get insights:
```sql
sql/04_analytics/dashboard_queries.sql
```

### Step 4: Export Data for Advanced Visualization (Optional)
1. Execute the export query: `sql/04_data_export/export_for_tableau.sql`
2. Download results as CSV from Snowflake
3. Import into Tableau Public for advanced visualizations

## Data Dictionary

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| Date | DATE | Date of the ride booking |
| Time | TIME | Time of the ride booking |
| Booking_ID | VARCHAR(50) | Unique identifier for each booking |
| Booking_Status | VARCHAR(50) | Status of the booking (completed, cancelled, etc.) |
| Customer_ID | VARCHAR(50) | Unique identifier for each customer |
| Reason_for_cancelling_by_Customer | VARCHAR(500) | Reason provided by customer for cancellation |
| Reason_for_cancelling_by_Driver | VARCHAR(500) | Reason provided by driver for cancellation |
| Booking_Value | NUMBER | Monetary value of the booking |
| Ride_Distance | NUMBER | Distance of the ride in kilometers |
| Driver_Ratings | INTEGER | Rating given to the driver (1-5 scale) |
| Customer_Rating | INTEGER | Rating given by the customer (1-5 scale) |
| Payment_Method | VARCHAR(100) | Payment method used for the booking |

## Implementation Results

✅ **Successfully Implemented All Dashboard Queries**
- All 10 analytical queries executed successfully in Snowflake
- Generated comprehensive insights across all problem statements
- Identified key business metrics and patterns

✅ **Key Business Insights Discovered**
- 86% ride completion rate with clear improvement opportunities
- UPI dominance in payment preferences  
- Distance-based completion rate variations
- Fraud detection system identifying high-risk customers
- Driver performance consistency across rating levels

✅ **Actionable Recommendations Generated**
- Focus on reducing "Wrong Address" customer cancellations
- Investigate driver-side "Customer Related Issues"
- Monitor short-distance ride completion rates
- Implement targeted fraud prevention for identified high-risk customers

## Dashboard Analysis Areas

Based on the enriched data and successful query execution, the dashboards focus on:

### Ride Cancellation Reason with Fraud Detection 
- ✅ Cancellations by driver or customers
- ✅ Reasons for cancellation 
- ✅ Is cancellation and booking value related 
- ✅ Is distance affecting ride completion 
- ✅ Customer ID to predict fraud 
- ✅ Are payment methods related to cancellations 
- ✅ Payment methods commonly used 
- ✅ Driver rate resulting in cancellation or incomplete rides

## Usage Workflow

1. **Data Pipeline**: S3 → Snowflake (Raw → Refined → Enriched)
2. **Analytics**: Execute dashboard queries for business insights
3. **Visualization**: Use Snowflake dashboards or export to Tableau
4. **Business Actions**: Implement recommendations based on findings

## Key SQL Objects

- **Main Data Source**: `UberRideBooking.Enriched_Schema.Enriched_Rides` (VIEW)
- **Analytics Queries**: `sql/04_analytics/dashboard_queries.sql`
- **Export Query**: `SELECT * FROM UberRideBooking.Enriched_Schema.Enriched_Rides`

## Performance Metrics

- **Data Volume**: 300,000+ ride records analyzed
- **Query Performance**: All 10 analytical queries execute successfully
- **Data Quality**: Clean, fraud-detection ready dataset
- **Business Value**: Actionable insights for operational improvements

## Contributing
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-analysis`)
3. Commit your changes (`git commit -am 'Add new analysis'`)
4. Push to the branch (`git push origin feature/new-analysis`)  
5. Create a Pull Request

## Data Privacy and Security
- All customer data is anonymized
- Access to production data requires appropriate permissions
- Follow company data governance policies
- Fraud detection respects privacy while identifying patterns

## Support and Maintenance
- **Data Refresh**: Load new CSV files to S3 and re-run COPY INTO commands
- **Dashboard Updates**: Re-run analytical queries for latest insights
- **Schema Changes**: Update transformation scripts as needed
- **Monitoring**: Set up alerts for data quality and fraud patterns

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Contact
- **Project Owner**: Ishitha Nanda Kumar

---
*Project successfully implemented and delivering actionable business insights*
*Last Updated: September 23, 2025*
