# 311_covid_trends
An analysis of trends during COVID from the 311 NY complaints dataset.

## 311 Data

The raw 311 dataset is on HFS at `/user/jr4964/final-project/complaints.csv` and a sample of it is at `/user/jr4964/final-project/sample_complaints.csv`. The processed and reduced dataset is at `/user/djk525/big-data/project/data/311_reduced.csv` and `/user/djk525/big-data/project/data/311_sample_reduced.csv`. The processed data contains the created date, complaint type, complaint descriptor, and zcta ([zip code tabulation area](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html)). The zcta is obtained by joining with `/user/djk525/big-data/project/data/zip_zcta.csv` on zip code. This join also has the effect of removing complaints with invalid zip codes. The 311 complaints are also filtered on the following conditions:

- Creation data is in the range January 1st, 2010 - October 31st, 2020.
- Complaint type is not empty.

`/user/djk525/big-data/project/data/complaints_by_zip_day.csv` and `/user/djk525/big-data/project/data/complaints_by_zip_day_sample.csv` have the number of complaints on each day for each zcta.

The file under /user/jr4964/final-project/complaint_census_join.csv contains the result of an inner join operation between the census and 311 datasets. Both datasets were joined on ZCTA key. The result dataset follows the schema below:


attribute name | zcta | complaint datetime | complaint type | complaint descriptor | geoID | median earning | full time median earning | full time mean
--- | --- | --- | --- |--- |--- |--- |--- |--- |--- |--- |---
datatype | number | datetime (%d/%m/%Y %H:%M:%S)  | string | string | string | number | number | number 

