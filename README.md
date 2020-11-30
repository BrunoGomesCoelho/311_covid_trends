# 311_covid_trends
An analysis of trends during COVID from the 311 NY complaints dataset.

## 311 Data

The raw 311 dataset is on HFS at `/user/jr4964/final-project/complaints.csv` and a sample of it is at `/user/jr4964/final-project/sample_complaints.csv`. The processed and reduced dataset is at `/user/djk525/big-data/project/data/311_reduced.csv` and `/user/djk525/big-data/project/data/311_sample_reduced.csv`. The processed data contains the created date, complaint type, complaint descriptor, and incident zip code. It is filtered on the following conditions:

- Zip code matches the regex `^1[0-9]{4}$`, which matches strings that only contain a 1 followed by four digits.
- Creation data is in the range January 1st, 2010 - October 31st, 2020.
- Complaint type is not empty.

`/user/djk525/big-data/project/data/complaints_by_zip_day.csv` and `/user/djk525/big-data/project/data/complaints_by_zip_day_sample.csv` have the number of complaints on each day for each zip code.
