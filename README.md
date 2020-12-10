# 311_covid_trends
An analysis of trends during COVID from the 311 NY complaints dataset.

## Data

### 311 Data

The raw 311 dataset downloaded from [NYC Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) is available on HFS at `/user/jr4964/final-project/complaints.csv`. The processed and reduced dataset is at `/user/djk525/big-data/project/data/311_reduced.csv` The processed data contains the created date, complaint type, complaint descriptor, and zcta ([zip code tabulation area](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html)). The zcta is obtained by joining with `/user/djk525/big-data/project/data/zip_zcta.csv` on zip code. This join removes complaints with invalid zip codes. The 311 complaints are also filtered on the following conditions:

- Creation data is in the range January 1st, 2010 - October 31st, 2020.
- Complaint type is not empty.

### 311 and Census Data

The file under `/user/djk525/big-data/project/data/complaint_census_join.csv` contains the result of an inner join operation between the census and 311 datasets. Both datasets were joined on ZCTA key. The result dataset follows the schema below:


column                  | format
-----                   |-------
zcta                    | number
complaint datetime      | datetime (MM/dd/yyyy hh:mm:ss a)
complaint type          | string
complaint descriptor    | string
geoID                   | string
median earning          | number
full time median earning| number
full time mean earning  | number

#### Aggregation

We aggregated the number of total complaints and number of noise complaints by day alone and by both day and zip code for March 1st - October 31st in 2019 and 2020.

`/user/djk525/big-data/project/data/complaints_census_by_zip_day.csv` has the following schema:

column                  | format
-----                   |-------
zcta                    | number
day                     | date (yyyy-MM-dd)
num_complaints          | number
num_noise_complaints    | number
geoID                   | string
median_earning          | number
full_time_median_earning| number
full_time_mean_earning  | number


`/user/djk525/big-data/project/data/complaints_by_day.csv` has the following schema:

column                  | format
-----                   |-------
day                     | date (yyyy-MM-dd)
num_complaints          | number
num_noise_complaints    | number

### Links

Our small and aggregated datasets are available on [Google Drive](https://drive.google.com/file/d/1L5kfekprklV9Np3720pa6qYUCJT7BYrf/view?usp=sharing) and on GitHub Gist:

- [zip_zcta.csv](https://gist.github.com/DanielKerrigan/b774aa655e3ccb320cd3560863ed3d3d)
- [nyc-geojson.json](https://gist.github.com/DanielKerrigan/a726b9dd2db50a90b308f7a9915db531)
- [covid.csv](https://gist.github.com/DanielKerrigan/f7baab69fa175bfbd5d10e38ad85b1b4)
- [complaints_census_by_zip_day.csv](https://gist.github.com/DanielKerrigan/c0c8bd921a052bf6cdf87343773202ba)
- [complaints_by_day.csv](https://gist.github.com/DanielKerrigan/fe5ab6e81f5ce6127f05d79933bdf893)
