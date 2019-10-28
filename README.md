# JobMiner

## Summary
Within three weeks, I built a job search platform that allows user to input skill tags to search for tech jobs and recommends similar jobs to users. The platform used scrapped data from Dice.com and tags from Stackoverflow. The raw data was processed in Spark and the results were stored in Elasticsearch. The Dash app allows users to query multiple tags and similar jobs are recommended on the fly. Airflow was set up to automatically update the database. Whenever new raw data is uploaded, an Airflow sensor would be triggered to run the whole batch process. More details can be found below and in the slides. 
 * [Demo Slides](https://www.tinyurl.com/y5n2sxsf)
 * [Demo WebUI](http://www.datatrailblazer.me)

## Motivation 
Finding the right tech job can be a real challenge, especially when the same role can vary significantly from company to company. It can also be a struggle for the firms to identify the right candidates, as they are often overwhelmed with applications from people who applied without scrutinizing the skills requirement. Currently, most job search sites allow users to search by job title, company name, or perhaps, keywords. However, the keyword search function is somewhat limited, and very little guidance is provided to the users on what the right words are to search. 

I decided to build a job search platform that starts with your skills and interests. There are two functions: 

  * Allows users to filter jobs by skill and topic tags
  * Recommends users other similar jobs 

## Approach 

### Stage 1: Tagging 
To avoid personal bias coming up the tags and the trouble of manually modifying tags over time, I decided to use the Top 500 tags from Stackoverflow. I used SQL to query the tags and assigned them onto the job postings. Users can then search for jobs by inputting any number of tags. 
![Stage 1](/static/tagging.png)

### Stage 2: Recommending 
Instead of recommending jobs based on a similar set of tags or the full job description, I extracted a set of keywords from each job description and compared it against other sets of keywords. Other postings sharing the most number of keywords will be recommended as similar jobs. 
![Stage 2](/static/recommending.png)

## Data 
  * 9.8M scrapped Dice.com job postings 
  * Top 500 Stackoverflow tags 

## Data Pipeline 
![Pipeline](/static/pipeline.png)

### Directory Structure 

```bash
├── airflow
│   ├── dags
│   │   ├── job_update.py
│   │   └── spark_bash_commands.py
│   └── plugins
│       ├── custom_plugins.py
│       └── airflow_config.py
├── dash_app
│   ├── app.py
│   └── assets
│       └── w3.css
├── src
│   ├── save_parquet.py
│   ├── process.py
│   └── util
│       ├── clean_description.py
│       ├── generate_common_words.py
│       ├── match_tags.py
│       ├── save_data.py
│       └── config.py
├── database_tests
│   ├── db_test.py
│   └── db_config.py
├── README.md
└── .gitignore
```

### Batch Processing 
#### XML-Parquet Conversion 
Raw scrapped data was in XML format. It was converted to Parquet using Databrick's Spark-XML package for the following reasons: 
 * Reduce the overall data size
 * Standardize raw data format 
 * Increase data ingestion speed 

#### Duplicates Removal
Job postings from the same company, state, and with the same job description are removed as duplicates.  

#### Tags Assignment 
The Top 500 tags were parsed and matched to words that appear in job postings. The parsing entails removing the version number and the brand name for some tools. For example, python-3.X -> python and apache-spark -> spark. 

#### Keywords Extraction
All the non-essential words were removed and the leftover words were used to construct a concise set of keywords for each job posting for similiarity comparsion. The steps are listed below: 
 1. Regex Parsing: removes symbols, punctuations, extra line, leading spaces, and numbers. 
 2. Tokenization: tokenizes the text 
 3. Stopword Removal: removes standard stopwords given in the NLTK package 
 4. Lemmatization: extracts word lemmas
 5. Common Job Description Words Removal: Document frequency for every unique word that appeared in a posting was computed. If a word appeared in over 50% of the job postings, it was considered as a common job description word. The common words were subsequently removed.
 
#### Bulk Writing to Elasticsearch 
Elasticsearch-Hadoop package was used to writing the results from Pyspark dataframe to Elasticsearch. The bulk writing size was reduced to 100 to avoid out of memory issues.

### Database Selection 
It was initially which data
#### PostgreSQL
Postgres
#### Elasticsearch 
#### Speed Test 
#### Functionality Test
### Airflow
Since the task was signed to be a daily batch job, Airflow was incorporated to schedule and to run the jobs automatically. A customized sensor was written to detect new successfuly raw data uploads in S3. The batch process would then be triggered and any failure and success would be emailed to the data engineers. 
![airflow](/static/airflow.png)

Below is a screenshot of email notifications sent to engineers. 

![airflow_email](/static/airflow_email.png)
### Optimization 

### Visualization 
![dash](/static/dash.png)
 1. State selection input 
 2. Tags input from text or dropdown 
 3. Suggested tags 
 4. Next button 
 5. Raw job posting information: title, date and full job description 
 6. Associated tags 
 7. Job recommendations 

### Future Vision
For the future, there are a few things that can be implemented to improve on the existing project: 
 1. Not all of the tags from Stackoverflow are meaningful and relevant to job postings. A process that filters or better transforms Stackoverflow tags can be implemented to allow users to more precisely search by skills and interests. 
 2. Currently Airflow is only scheduled to run the whole batch process again. It can be changed in the way that whenever new scrapped data comes in, Airflow runs a task that compares new and existing postings and only updates/removes postings rather than re-building the whole database. 
 3. More benchmarks and tests can be done. Other NLP methods and databases might be better suited for the project. 





