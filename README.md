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
![Stage 1](/static/stage_1_tagging.png)

### Stage 2: Recommending 
Instead of recommending jobs based on a similar set of tags or the full job description, I extracted a set of keywords from each job description and compared it against other sets of keywords. Other postings sharing the most number of keywords will be recommended as similar jobs. 

## Data 
  * 9.8M scrapped Dice.com job postings 
  * Top 500 Stackoverflow tags 
