# ReadMe

## Project Context and Goals

The objective of our project is to create an ETL pipeline to provide key metrics/aggregations of exchange rates from an API and persist them in a database. This can be used by **data analysts** and **data scientists** in businesses whose operations involve foreign exchanges (e.g. A travel agency organising tours worldwide). It would allow them to create analytical deicisions/modelling based on metrics, such as but not limited to:

> - short- and long-term trends (based on moving averages) of select exchange rates/currencies
> - exchange rate pairings having the highest relative strength at a given day
> - most volatile exchange rate pairing over the past week
> - performance of a currency from the start of the month to the current date (MTD)
> - the rate change from a specific date to the rate on the same day last year (YOY)

## Datasets Selected
| Source name | Source type | Source documentation |
| - | - | - |
| [Exchange Rates API](https://exchangeratesapi.io/) | REST API | [Documentation](https://exchangeratesapi.io/documentation/) |

In the API, the group utilized both *latest rates* and *historical rates* endpoints which are the only ones available for the free tier.

![images/DEC-Project-Endpoints-Utilized.png](images/DEC-Project-Endpoints-Utilized.png)

## Solution architecture
![images/DEC-Project-Architecture-Diagram.png](images/DEC-Project-Architecture-Diagram.png)


## Techniques Applied
- The script utilized an ETL process and an initial run was orchestrated manually to obtain 370 days worth of data, enough to calculate the YOY metric in an RDS PostgreSQL instance.
- The automated runs utilize an incremental extract wherein the max or latest date is queried from the database and is used as the process start date in the API.
- Data extracted by the automated runs are loaded into the same RDS instance as inserts. 
- Calculation of various metrics are performed once the extract process completes; the transformation scripts are stored in jinja templates.
- All the aforementioned processes are containerized using docker and the images are uploaded into AWS ECR. AWS ECS is provisioned to run the whole pipeline at 24-hour intervals.

### Configurations of the AWS services:
#### Amazon Elastic Container Service (ECS)
Cluster:
![images/DEC-Project-Cluster.png](images/DEC-Project-Cluster.png)

Cluster - Scheduled Task:
![images/DEC-Project-Cluster-Sched-Task.png](images/DEC-Project-Cluster-Sched-Task.png)

#### Amazon Elastic Container Registry (ECR)
Repository:
![images/DEC-Project-Repository.png](images/DEC-Project-Repository.png)

Images:
![images/DEC-Project-Images.png](images/DEC-Project-Images.png)

#### Relational Database Service (RDS)
Database:
![images/DEC-Project-Database.png](images/DEC-Project-Database.png)

#### Identity and Access Management (IAM)
Role:
![images/DEC-Project-Role-IAM.png](images/DEC-Project-Role-IAM.png)

#### Simple Storage Service (S3)
Bucket (for .env file):
![images/DEC-Project-env-Bucket.png](images/DEC-Project-env-Bucket.png)

Bucket Permissions:
![images/DEC-Project-Bucket-Permissions.png](images/DEC-Project-Bucket-Permissions.png)

## Final Dataset and Demo Run

### This is a rough schema of the database.
![images/DEC-Project-Data-Model.png](images/DEC-Project-Data-Model.png)
**The group decided to store both the raw and serving datasets into one RDS instance only.**

## Lessons Learnt
- Setting up a composite key into each table can help in implementing upserts in future projects. (Gelo)
- CTEs can be used in "CREATE TABLE AS" commands and the whole CTE logic is useable in Jinja templates as well. (Gelo)