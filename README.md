# DEND_P3

### SUMMARY ...

In this section we add description about the different files used:

* sql_queries.py: All SQL instruction to execute vs RedShift Database: DROPs, CREATEs, LOADs
* create_tables.py: Functions 2 create from scratch all RedShift tables
* etl.py: Main python program with all the logic to load RedShift tables

### DIAGRAM ...

We can observe how the database diagram has been implemented:

![alt text](./redshift_diagram.jpg "sparkify db Diagram")

### DOCUMENT PROCESS ...

__Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.__

The purpose of this database is joining the 2 dataset Sparkify Startup owns in order to get conclussions about the traffic on their application. With this, and taking into account the requirements, we could be able to obtain information with just querying one database.

__State and justify your database schema design and ETL pipeline.__

Based on volumetries of the tables and how every table relates, about all vs song_plays, we have chosen as sortkey every "fk" vs fact table. There are 2 exceptions, songs and artists table, which own distkey. As you can see the distribution elected is key.


### HOW TO ...

```
python create_tables.py
python etl.py
```
