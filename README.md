## Udacity Nanodegree in Data Engineering with AWS 
# PROJECT - stedi-human-balance-analytics 
# Introduction
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Data Engineer on the STEDI Step Trainer team is needed to extract the data produced by the STEDI Step Trainer sensors and 
the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

# Project Data
3 json files :
- customer (serialnumber,sharewithpublicasofdate,birthday,registrationdate,sharewithresearchasofdate,customername,email,
  lastupdatedate,phone,sharewithfriendsasofdate)
- step_trainer (sensorReadingTime,serialNumber,distanceFromObject)
- accelerometer (timeStamp,user,x,y,z)

# Requirements
1. 2 Glue tables for customer and accelrometer data in the LANDING ZONE
2. 2 Glue jobs to get custoemr and accelerometer trusted data in TRUSTED ZONE and create 2 tables for them.
3. Sanitize the customer data with a Glue job to create customer curated data in CURATED ZONE and create table for it.
4. Create step trainer trusted table using a Glue job from step trainer landing data.
5. Create machine learning curated data from step trainer trusted and accelerometer trusted data whihc will be consumed by the
   machine learning model.
