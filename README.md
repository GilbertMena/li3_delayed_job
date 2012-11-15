# Delayed Job

Delayed_job (or DJ) encapsulates the common pattern of asynchronously executing longer tasks in the background.

This is a direct port of [Delayed::Job](https://github.com/tobi/delayed_job) and this fork also supports MySQL.

The reason I've added MySQL support is because I required a data store that will ensure transactions for the project 
I plan to embed this into and I don't want to maintain an mongo installation just for the sake of this plugin. 

This can be modified to use any Database adapter or data source that you want but so far I've only tested MySQL 
and the model will throw an exception if you try to use something other than mongo and MySQL. 
Feel free to fork and send me a pull request.

## Pre-Requisites - If using MongoDB


- Working installation of MongoDB

    http://www.mongodb.org/display/DOCS/Quickstart+Unix

- Working installation of Mongo PEAR extension

    `apt-get install php5-dev php-pear`

    `pecl install mongo`

    `echo 'extension=mongo.so' > /etc/php5/apache2/conf.d/mongo.ini`

    `service apache2 restart`

## Pre-Requisites - If using MySQL

- Working installation/connection to a MySQL server

- You can safely comment `use MongoDate;` in `li3_delayed_jobs\models\Jobs.php`

> In fact, you must comment this line unless the Mongo PEAR extension is enabled in your PHP installation. (Just do it if not using mongo for this).

## Installation

Check out the code to your library directory

    cd libraries
    git clone git@github.com:cgarvis/li3_delayed_job.git
    
Include the library in your `/app/config/bootstrap/libraries.php`

    Libraries::add('li3_delayed_job');

## Configuration

- In the `li3_delayed_job\models\Jobs.php` you can modify the following:
- `Jobs::storeObject` determines whether or not to keep the queue objects in the table
- `Jobs::destroyFailedJobs` determines whether or not to erase failed tasks after attempts are exhausted
- `Jobs::keyID` Stores the default storage engine table/document table primary key.  Currently set automatically to `_id` for mongo and `id` for database (assuming MySQL).  Look at `Jobs::setDataSourceType()` for change these.

## Usage


Jobs are simple objects with a method called perform.  Any object which responds to perform can be stuffed into the jobs collection. Job objects are serialized to yaml so that they can later be resurrected by the job runner.

## Creating a job

See examples under `./tests`.

## Running the jobs

You can invoke `li3 jobs work` which will start working off jobs.  You can cancel the task with `CTRL-C`

Keep in mind that each worker will check the database at least every 5 seconds.

### Cleaning up

You can invoke `li3 jobs clear` to delete all jobs in the queue

