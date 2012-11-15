<?php

namespace li3_delayed_job\models;

use lithium\analysis\Logger;
use ErrorException;
use InvalidArgumentException;
use MongoDate;
use lithium\data\Connections;

class Jobs extends \lithium\data\Model {
  /**
   * The maxium number of attempts a job will be retried before it is considered completely failed.
   */
  const MAX_ATTEMPTS = 25;
  
  /**
   * The maxium length of time to let a job be locked out before it is retried.
   */
  const MAX_RUN_TIME = '4 hours';
  
  /**
   * @var bool
   */
  public $destroyFailedJobs = false;
  
  /**
   *
   */
  protected $entity;
  
  /**
   * @var int
   */
  public static $minPriority = null;
  
  /**
   * @var int
   */
  public static $maxPriority = null;
  
  /**
   * @var string
   */
  public $workerName;
  
  /**
   *@var string
   */
  protected static $dataSourceType;
  /*
   *@var bool
   *@description whether or not to delete the queued objects after completion
   */
  public static $storeObject = true;
  
  /*
   *@var string
   *@description The default data store id. Use _id for Mongo or id for MySQL, it is set automatically
   */
  public static $keyID = 'id';

  protected $_meta = array(
    'name' => null,
    'title' => null,
    'class' => null,
    'source' => 'delayed_jobs',
    'connection' => 'default',
    'initialized' => false
  );
  
  public function __construct()
  {
    //set the datasourcetype if not already set
    if(empty(self::$dataSourceType))
    {
      self::setDataSourceType();
    }
    
    $this->workerName = 'host:'.gethostname().' pid:'.getmypid();
  }
  
  public function setDataSourceType()
  {
    $config = Connections::get($this->_meta['connection'], array('config' => true));

    if($config['type']=='MongoDB')
    {
      self::$dataSourceType = 'Mongo';
      self::$keyID = '_id';
      return;
    }
    
    if($config['type']=='database')
    {
      if($config['adapter']=='MySql')
      {
        self::$dataSourceType = 'Database';
        self::$keyID = 'id';
        return;
      }else
      {
        throw new \ErrorException('Database adapter '.$config['adapter'].' is not currently supported.');
      }
    }
    
    throw new \ErrorException('Couldnt determine the datasourcetype.');
  }
  
  public function __get($property) {
    
    //set the datasourcetype if not already set
    if(empty(self::$dataSourceType))
    {
      $this->setDataSourceType();
    }
    
    if($property == 'name') {
      if(method_exists($this->payload, 'displayName')) {
        $this->name = $this->payload->displayName();
      } else {
        $this->name = get_class($this->payload);
      }
      
      return $this->name;
    }
    
    if($property == 'payload') {
      $this->payload = static::deserialize($this->handler);
      return $this->payload;
    }
    
    //reset the _id to id for database data source types
    if($property=='_id'&&self::$dataSourceType=='Database')
    {
      $property = 'id';
    }

    if(isset($this->entity)) {
      return $this->entity->$property;
    }
    
    throw new InvalidArgumentException("Property {$property} doesn't exist");
  }
  
  public function __set($property, $value) {
    
    //set the datasourcetype if not already set
    if(empty(self::$dataSourceType))
    {
      $this->setDataSourceType();
    }
    
    if(isset($this->$property) || $property == 'name' || $property == 'payload') {
      $this->$property = $value;
    }
    
    //reset the _id to id for database data source types
    if($property=='_id'&&self::$dataSourceType=='Database')
    {
      $property = 'id';
    }

    $this->entity->$property = $value;
  }
  
  /**
   * When a worker is exiting, make sure we don't have any locked jobs.
   */
  public static function clearLocks() {
    // @TODO: implement
  }
  
  /**
   * Deserializes a string to an object.  If the 'perform' method doesn't exist, it throws an ErrorException
   *
   * @param $source string
   * @return object
   * @throws ErrorException
   */
  public static function deserialize($source) {
    $handler = unserialize($source);
    if(method_exists($handler, 'perform')) {
      return $handler;
    }
    
    throw new \ErrorException('Job failed to load: Unknown handler. Try to manually require the appropiate file.');
  }
  
  /**
   * Add a job to the queue
   *
   * @param $job stdClass
   * @param $priority int
   * @param $runAt MongoDate|string
   * @return bool
   * @throws ErrorException
   */
  public static function enqueue($object, $priority = 0, $runAt = null) {
    
    $data = array(
      'attempts' => 0, 
      'handler' => serialize($object), 
      'priority' => $priority, 
      //'run_at' => $runAt, 
      'completed_at' => null,
      'failed_at' => null, 
      'locked_at' => null, 
      'locked_by' => null,
      'last_error' => null, 
    );
    
    //need to instantiate the object first so that we can access the _meta instance property
    $job = Jobs::create($data);
    
    //set the datasourcetype if not already set
    if(empty(self::$dataSourceType))
    {
      $job->setDataSourceType();
    }
    
    if(!method_exists($object, 'perform')) {
      throw new ErrorException('Cannot enqueue items which do not respond to perform');
    }
    
    if(!is_a($runAt, 'MongoDate')&&self::$dataSourceType=='Mongo') {
      $runAt = new MongoDate($runAt);
    }
    
    if($runAt==null&&self::$dataSourceType=='Database')
    {
      $runAt = date('Y-m-d H:i:s') ;
    }elseif(self::$dataSourceType=='Database')
    {
      $pattern = '(\d{2}|\d{4})(?:\-)?([0]{1}\d{1}|[1]{1}[0-2]{1})(?:\-)?([0-2]{1}\d{1}|[3]{1}[0-1]{1})(?:\s)?([0-1]{1}\d{1}|[2]{1}[0-3]{1})(?::)?([0-5]{1}\d{1})(?::)?([0-5]{1}\d{1})';
      $patternValidation = preg_match($pattern,$runAt);
      if(!$patternValidation||!DateTime::createFromFormat('Y-m-d H:i:s', $runAt))
      {
        throw new ErrorException('The runAt parameter does not comform to mysql DATETIME Y-m-d H:i:s or is not a valid date');
      }
      
      
    }
    
    $data['run_at'] = $runAt;
    
    //hate to do it this way but need to for lack of a better option right now, im recreating the object so we have the correctn run_at
    $job = Jobs::create($data);
    
    return $job->save();
  }
  
  /**
   * Find and lock a job ready to be run
   *
   * @return bool|\lithium\data\entity\Document
   */
  public static function findAvailable($limit = 5, $maxRunTime = self::MAX_RUN_TIME)
  {
    
    //set the datasourcetype if not already set
    if(empty(self::$dataSourceType))
    {
      self::setDataSourceType();
    }
    
    if(self::$dataSourceType=='Mongo')
    {
      $conditions = array(
        'run_at' => array('$lte' => new \MongoDate()),
      );
      
      if(isset(static::$minPriority))
      {
        $conditions['priority'] = array('$gte' => static::$minPriority);
      }
      
      if(isset(static::$maxPriority))
      {
        $conditions['priority'] = array('$lt' => static::$maxPriority);
      }
    }
    
    if(self::$dataSourceType=='Database')
    {
      $conditions = array(
        'run_at' => array('<=' => date('Y-m-d H:i:s')),
        'completed_at'=>null
      );
      
      if(isset(static::$minPriority))
      {
        $conditions['priority'] = array('>=' => static::$minPriority);
      }
      
      if(isset(static::$maxPriority))
      {
        $conditions['priority'] = array('<' => static::$maxPriority);
      }
    }

    return Jobs::all(compact('conditions', 'limit'));
  }
  
  /**
   * @param \lithium\data\entity\Document
   * @param string                          Formatted for strtotime
   */
  protected function invoke() {
    $this->payload->perform();
    unset($this->payload);
  }
  
  protected function lockExclusively($maxRunTime, $worker) {
    
    //set the datasourcetype if not already set
    if(empty(self::$dataSourceType))
    {
      self::setDataSourceType();
    }
    
    if(self::$dataSourceType=='Mongo')
    {
      $time_now = new MongoDate();
    }
    
    if(self::$dataSourceType=='Database')
    {
      $time_now = date('Y-m-d H:i:s');
    }
    
    
     $idKey = self::$keyID;
    
    
    if($this->locked_by != $worker) {
      $locked = Jobs::update(array('locked_at' => $time_now, 'locked_by' => $worker), array($idKey => $this->$idKey));
    } else {
      $locked = Jobs::update(array('locked_at' => $time_now), array($idKey => $this->$idKey), array('_id' => $this->$idKey));
    }
    
    if($locked) {
      $this->locked_at = $time_now;
      $this->locked_by = $worker;
      return true;
    }
    
    return false;
  }
  
  /**
   * @param $message string
   */
  public function reschedule($message) {
    
    //set the datasourcetype if not already set
    if(empty(self::$dataSourceType))
    {
      self::setDataSourceType();
    }

    
    if($this->attempts < self::MAX_ATTEMPTS) {
      $this->attempts += 1;
      $this->run_at = $time;
      $this->last_error = $message;
      $this->unlock();
      $this->entity->save();
    } else {
      Logger::info('* [JOB] PERMANENTLY removing '.$this->name.' because of '.$this->attempts.' consequetive failures.');
      if($this->destroyFailedJobs) {
        Jobs::delete($this->entity);
      } else {
        
        if(self::$dataSourceType=='Database')
        {
           $this->failed_at = date('Y-m-d H:i:s');
        }
        
        if(self::$dataSourceType=='Mongo')
        {
          $this->failed_at = new MongoDate();
        }
        
      }
    }
  }
  
  /**
   * Run the next job we can get an exclusive lock on.
   * If no jobs are left we return -1
   *
   * @return int
   */
  public static function reserveAndRunOneJob($maxRunTime = self::MAX_RUN_TIME) {
    $jobs = static::findAvailable(5, $maxRunTime);
  
    foreach($jobs as $job) {
      $t = $job->runWithLock($maxRunTime);
      if(!is_null($t)) {
        return $t;
      }
    }
    
    return null;
  }
  
  public function runWithLock($entity, $maxRunTime, $workerName = null) {
    $workerName = $workerName ? $workerName : $this->workerName;  
    $this->entity = $entity;
    Logger::info('* [JOB] aquiring lock on '.$this->name);
    if(!$this->lockExclusively($maxRunTime, $workerName)) {
      Logger::warn('* [JOB] failed to aquire exclusive lock for '.$this->name);
      return null;
    }

    try {
      $time_start = microtime(true);
      $this->invoke();
      if(self::$storeObject)
      {
        //needs to be moved into its own method because the code exists in self::lockExclusively
        if(self::$dataSourceType=='Mongo')
        {
          $time_now = new MongoDate();
        }
        
        if(self::$dataSourceType=='Database')
        {
          $time_now = date('Y-m-d H:i:s');
        }
        $idKey = self::$keyID;
        //end of code that needs refactoring
        
        $complete = Jobs::update(array('completed_at' => $time_now), array($idKey => $this->$idKey));
      }else
      {
        $this->delete($this->entity);
      }
      
      
      $time_end = microtime(true);
      $runtime = $time_end - $time_start;
      
      Logger::info(sprintf('* [JOB] '.$this->name.' completed after %.4f', $runtime));
      return true;
    } catch(Exception $e) {
      $this->reschedule($e->getMessage());
      $this->logException($e);
      return false;
    }
  }
  
  /**
   * Unlock this job (note: not saved to DB)
   */
  public function unlock() {
    $this->locked_at = null;
    $this->locked_by = null;
  }
  
  /**
   * Do num jobs and return stats on success/failure.
   *
   * @param $num    int
   * @return array
   */
  public static function workOff($num = 100) {
    $success = $failure = 0;
    
    for($i = 0; $i < $num; $i++) {
      $result = self::reserveAndRunOneJob();
      if(is_null($result)) {
        break;
      }
      
      if($result === true) {
        $success++;
      } else {
        $failure++;
      }
    }
    
    return compact('success', 'failure');
  }
  
  public static function logException($e) {
    print_r($e);
    Logger::error('* [JOB] ',$this->name.' failed with '.$e->message());
    Logger::error($e);
  }
}