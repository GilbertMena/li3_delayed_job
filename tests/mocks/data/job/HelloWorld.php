<?php

namespace li3_delayed_job\tests\mocks\data\job;

class HelloWorld
{
  private $name = 'Test Method';
  private $constructVal;
  
  public function __construct($testID)
  {
    $this->constructVal = $testID;
    
  }
  
  public function newMethod()
  {
    echo 'new' ."\r\n";
  }
  
  public function perform() {
    echo 'Started performing'."\r\n";
    echo $this->name ."\r\n";
    echo $this->constructVal."\r\n";
    $this->newMethod();
  }
}