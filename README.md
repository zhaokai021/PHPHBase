PHPHBase
========

PHP Client For HBase

Simple client of HBase for PHPers.

Note: you must start you HBase thrift server and all these apis are based on thrift version 0.9.1 .
Simple usage:
<?php  
 
ini_set('display_errors', E_ALL);  
require_once( './ThriftPHP.php' ); 
$cli = new ThriftPHP('127.0.0.1','9090','','');
print_r($cli->getAllTableNames());
print_r($cli->query("dict_station",array("101010100"),array("station")));
$cli->insert("hbasephp",array(
 "abc"=>array("fm:abc"=>"123","fm:abc4"=>"1234"),
 "efg"=>array("fm:efg"=>"456"),
 
 ));
print_r($cli->queryByStartAndStopKey("hbasephp","abc","zzz",array("fm"),10)); 


?>  
 

