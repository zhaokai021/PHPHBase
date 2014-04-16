<?php  
 
ini_set('display_errors', E_ALL);  

require_once( './ThriftPHP.php' ); 
$cli = new ThriftPHP('61.4.185.193','9090','','');
print_r($cli->getAllTableNames());
print_r($cli->query("dict_station",array("101010100"),array("station")));
$cli->insert("hbasephp",array(
 "abc"=>array("fm:abc"=>"123","fm:abc4"=>"1234"),
 "efg"=>array("fm:efg"=>"456"),
 
 ));
print_r($cli->queryByStartAndStopKey("hbasephp","abc","zzz",array("fm"),10)); 

?>  

