<?php 
namespace {

$GLOBALS['THRIFT_ROOT'] = './lib/';
include_once( $GLOBALS['THRIFT_ROOT'] . '/autoload.php');   
include_once( $GLOBALS['THRIFT_ROOT'] . '/Hbase.php' );  
include_once( $GLOBALS['THRIFT_ROOT'] . '/Types.php' );  

use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocol;
/**
* Author : ZHAOKAI
* Version : 1.0
* PHP Client For HBase
*/
class ThriftPHP { 
      /**
      *与HBase通信的socket通道
      */
      private  $socket=null;
      private  $transport=null;
      private  $protocol=null;
      private  $client=null;
      /**
      *默认发送数据超时时间
      */
      private  $sendTimeout=10000;
      /**
      *默认接收数据超时时间
      */
      private  $recvTimeout=20000;

      /**
      *构造函数
      *@param serverip 服务器IP
      *@param port 服务器连接端口
      *@param sendTimeout 
      *@param recvTimeout
      */

      public function __construct($serverip,$port,$sendTimeout,$recvTimeout){
        $this->socket = new TSocket($serverip,$port);
        $this->socket->setSendTimeout(empty($sendTimeout)?$this->sendTimeout:$sendTimeout); 
        $this->socket->setRecvTimeout(empty($recvTimeout)?$this->recvTimeout:$recvTimeout); 
        $this->transport = new TBufferedTransport($this->socket);  
        $this->protocol = new TBinaryProtocol($this->transport);  
        $this->client = new HbaseClient($this->protocol);
        $this->transport->open();
        echo "init ok\n";
        echo $sendTimeout,$recvTimeout,"\n";
      }

      /**
      *获取所有表名
	  *@return array
      */
      public function getAllTableNames(){
          return $this->client->getTableNames();
      }
      /**
      *建表
      *@param table 表名
      *@param familyArray 列簇名数组 
      *throw AlreadyExists , Exception
      */
      public function create($table,$familyArray){
        if(is_array($familyArray)){
            $columns = array();
            foreach ($familyArray as $value) {
              $columns[] = new ColumnDescriptor(array('name'=>"{$value}:"));
            }
            try {
                    $this->client->createTable($table,$columns);
                } catch (AlreadyExists $ae){
                    throw $ae;
                }
            
        }else{
          throw new Exception("familyArray is not array");
        }

      } 
      /**
      *删除表
      *@param table 表名
      */
      public function drop($table){
            $this->client->disableTable($table);
            $this->client->deleteTable($table);
      }
      /**
      *插入数据
	  *@param table 表名
	  *@param rows 插入数据数组
      */
      public function insert($table,$rows,$attributes=array()){
        $puts = array();
        foreach ($rows as $key => $value) {
          $batchMutation  = new BatchMutation();
          $batchMutation->row=$key;
          foreach ($value as $col => $val) {
            $record = new Mutation(array('column'=>$col,'value'=>$val));
            $batchMutation->mutations[] = $record;
          } 
          $puts[] =  $batchMutation;
        }
        $this->client->mutateRows($table,$puts,$attributes);

      } 
      /**
      *查询数据
	  *@param table 表名
	  *@param rows 查询主键数组
	  *@param $columns 列簇名数组
      */
      public function query($table,$rows,$columns,$attributes=array()){
          try{
				
              return $this->client->getRowsWithColumns($table,$rows,$columns,$attributes);

          } catch ( Exception $e) {
             throw $e;
          }
      }
      /**
      *删除数据
	  *@param table 表名
	  *@param row 主键
	  *@param column 列名
      */
      public function delete($table, $row, $column, $attributes=array()){
        try{

          return  $this->client->deleteAll($table, $row, $column, $attributes);
        }catch(Exception $e){
          throw $e;

        }
       
      }
	  /**
	  *将查询结果转换为管理数组
	  *@param result  查询结果集
	  *@throw Exception
	  *@return array
	  */
	  public function fetchArray($result){
	   if(is_array($result)){
			$array=array();
			foreach($result as $rowresult){
			$record = array();
			 if($rowresult instanceof TRowResult){
				$record['row']=$rowresult->row;
				foreach($rowresult->columns as $key => $cell){
					if($cell instanceof TCell){
						$record[$key]=$cell->value;
					}else{
						throw new Exception("cell element is not instance of TCell");
					}
				}
			 }else{
				throw new Exception("result element is not instance of TRowResult");
			 }
			 $array[]=$record;
			}
			return $array;
	  }else{
		throw new Exception("result is not array");
	  }
	 }
	  /**
	  *根据前缀查询数据
	  *@param tbale 表名
	  *@param startAndPrefix 前缀
	  *@param columns 列簇名数组
	  *@throw Exception
	  *@return array
	  */
	  public function queryByPrefix($table, $startAndPrefix, $columns, $attributes=array()){
		
		try{
			$scanid=$this->client->scannerOpenWithPrefix($table, $startAndPrefix, $columns, $attributes);
			$result=$this->fetchArray($this->client->scannerGet($scanid));
			$this->client->scannerClose($scanid);
			return $result;
        }catch(Exception $e){
          throw $e;

        }
	  }
	  /**
	  *根据起始和结束键查询
	  *@param tbale 表名
	  *@param startRow 起始主键
	  *@param stopRow 结束主键
	  *@param columns 列簇名数组
	  *@throw Exception
	  *@return array
	  */
	  public function queryByStartAndStopKey($table, $startRow, $stopRow, $columns, $attributes=array()){
	  
	  try{
		    $scanid=$this->client->scannerOpenWithStop($table, $startRow, $stopRow, $columns, $attributes);
			$result=$this->fetchArray($this->client->scannerGet($scanid));
			$this->client->scannerClose($scanid);
			return $result;
        }catch(Exception $e){
          throw $e;

        }
	  
	  }

      /**
	  *析构函数
	  */
      public function __destruct(){
        $this->transport->close();

      }
  }

} 



