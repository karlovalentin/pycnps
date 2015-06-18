<?php

error_reporting(E_ALL ^ E_NOTICE);
ini_set('display_errors', 1);
//conexion cassandra
$username='sparkia';
$password='Mofos2014';

$db = new PDO("cassandra:host=10.240.207.160;port=9160;cqlversion=3.0.0", $username, $password);
$db->exec("USE apeplazas");


$humandate= date("Y-m-d G:i:s",time());

$node=$_POST[node];
$uptime=$_POST[uptime];
$node_time=$_POST[node_time];
#echo "$node_time";

if ($_POST['clave']!='mofos2014') {
	exit('WRONG PASSWORD BITCH!!!ape');
	//echo "WRONG PASSWORD BITCH!!!";
	//exit();
}


$stmt = $db->prepare("INSERT into beats (node,reported,nodetime,uptime) values ('$node','$humandate','$node_time','$uptime');");
$stmt->execute();

echo "reported beat $humandate\n";


?>