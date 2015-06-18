<?php

#include("../conexion.php");

error_reporting(E_ALL ^ E_NOTICE);
ini_set('display_errors', 1);
//conexion cassandra
$username='sparkia';
$password='Mofos2014';

$db = new PDO("cassandra:host=10.240.207.160;port=9160;cqlversion=3.0.0", $username, $password);
$db->exec("USE cinepolis");


$humandate= date("Y-m-d G:i:s",time());

$uploaded=time();

$deviceID=$_POST['gateway'];

if ($_POST['clave']!='mofos2014') {
	exit('WRONG PASSWORD BITCH!!!');
	//echo "WRONG PASSWORD BITCH!!!";
	//exit();
}

$datado=$uploaded."-".$deviceID."-".rand(0,99999).".csv";

$uploaddir = "/var/www/csv2/";
$uploadfile = $uploaddir . $datado;

$original=basename($_FILES['file_contents']['name']);

//echo '<pre>';
	if (move_uploaded_file($_FILES['file_contents']['tmp_name'], $uploadfile)) {
//echo "1";
//$volc=1;
$pros=0;
	$stmt = $db->prepare("INSERT into prosfiles (name_file,beginpros,humandate,lastpros,originalname,pros,uploaded,uploader) values ('$datado','$humandate','$humandate','$humandate','$original',$pros,$uploaded,'$deviceID');");
$stmt->execute();
//var_dump($stmt);
echo "1";
	} else {
echo "not upload 0";

	}




















/*
this was V1
$datado=$uploaded."-".$deviceID."-".rand(0,9999).".csv";

$uploaddir = "/var/www/csv2/";
$uploadfile = $uploaddir . $datado;

$original=basename($_FILES['file_contents']['name']);

//echo '<pre>';
	if (move_uploaded_file($_FILES['file_contents']['tmp_name'], $uploadfile)) {
//echo "1";
//$volc=1;

	$insert=$mysqli->query("INSERT into prosfiles values ('','$datado','$deviceID','$uploaded','0','$original','$humandate','')");
echo $mysqli->affected_rows;

	} else {
echo "not upload 0";

	}


*/

?>