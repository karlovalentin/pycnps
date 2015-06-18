<?php
error_reporting(E_ALL ^ E_NOTICE);
ini_set('display_errors', 1);
$mysqli = new mysqli("173.194.251.199", "receptor", "mofos2014", "cinepolis");
if ($mysqli->connect_errno) {
    echo "Fallo al contenctar a MySQL: (" . $mysqli->connect_errno . ") " . $mysqli->connect_error;
}



?>