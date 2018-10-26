<?php
// https://gist.github.com/mathiasverraes/9046427#gistcomment-2252932
//function it($m,$p){echo"\e[3".($p?"2m✔":"1m✘")."\e[0m It $m\n";if(!$p){$GLOBALS['e']=1;$d=debug_backtrace()[0];echo"ERROR {$d['file']}@{$d['line']}\n";}}register_shutdown_function(function(){echo"\e[1;3".(($e=@$GLOBALS['e'])?"7;41mFAIL":"2mOK")."\e[0m\n";die($e);});

function it($m,$p){
 $d=debug_backtrace(0)[0];
 is_callable($p) and $p=$p();
 global $e;$e=$e||!$p;
 $o=($p?"✔":"✘")." It $m";
 fwrite($p?STDOUT:STDERR,$p?"$o\n":"$o FAIL: {$d['file']} #{$d['line']}\n");
}

register_shutdown_function(function(){global $e; $e and die(1);});

function throws($exp,Closure $cb){try{$cb();}catch(Exception $e){return $e instanceof $exp;}return false;}

