<?php
class conductor_client{
    public static function repeat(string $ip, int $port = 52000){
        $noJobLastTime = false;
        while(true){
            $lastTime = time::stamp();
            $jobData = self::getJob($ip, $port);
            if(is_array($jobData)){
                if(isset($jobData['id'])){
                    $noJobLastTime = false;
                    if(isset($jobData['action'])){
                        if(is_string($jobData['action'])){
                            $return = false;
                            $error = false;
                            try{
                                echo "Executing " . $jobData['action'] . "\n";
                                $return = eval('return ' . $jobData['action'] . ';');
                            }
                            catch(Throwable $throwable){
                                $error = true;
                                $return = false;
                                echo "Error running " . $jobData['action'] . ": " . explode("\n",$throwable)[0] . "\n";
                            }

                            if(!self::updateJob($ip, $jobData['id'], $return, $error, $port)){
                                echo "Failed to update job\n";
                            }
                        }
                        else{
                            echo "Job action is not a string\n";
                        }
                    }
                    else{
                        echo "No action specified\n";
                    }
                }
                else{
                    if($noJobLastTime === false){
                        echo "No jobs available (retrying every 5 seconds)\n";
                        $noJobLastTime = true;
                    }
                }
            }
            else{
                echo "Error connecting to conductor server\n";
            }

            while((time::stamp() - $lastTime) < 5){
                sleep(1);
            }
            
            if(is_file("temp/conductor_client/stop")){
                mklog("general","Conductor Client: Stop file found, stopping",false);
                return;
            }
        }
    }
    public static function addJob(string $ip, string $function, int $port = 52000, bool|string $finishFunction = false):string|false{
        $requirements = array();
        $dotspos = strpos($function,"::");
        if($dotspos !== false){
            $package = substr($function,0,$dotspos);
            $requirements[$package] = 1;
        }

        $data = array(
            "type"    => "addJob",
            "payload" => array(
                "requirements" => $requirements,
                "action_type" => "function_string",
                "action" => $function
            )
        );

        if(is_string($finishFunction)){
            $data['payload']['finish_function'] = $finishFunction;
        }

        $result = self::run($ip, $data, $port);

        if($result['success']){
            return $result['job_id'];
        }
        return false;
    }
    public static function getJob(string $ip, int $port = 52000):array|false{
        $data = array(
            "abilities" => pkgmgr::getLoadedPackages()
        );

        $result = self::run($ip,array("type"=>"requestJob","payload"=>$data),$port);
        if($result['success']){
            if(isset($result['job'])){
                return $result['job'];
            }
            else{
                return array();
            }
        }

        if(isset($result['error'])){
            echo "Conductor error: " . $result['error'] . "\n";
        }
        return false;
    }
    public static function updateJob(string $ip, string $jobId, mixed $return, bool $error, int $port = 52000):bool{
        $data = array(
            "id" => $jobId,
            "return" => $return,
            "error_completing" => $error
        );

        $result = self::run($ip,array("type"=>"updateJob","payload"=>$data),$port);
        if($result['success']){
            return true;
        }

        if(isset($result['error'])){
            echo "Conductor error: " . $result['error'] . "\n";
        }
        return false;
    }
    public static function run(string $ip, array $data, int $port = 52000):array{
        $socket = communicator::connect($ip,$port,false,$socketError,$socketErrorString);
        if($socket !== false){
            return self::execute($socket,$data);
        }
        return array("success"=>false,"error"=>"Unable to connect to " . $ip . ":" . $port);
    }
    private static function execute($socket, array $data):array{
        $return = array("success"=>false);

        if(!isset($data['type'])){
            $return["error"] = "Type not set";
            goto end;
        }

        if(!isset($data['payload'])){
            $return["error"] = "Payload not set";
            goto end;
        }

        $data['name'] = communicator::getName();
        $data['password'] = communicator::getPasswordEncoded();

        $data = base64_encode(json_encode($data));

        if(!communicator::send($socket,$data)){
            $return["error"] = "Error sending data";
            goto end;
        }

        $result = communicator::receive($socket);
        if($result === false){
            $return["error"] = "Error receiving data";
            goto end;
        }

        $result = json_decode(base64_decode($result),true);
        if($result === null){
            $return["error"] = "Empty response";
            goto end;
        }

        end:
        communicator::close($socket);
        return $result;
    }
}