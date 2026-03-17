package rt.analysis;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
 
@RestController
public class KafkaController {

    @GetMapping("/welcome")
    public String welcome(){

        return "welcome to real time analysis";
    }

    @GetMapping("/realTimeGenerate")
    public void threadMonitor(){

    }
}
