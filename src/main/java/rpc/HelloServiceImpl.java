package rpc;

/**
 * Created by dmf on 2017/9/15.
 */
public class HelloServiceImpl implements HelloService {

    public String say() {
        return "hello";
    }

    public String say(String name) {
        return "hello:"+ name;
    }
}
