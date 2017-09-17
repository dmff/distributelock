package rpc;

public class RpcConsummer {

    public static void main(String[] args) throws Exception {
        HelloService service = RpcFramework.refer(HelloService.class, "127.0.0.1", 8080);
        String result = service.say("dmf");
        System.out.println(result);
    }
}
