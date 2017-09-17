package rpc;

public class RpcProvide {
    public static void main(String[] args) throws Exception {
        RpcFramework.export(new HelloServiceImpl(),HelloService.class,8080);
    }

}
