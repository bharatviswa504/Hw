package org.bh.hadooprpc;

/**
 * Created by bharatviswanadham on 6/21/17.
 */
import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;


/*
 * https://wiki.apache.org/hadoop/HadoopRpc
 * https://github.com/elazarl/hadoop_rpc_walktrhough/
 */
public class HadoopRpc
{
    @ProtocolInfo(protocolName = "ping", protocolVersion = 1)
    public static interface PingProtocol  {
        String ping();
        String hostName();
    }

    public static class Ping implements PingProtocol {
        public String ping() {
            System.out.println("Server: ");
            return "pong";
        }

        public String hostName() {
            String hostname = "";
            try {
                InetAddress ip;
                ip = InetAddress.getLocalHost();
                hostname = ip.getHostName();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            return hostname;
        }
    }

    public static InetSocketAddress addr = new InetSocketAddress("localhost", 5122);

    public static RPC.Server server() throws IOException {
        final RPC.Server server = new RPC.Builder(new Configuration()).
                setBindAddress(addr.getHostName()).
                setPort(addr.getPort()).
                setInstance(new Ping()).
                setProtocol(PingProtocol.class).
                build();
        server.start();
        return server;
    }

    public static void client() throws IOException {
        final PingProtocol proxy = RPC.getProxy(PingProtocol.class, RPC.getProtocolVersion(PingProtocol.class),
                addr, new Configuration());

        System.out.println("Client: ping " + proxy.ping());


        System.out.println("Rpc Protocol Name " + RPC.getProtocolName(PingProtocol.class));

        System.out.println("Rpc Protocol Version " + RPC.getProtocolVersion(PingProtocol.class));

        System.out.println("Client: finding hostname of server " + proxy.hostName());
    }

    public static void main(String[] args ) throws IOException {
        final String runThis = args.length > 0 ? args[0] : "";
        if (runThis.equals("server")) {
            server();
        } else if (runThis.equals("client")) {
            client();
        } else {
            final RPC.Server server = server();
            client();
            server.stop();
        }

    }
}
