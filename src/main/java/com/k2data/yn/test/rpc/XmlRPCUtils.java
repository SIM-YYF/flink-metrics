package com.k2data.yn.test.rpc;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class XmlRPCUtils {

    private static XmlRpcClient client = new XmlRpcClient();

    private static void initRpcClient(String rpcChannel) {
        XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
        try {
            config.setServerURL(new URL("http://" + rpcChannel));
            config.setEnabledForExtensions(true);
            config.setGzipCompressing(true);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        client.setConfig(config);
    }

    /**
     * 执行算子
     *
     * @param rpcChannel   rpcHost:rpcPort
     * @param operatorName
     * @param data
     * @param index
     * @param kwargs
     * @return
     * @throws XmlRpcException
     */
    public static Object[] callRPC(String rpcChannel, String operatorName, Map<String, List<Object>> data, List<Object> index, Map<String, Object> kwargs) throws XmlRpcException {
        if (((XmlRpcClientConfigImpl) client.getClientConfig()).getServerURL() == null) {
            initRpcClient(rpcChannel);
        }

        Object[] params = new Object[4];

        params[0] = operatorName;
        params[1] = data;
        params[2] = index;
        params[3] = kwargs;

        Object[] response = (Object[]) client.execute("run_operator", params);

        return response;
    }

    /**
     * 同步算子
     *
     * @param operatorInfos
     * @return
     * @throws XmlRpcException
     */
    public static Object[] syncOperatorRPC(List<Object> operatorInfos) throws XmlRpcException {
        Object[] params = new Object[1];

        params[0] = operatorInfos;

        Object[] response = (Object[]) client.execute("sync_operators", params);

        return response;
    }


}
