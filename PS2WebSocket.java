package org.myorg.project;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

// library:  https://github.com/TooTallNate/Java-WebSocket

/*
    Planetside 2 websocket - Flink native does not support a websocket service as input
    Flink needs to connect to localhost:1337 in order to get messages from this planetside 2 websocket service
*/

public class PS2WebSocket extends WebSocketClient {

    private PrintWriter out = null;

    private PS2WebSocket(URI serverURI) {
        super( serverURI );
    }

    @Override
    public void onOpen( ServerHandshake handshakedata ) {
        System.out.println( "open" );
    }

    @Override
    public void onMessage( String message ) {
        //System.out.println( "received: " + message );
        out.write(message+"\n");
        out.flush();
    }

    @Override
    public void onClose( int code, String reason, boolean remote ) {
        System.out.println( "Connection closed by " + ( remote ? "remote peer" : "us" ) + " Code: " + code + " Reason: " + reason );
    }

    @Override
    public void onError( Exception ex ) { ex.printStackTrace(); }

    private void start_server() {
        try{

            ServerSocket serverSocket = new ServerSocket(1337);
            Socket socket = serverSocket.accept();
            out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));

            connect();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void send_msg_to_wss(String msg){
        try{
            send(msg);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args){

        // wss address
        String addr = "wss://push.planetside2.com/streaming?environment=ps2&service-id=s:MYIDHERE";

        // messages to get the data
        String msgLogInOut = "{\"service\":\"event\",\"action\":\"subscribe\",\"worlds\":[\"all\"],\"eventNames\":[\"PlayerLogin\",\"PlayerLogout\"]}";
        String msgExperience = "{\"service\":\"event\",\"action\":\"subscribe\",\"characters\":[\"all\"],\"worlds\":[\"all\"],\"eventNames\":[\"GainExperience\"],\"logicalAndCharactersWithWorlds\":true}";
        String msgDeath = "{\"service\":\"event\",\"action\":\"subscribe\",\"characters\":[\"all\"],\"worlds\":[\"all\"],\"eventNames\":[\"Death\"],\"logicalAndCharactersWithWorlds\":true}";
        String msgFacility = "{\"service\":\"event\",\"action\":\"subscribe\",\"characters\":[\"all\"],\"worlds\":[\"all\"],\"eventNames\":[\"PlayerFacilityCapture\",\"PlayerFacilityDefend\"]}";

        // launch
        try{
            PS2WebSocket ws = new PS2WebSocket(new URI(addr));
            ws.start_server();
            Thread.sleep(4000);
            ws.send_msg_to_wss(msgLogInOut);
            ws.send_msg_to_wss(msgExperience);
            ws.send_msg_to_wss(msgDeath);
            ws.send_msg_to_wss(msgFacility);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
