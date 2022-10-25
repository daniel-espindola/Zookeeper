package zookeeper;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import com.google.gson.Gson;

public class Server {
  private static String Ip_Servidor;
  private static int Porta_Servidor;
  private static String Ip_Lider;
  private static int Porta_Lider;
  private static ServerSocket serverSocket;
  private static Gson g = new Gson();
  private static Map<String, String> map = new HashMap<String, String>();

  public static class ThreadAtendimento extends Thread {
    private Socket s;
    public ThreadAtendimento(Socket no) {
      s = no;
    }

    public void run() {
      InputStreamReader is;

      try {
        is = new InputStreamReader(s.getInputStream());
        BufferedReader reader = new BufferedReader(is);

        OutputStream os;
        os = s.getOutputStream();

        DataOutputStream writer = new DataOutputStream(os);

        String requestStr = reader.readLine();
        Message msg = new Gson().fromJson(requestStr, Message.class);

        System.out.println("mensagem recebida");
        switch(msg.Tipo) {
          case "PUT":
            System.out.println("recebido uma requisição PUT " + msg.Corpo);
          break;

          case "REPLICATION":
            System.out.println("recebido uma requisição REPLICATION " + msg.Corpo);
          break;

          case "GET":
            System.out.println("recebido uma requisição GET " + msg.Corpo);
          break;
        }

        writer.writeBytes("response");

        s.close();

      } catch (Exception e) {

      }
    }
  }

  public static void main(String[] args) throws Exception {
    Scanner sc = new Scanner(System.in);
    
    System.out.println("Por favor insira o ip:porta desse servidor\n");
    String ipPorta = sc.nextLine();
    
    Ip_Servidor = ipPorta.split(":")[0];
    Porta_Servidor = Integer.parseInt(ipPorta.split(":")[1]);

    System.out.println("Por favor insira o ip:porta do servidor líder\n");
    ipPorta = sc.nextLine();

    Ip_Lider = ipPorta.split(":")[0];
    Porta_Lider = Integer.parseInt(ipPorta.split(":")[1]);

    serverSocket = new ServerSocket(Porta_Servidor);

    while (true) {
      Socket no = serverSocket.accept();

      ThreadAtendimento thread = new ThreadAtendimento(no);
      thread.start();
    }
  }

}