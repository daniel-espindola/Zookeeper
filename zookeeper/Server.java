package zookeeper;

import java.net.Socket;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import com.google.gson.Gson;

public class Server {
  private static String Ip_Servidor;
  private static int Porta_Servidor;
  private static String Ip_Lider;
  private static int Porta_Lider;
  private static String Ip_Server_2;
  private static int Porta_Server_2;
  private static String Ip_Server_3;
  private static int Porta_Server_3;
  private static ServerSocket serverSocket;
  private static Gson g = new Gson();
  private static Map<String, String> map = new HashMap<String, String>();
  private static Map<String, Long> ts = new HashMap<String, Long>();

  public static boolean souLider() {
    return Ip_Servidor.equals(Ip_Lider) && Porta_Servidor == Porta_Lider;
  }

  public static Message enviaMensagem(Message msg) {
    try {
      Socket s = new Socket(msg.Ip_Destino, msg.Porta_Destino);
      // Escreve pelo socket
      OutputStream os = s.getOutputStream();
      DataOutputStream writer = new DataOutputStream(os);

      // Lê pelo socket
      InputStreamReader is = new InputStreamReader(s.getInputStream());
      BufferedReader reader = new BufferedReader(is);

      String sendData = g.toJson(msg);
      writer.writeBytes(sendData + "\n");

      String response = reader.readLine();
      return g.fromJson(response, Message.class);

    } catch (Exception e) {
    }

    return null;
  }

  public static String removeCaracteresFim(String s, int c) {
    return s.substring(0, s.length() - c);
  }

  /**
   * Funcionalidade (B) do servidor
   * Essa thread é inicializada toda vez que o servidor recebe um request
   * permitindo que multiplos requests sejam tratados ao mesmo tempo
   */
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
        String sendData;
        Message res;
        
        switch(msg.Tipo) {
          /**
           * Funcionalidade (C) do servidor
           * Lida com as requisições do tipo PUT
           * inserindo e replicando a informação caso seja o líder
           * ou encaminhando para o líder caso contrário
           */
          case "PUT":
            if(souLider()) {
              System.out.println("Cliente " + msg.Ip_Requisitante + ":" + msg.Porta_Requisitante + " PUT key:" + msg.Key
                  + " value:" + msg.Value);
                  
              Long tsAtual = System.currentTimeMillis();
              if (map.containsKey(msg.Key) == false) {
                map.put(msg.Key, msg.Value);
                ts.put(msg.Key, tsAtual);
              } else {
                map.replace(msg.Key, msg.Value);
                ts.replace(msg.Key, tsAtual);
              }

              Message msgReplication = new Message();
              msgReplication.Ip_Destino = Ip_Server_2;
              msgReplication.Tipo = "REPLICATION";
              msgReplication.Porta_Destino = Porta_Server_2;
              msgReplication.Key = msg.Key;
              msgReplication.Value = msg.Value;
              msgReplication.ts = tsAtual;
              
              Message resReplication1 = enviaMensagem(msgReplication);

              msgReplication.Ip_Destino = Ip_Server_3;
              msgReplication.Porta_Destino = Porta_Server_3;
              
              Message resReplication2 = enviaMensagem(msgReplication);
              
              /**
               * Recebe o REPLICATION_OK dos demais servidores e caso esteja tudo válido envia o PUT_OK de volta para o cliente
               */
              if (resReplication1.Tipo.equals("REPLICATION_OK") && resReplication2.Tipo.equals("REPLICATION_OK")) {
                System.out.println(
                    "Enviando PUT_OK ao cliente " + msg.Ip_Requisitante + ":" + msg.Porta_Requisitante + " da key:"
                        + msg.Key
                        + " ts:" + tsAtual);
              } else {
                System.out.println(
                    "Erro no replication " + resReplication1 + resReplication2);
                return;
              }
              res = new Message();
              res.Tipo = "PUT_OK";
              res.ts = tsAtual;

            } else {
              System.out.println("Encaminhando PUT key:" + msg.Key + " value:" + msg.Value);
              msg.Ip_Destino = Ip_Lider;
              msg.Porta_Destino = Porta_Lider;
              res = enviaMensagem(msg);
            }
            
            res.Ip_Destino = Ip_Lider;
            res.Porta_Destino = Porta_Lider;
            sendData = g.toJson(res);
            writer.writeBytes(sendData + "\n");

          break;

        /**
         * Funcionalidade (D) do servidor
         * Recebe uma requisição replication e insere o valor e timestamp, retornando um REPLICATION_OK
         */
          case "REPLICATION":
            System.out.println("REPLICATION key:" + msg.Key + " value:" + msg.Value + " ts:" + msg.ts);
            if (map.containsKey(msg.Key) == false) {
              map.put(msg.Key, msg.Value);
              ts.put(msg.Key, msg.ts);
            } else {
              map.replace(msg.Key, msg.Value);
              ts.replace(msg.Key, msg.ts);
            }

            msg.Tipo = "REPLICATION_OK";
            sendData = g.toJson(msg);
            writer.writeBytes(sendData + "\n");
          break;

          /**
         * Funcionalidade (F) do servidor
         * Recebe a requisição GET de um cliente e retorna um value null caso não encontre a key, 
         * TRY_OTHER_SERVER_OR_LATER caso o ts do cliente seja superior ao do servidor 
         * ou o value e timestamp que estava salvo nesse servidor, caso encontre a key.
         */
          case "GET":
            msg.Ip_Destino = Ip_Servidor;
            msg.Porta_Destino = Porta_Servidor;
            if(map.containsKey(msg.Key) == false) {
              
              System.out
                  .println("Cliente " + s.getInetAddress().toString() + ":" + s.getPort() + " GET da key:" + msg.Key
                      + " ts:" + msg.ts + ". Meu ts é null portanto devolvendo null\n");
                    
              msg.Value = null;
              sendData = g.toJson(msg);
              writer.writeBytes(sendData + "\n");
              
              break;
            }

            if(msg.ts == null || ts.get(msg.Key) >= msg.ts) {
              msg.Value = map.get(msg.Key);
              System.out
                  .println("Cliente " + s.getInetAddress().toString() + ":" + s.getPort() + " GET da key:" + msg.Key
                      + " ts:" + msg.ts + ". Meu ts é " + ts.get(msg.Key) + ", portanto devolvendo " + msg.Value+"\n");
              
              msg.ts = ts.get(msg.Key);
            } else {
              System.out
                  .println("Cliente " + s.getInetAddress().toString() + ":" + s.getPort() + " GET da key:" + msg.Key
                      + " ts:" + msg.ts + ". Meu ts é " + ts.get(msg.Key) + ", portanto devolvendo TRY_OTHER_SERVER_OR_LATER"
                      + "\n");
                      msg.Value = "TRY_OTHER_SERVER_OR_LATER";
                      msg.ts = ts.get(msg.Key);
            }

            sendData = g.toJson(msg);
            writer.writeBytes(sendData + "\n");
          break;
        }

        writer.writeBytes("response" +"\n");

        s.close();

      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Scanner sc = new Scanner(System.in);
    
    /**
     * Funcionalidade (A) do servidor
     * Recebe o ip e porta desse servidor e do servidor líder
     * os demais servidores são os que sobraram das portas padrão
     */
    System.out.println("Por favor insira o ip:porta desse servidor\n");
    String ipPorta = sc.nextLine();
    
    Ip_Servidor = ipPorta.split(":")[0];
    Porta_Servidor = Integer.parseInt(ipPorta.split(":")[1]);

    System.out.println("Por favor insira o ip:porta do servidor líder\n");
    ipPorta = sc.nextLine();

    Ip_Lider = ipPorta.split(":")[0];
    Porta_Lider = Integer.parseInt(ipPorta.split(":")[1]);

    if(Porta_Servidor ==10097) Porta_Server_2 = 10098; Porta_Server_3 = 10099;
    if(Porta_Servidor ==10098) Porta_Server_2 = 10097; Porta_Server_3 = 10099;
    if(Porta_Servidor ==10099) Porta_Server_2 = 10097; Porta_Server_3 = 10098;

    serverSocket = new ServerSocket(Porta_Servidor);

    sc.close();
    while (true) {
      Socket no = serverSocket.accept();

      ThreadAtendimento thread = new ThreadAtendimento(no);
      thread.start();
    }
  }

}