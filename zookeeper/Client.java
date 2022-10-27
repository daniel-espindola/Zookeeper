package zookeeper;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.Timestamp;

import com.google.gson.Gson;

public class Client {
   private static String[] Ip_Servidor = new String[3];
   private static int[] Porta_Servidor = new int[3];
   private static Gson g = new Gson();
   private static Map<String, String> map = new HashMap<String, String>();
   private static Map<String, Timestamp> ts = new HashMap<String, Timestamp>();
   
  /**
   *
   * @param sc O Scanner para ler do teclado
   */
  public static void obtemDadosTeclado(Scanner sc) {
    System.out.println("Funcionalidade INICIALIZA selecionada\n");
    String ipPorta;
    
    int i = 0;
    while (i < 3) {
      System.out.println("Insira o IP do servidor "+i+":");
      ipPorta = sc.nextLine();
      if (ipPorta.split(":").length != 2) {
        System.out.println("Formato inválido! por favor inserir no formato ip:porta");
        continue;
      }
      Ip_Servidor[i] = ipPorta.split(":")[0];
      Porta_Servidor[i] = Integer.parseInt(ipPorta.split(":")[1]);
      i++;
    }
  }

  /**
   *
   * @param sc O Scanner para ler do teclado
   */
  public static Message enviaMensagem(Message msg){
    Random rng = new Random(3141527);
    int i = rng.nextInt(2);
    try {
      Socket s = new Socket(Ip_Servidor[i], Porta_Servidor[i]);
      // Escreve pelo socket
      OutputStream os = s.getOutputStream();
      DataOutputStream writer = new DataOutputStream(os);

      // Lê pelo socket
      InputStreamReader is = new InputStreamReader(s.getInputStream());
      BufferedReader reader = new BufferedReader(is);
      
      String sendData = g.toJson(msg);
      writer.writeBytes(sendData + "\n");

      String response = reader.readLine();
      s.close();
      return g.fromJson(response, Message.class);

    } catch (Exception e) {}

    return null;
  }

  public static void main(String[] args) throws Exception {
	    Scanner sc = new Scanner(System.in);
      Message sendMsg;
      String key;
      String value;
      Message res;

	    while(true) {

		    System.out.println("Selecione uma funcionalidade:\nINIT\nPUT\nGET\n");
		    String funcao = sc.nextLine().toUpperCase();
		    
        Ip_Servidor[0] = "127.0.0.1";
        Ip_Servidor[1] = "127.0.0.1";
        Ip_Servidor[2] = "127.0.0.1";
        Porta_Servidor[0] = 10097;
        Porta_Servidor[1] = 10097;
        Porta_Servidor[2] = 10097;
		    switch(funcao) {
		    	case "INICIALIZA":
				    //obtemDadosTeclado(sc);
		    		break;

		    	case "PUT":
				    System.out.println("Funcionalidade PUT selecionada\nInsira a key e value a ser inserido (key value):\n");
            key = sc.nextLine();
            value = key.split(" ")[1];
            key = key.split(" ")[0];
            
            sendMsg = new Message();
            sendMsg.Tipo = "PUT";
            sendMsg.Key = key;
            sendMsg.Value = value;

            res = enviaMensagem(sendMsg);
            map.put(res.Key, res.Value);
            ts.put(res.Key, res.ts);
            
            System.out.println("PUT_OK key: "+sendMsg.Key+" value "+sendMsg.Value+" timestamp "+sendMsg.ts+" realizada no servidor "+sendMsg.Ip_Destino+":"+sendMsg.Porta_Destino+"\n");
          
						break;

		    	case "GET":
				    System.out.println("Funcionalidade GET selecionada\nInsira a key a ser procurada:\n");

            key = sc.nextLine();
            sendMsg = new Message();
            sendMsg.Tipo = "GET";
            sendMsg.Key = key;
            if (ts.containsKey(key))
              sendMsg.ts = ts.get(key);

            res = enviaMensagem(sendMsg);
            if(res.Value == null)
              break;
              
            ts.replace(key, res.ts);
            System.out.println("GET key: "+sendMsg.Key+" value: "+sendMsg.Value+" obtido do servidor " + sendMsg.Ip_Destino + ":" +sendMsg.Porta_Destino +", meu timestamp "+sendMsg.ts+" e do servidor "+res.ts+"\n");

						break;
		    		
		    	default:
		    		System.out.println("Funcionalidade invalida");
		    		break;
		    }
	    }
	}

}