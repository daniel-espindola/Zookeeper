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
   private static Map<String, Long> ts = new HashMap<String, Long>();
   private static Random rng = new Random(System.currentTimeMillis());

  /**
   * Funcionalidade (A) do cliente
   * Captura do teclado 3 ips e portas dos servidores, sem distinção de quem é o líder
   * @param sc O Scanner para ler do teclado
   */
  public static void obtemDadosTeclado(Scanner sc) {
    System.out.println("Funcionalidade INICIALIZA selecionada\n");
    String ipPorta;
    
    int i = 0;
    while (i < 3) {
      System.out.println("Insira o IP:Porta do servidor "+i+":");
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
   * Recebe uma mensagem, sorteia um dos 3 servidores inseridos pelo cliente e envia a mensagem pra eles
   * @param msg A Memssagem a ser enviada
   * @return a Mensagem de resposta recebida do servidor
   */
  public static Message enviaMensagem(Message msg){
    int i = rng.nextInt(3);
    try {
      Socket s = new Socket(Ip_Servidor[i], Porta_Servidor[i]);
      // Escreve pelo socket
      OutputStream os = s.getOutputStream();
      DataOutputStream writer = new DataOutputStream(os);

      // Lê pelo socket
      InputStreamReader is = new InputStreamReader(s.getInputStream());
      BufferedReader reader = new BufferedReader(is);

      msg.Ip_Requisitante = s.getLocalAddress().toString();
      msg.Porta_Requisitante = s.getLocalPort();

      String sendData = g.toJson(msg);
      writer.writeBytes(sendData + "\n");
      String response = reader.readLine();
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

		    switch(funcao) {
		    	case "INIT":
				    obtemDadosTeclado(sc);
		    		break;

		    	case "PUT":
            // Funcionalidade (B) - Captura do teclado o key e value e envia a mensagem PUT a um servidor aleatório
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

            System.out.println("PUT_OK key: " + sendMsg.Key + " value: " + sendMsg.Value + " timestamp: " + res.ts
                + " realizada no servidor " + res.Ip_Destino + ":" + res.Porta_Destino + "\n");

						break;

		    	case "GET":
            // Funcionalidade (C) - Captura do teclado a key a ser procurada e envia uma mensagem GET a um servidor aleatório
				    System.out.println("Funcionalidade GET selecionada\nInsira a key a ser procurada:\n");

            key = sc.nextLine();
            sendMsg = new Message();
            sendMsg.Tipo = "GET";
            sendMsg.Key = key;
            if (ts.containsKey(key))
              sendMsg.ts = ts.get(key);

            res = enviaMensagem(sendMsg);
            

            System.out.println("GET key: "+sendMsg.Key+" value: "+res.Value+" obtido do servidor " + res.Ip_Destino + ":" +res.Porta_Destino +", meu timestamp "+sendMsg.ts+" e do servidor "+res.ts+"\n");
            if(res.Value != null) {
              if(map.containsKey(key))
                map.replace(key, res.Value);
              else
                map.put(key, res.Value);

              if (ts.containsKey(key))
                ts.replace(key, res.ts);
              else
                ts.put(key, res.ts);
            }

						break;
		    		
		    	default:
		    		System.out.println("Funcionalidade invalida");
		    		break;
		    }
	    }
	}

}