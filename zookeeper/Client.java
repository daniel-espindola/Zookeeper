package zookeeper;

import java.util.Scanner;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import com.google.gson.Gson;

public class Client {
   private static String[] Ip_Servidor = new String[3];
   private static int[] Porta_Servidor = new int[3];
   private static Socket s;
   private static Gson g = new Gson();

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

    try {
      // Escreve pelo socket
      OutputStream os = s.getOutputStream();
      DataOutputStream writer = new DataOutputStream(os);

      // Lê pelo socket
      InputStreamReader is = new InputStreamReader(s.getInputStream());
      BufferedReader reader = new BufferedReader(is);
      
      String sendData = g.toJson(msg);
      writer.writeBytes(sendData + "\n");
      System.out.println("mensagem enviada");

      String response = reader.readLine();
      
      System.out.println("Do Servidor: " + response);
      return g.fromJson(response, Message.class);

    } catch (Exception e) {}

    return null;
  }

  public static void main(String[] args) throws Exception {
	    Scanner sc = new Scanner(System.in);
      s = new Socket("127.0.0.1", 9000);
      Message sendMsg;
      String key;
      String value;

	    while(true) {

		    System.out.println("Selecione uma funcionalidade:\n - INICIALIZA\n - PUT\n - GET\n");
		    String funcao = sc.nextLine().toUpperCase();
		    
		    switch(funcao) {
		    	case "INICIALIZA":
				    
		    		break;

		    	case "PUT":
				    System.out.println("Funcionalidade PUT selecionada\nInsira a key e value a ser inserido:\n");
            key = sc.nextLine();
            value = sc.nextLine();
            
            sendMsg = new Message();
            sendMsg.Tipo = "PUT";
            sendMsg.Corpo = key+value;
            enviaMensagem(sendMsg);
            //RECEBE PUT_OK com um timestamp

						break;

		    	case "GET":
				    System.out.println("Funcionalidade GET selecionada\nInsira a key a ser procurada:\n");
            // ENVIA GET PRA UM SERVIDOR ALEATÓRIO
            
            key = sc.nextLine();
            // ENVIA o último timestamp que conhecemos do arquivo
            sendMsg = new Message();
            sendMsg.Tipo = "GET";
            sendMsg.Corpo = key;
            enviaMensagem(sendMsg);

						break;
		    		
		    	default:
		    		System.out.println("Funcionalidade invalida");
		    		break;
		    }
	    }
	}

}