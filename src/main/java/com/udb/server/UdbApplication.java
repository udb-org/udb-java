package com.udb.server;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/**
 * The main class of the application.
 * It sets the port to 8080 if no port is specified.
 * @author Udb
 * @version 1.0
 * @since 1.0
 */
@SpringBootApplication
public class UdbApplication {
	public static void main(String[] args) {
		//set the port to 8080 if no port is specified
		if(args.length > 0){
			String port = args[0];
			System.setProperty("server.port", port);
		}
		SpringApplication.run(UdbApplication.class, args);
	}
}
