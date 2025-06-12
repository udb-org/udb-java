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
	/**
	 * 
	 *  java -cp "udb-java-0.0.2:udb-java-0.0.2/BOOT-INF/classes:udb-java-0.0.2/BOOT-INF/lib/*:/Users/taoyongwen/.udb/server/driver/com/mysql/mysql-connector-j/9.3.0/mysql-connector-j-9.3.0.jar"
	 *  com.udb.server.UdbApplication 
	 *   10001 "mysql(2,3)"
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length!=2){
			return ;
		}
		String port = args[0];
		System.setProperty("server.port", port);
		String driver=args[1];
		SpringApplication.run(UdbApplication.class, args);
	}
}
