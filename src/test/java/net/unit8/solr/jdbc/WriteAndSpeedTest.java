package net.unit8.solr.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WriteAndSpeedTest {
	Connection conn;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCHSS() throws SQLException {
		System.out.println("Test CHSS");
		rebase();
		conn = DriverManager.getConnection("jdbc:solr:chss:http://localhost:8983/solr/collection1");
		runTest();
		conn.close();
	}
	
	@Test
	public void testCUSS() throws SQLException {
		System.out.println("Test CUSS");
		rebase();
		conn = DriverManager.getConnection("jdbc:solr:cuss:http://localhost:8983/solr/collection1?queueSize=500&threadCount=10");
		runTest();
		conn.close();

	}
	
	@Test
	public void testLBSS() throws SQLException {
		System.out.println("Test LBSS");
		rebase();
		conn = DriverManager.getConnection("jdbc:solr:lbss:http://localhost:8983/solr/collection1,http://localhost:8983/solr/collection1");
		runTest();
		conn.close();

	}
	
	public void rebase() throws SQLException {
		Connection setUpConn = DriverManager.getConnection("jdbc:solr:http://localhost:8983/solr/collection1");
		try {
			PreparedStatement dropStmt = setUpConn.prepareStatement("DROP TABLE cusstest1");

			try {
				dropStmt.executeUpdate();
			} catch(SQLException ignore) {
				ignore.printStackTrace();
			} finally {
				dropStmt.close();
			}

			PreparedStatement stmt = setUpConn.prepareStatement(
					"CREATE TABLE cusstest1 (player_id number PRIMARY, team varchar(10) PRIMARY, "
					+ " player_name varchar(50) PRIMARY, position varchar(10) ARRAY, "
					+ " was_homerun_king boolean, comment TEXT,"
					+ " registered_at DATE)");
			try {
				stmt.executeUpdate();
			} finally {
				stmt.close();
			}
			setUpConn.commit();
		} finally {
			setUpConn.close();
		}
	}
	
	public void runTest() throws SQLException {
		System.out.println(" Uploading 10000 documents....");
		long start = System.currentTimeMillis();
		PreparedStatement insStmt = conn.prepareStatement("INSERT INTO cusstest1 Values (?,?,?,?,?,?,?)");
		try {
			
			//Random idGen = new Random();
			int id = 0;
			long team = System.currentTimeMillis();
			int i = 0;
			while(i++ < 1000) {
				id = i;
				insStmt.setInt(1, id);
				insStmt.setString(2, String.valueOf(team));
				insStmt.setString(3, ("Player Name: " + id));
				insStmt.setObject(4, new String[]{String.valueOf(team), String.valueOf(id)});
				insStmt.setBoolean(5, (id%2 == 0));
				insStmt.setString(6, ("Comment : " + id +" : " + String.valueOf(team) + (" : Player Name : " + id) + " : "+ (id%2 == 0)));
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();
			}
		
		} finally {
			insStmt.close();
		}
		System.out.println(" Upload: Time Taken for 10000 documents ms : " + (System.currentTimeMillis() - start));
		conn.commit();
		System.out.println(" Commit: Time Taken for 10000 documents ms : " + (System.currentTimeMillis() - start));

	}

	@BeforeClass
	public static void init() throws SQLException, ClassNotFoundException {
		Class.forName(SolrDriver.class.getName());
	}
}
