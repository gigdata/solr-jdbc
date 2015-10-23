package net.unit8.solr.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.unit8.solr.jdbc.message.ErrorCode;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

//jdbc:solr:http://diapp-nprd1-101:8094/solr/bre
public class PrimaryKeyTest {

	Connection conn;

	@Before
	public void setUp() throws Exception {
		conn = DriverManager.getConnection("jdbc:solr:http://diapp-nprd1-101:8094/solr/bre");
	}

	@After
	public void tearDown() throws Exception {
		conn.close();
	}

	@Test
	public void testStatement() throws SQLException {
		// bob tim john bala jim
		String[][] expected = { { "bob" }, { "tim" }, { "john" }, { "bala" }, { "jim" } };
		verifyStatement("SELECT player_name FROM player ORDER BY player_id", expected);
	}

	@Test
	public void testStatementLimit() throws SQLException {
		String[][] expected = { { "tim" }, { "john" }, { "bala" } };
		verifyStatement("SELECT player_name FROM player ORDER BY player_id LIMIT 3 OFFSET 1", expected);
	}

	@Test
	public void testStatementOrderBy() throws SQLException {
		Object[][] expected = { { "jim" }, { "bala" }, { "john" }, { "tim" }, { "bob" } };
		verifyStatement("SELECT player_name FROM player ORDER BY player_id DESC", expected);
	}

	@Test
	public void testStatementCondition() throws SQLException {
		Object[][] expected1 = { { "tim" } };
		verifyStatement("SELECT player_name FROM player WHERE player_id > 1 AND player_id < 3", expected1);
		Object[][] expected2 = { { "bob" }, { "tim" }, { "john" } };
		verifyStatement("SELECT player_name FROM player WHERE player_id >= 1 AND player_id <= 3", expected2);
	}

	@Test
	public void testStatementOr() throws SQLException {
		Object[][] expected1 = { { "jim" } };
		Object[] params = { "blue team" };
		verifyPreparedStatement("SELECT player_name FROM player WHERE (player_id = 1 OR player_id = 5) AND team=?", params, expected1);
	}

	@Test
	public void testStatementCount() throws SQLException {
		Object[][] expected = { { "5" } };
		verifyStatement("SELECT count(*) FROM player", expected);
	}

	@Test
	public void testStatementGroupBy() throws SQLException {
		Object[][] expected = { { "red team", "4" }, { "blue team", "1" } };
		verifyStatement("SELECT team, count(*) FROM player GROUP BY team", expected);
	}

	@Test
	public void testStatementIn() throws SQLException {
		Object[][] expected = { { "tim" }, { "john" }, { "jim" } };
		Object[] params = { "pos3", "pos2" };
		verifyPreparedStatement("SELECT player_name FROM player WHERE position in (?,?) order by player_id", params, expected);
	}

	@Test
	public void testLike() throws SQLException {
		Object[][] expected = { { "bala" } };
		Object[] params = { "ba%" };
		verifyPreparedStatement("SELECT player_name FROM player WHERE player_name like ?", params, expected);
	}

	@Test
	public void testLikeForText() throws SQLException {
		Object[][] expected = { { "bob" } };
		Object[] params = { "%ob%" };
		verifyPreparedStatement("SELECT player_name FROM player WHERE comment like ?", params, expected);
	}

	@Test
	public void testStartsForText() throws SQLException {
		Object[][] expected = { { "bob" } };
		Object[] params = { "bo%" };
		verifyPreparedStatement("SELECT player_name FROM player WHERE comment like ?", params, expected);
	}

	@Test
	public void testBetween() throws SQLException {
		Object[][] expected = { { "tim" }, { "john" } };
		Object[] params = { 2, 3 };
		verifyPreparedStatement("SELECT player_name FROM player WHERE player_id BETWEEN ? AND ?", params, expected);
	}

	@Test
	public void testBoolean() throws SQLException {
		Object[][] expected = { { "bala" }, { "jim" } };
		Object[] params = { true };
		verifyPreparedStatement("SELECT player_name FROM player WHERE was_homerun_king=?", params, expected);
	}

	@Test
	public void testQueryContainsMetachar() throws SQLException {
		Object[][] expected = {};
		Object[] params = { ";&?" };

		verifyPreparedStatement("SELECT player_name FROM player WHERE player_name=?", params, expected);
	}

	@Test
	public void testStatementTableNotFound() {
		try {
			conn.prepareStatement("select * from prayer");
			fail("No Exception");
		} catch (SQLException e) {
			assertEquals("TableOrViewNotFound", ErrorCode.TABLE_OR_VIEW_NOT_FOUND, e.getErrorCode());
		}

	}

	@Test
	public void testStatementColumnNotFound() throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("select prayer_name from player");
			fail("No Exception");
		} catch (SQLException e) {
			assertEquals("ColumnNotFound", ErrorCode.COLUMN_NOT_FOUND, e.getErrorCode());
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}

	/**
	 * get resultSet by column name
	 */
	@Test
	public void testGetColumnLabel() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("Select player_name from player where player_id=3");
			assertTrue(rs.next());
			assertEquals("player_name", rs.getMetaData().getColumnLabel(1));
			assertEquals("john", rs.getString("player_name"));
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}

	@Test
	public void testUpdateCompositeKeyValue() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			// TODO fix this test. The following code will throw an SQLException, but the catch clause is not catching it.
			// stmt.executeUpdate("UPDATE player SET team='orange team' where player_id=5");
			// fail("No Exception");
		} catch (SQLException e) {
			assertEquals("SyntaxError", ErrorCode.SYNTAX_ERROR, e.getErrorCode());
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}

	@Test
	public void testUpdateComposite() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			int updateCount = stmt.executeUpdate("UPDATE player SET comment='updated comment text' where player_id=5");
			assertEquals(updateCount, 1);
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}

	@Test
	public void testAddColumn() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			int returnCode = stmt.executeUpdate("ALTER TABLE player ADD COLUMN mycolumn varchar (255)");
			assertEquals(0, returnCode);
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}

	@Test
	public void testDropColumn() throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			int returnCode = stmt.executeUpdate("ALTER TABLE player DROP COLUMN comment varchar (255)");
			assertEquals(0, returnCode);
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}

	private void verifyStatement(String selectQuery, Object[][] expected) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			int i = 0;
			while (rs.next()) {
				for (int j = 0; j < expected[i].length; j++) {
					assertEquals(expected[i][j], rs.getString(j + 1));
				}
				i += 1;
			}
			assertEquals("Length check", expected.length, i);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("SQLException:" + e.getMessage());
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	private void verifyPreparedStatement(String selectQuery, Object[] params, Object[][] expected) throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(selectQuery);
			for (int i = 0; i < params.length; i++) {
				stmt.setObject(i + 1, params[i]);
			}
			ResultSet rs = stmt.executeQuery();
			int i = 0;
			while (rs.next()) {
				for (int j = 0; j < expected[i].length; j++) {
					assertEquals(expected[i][j], rs.getString(j + 1));
				}
				i += 1;
			}
			assertEquals("Length check", expected.length, i);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("SQLException:" + e.getMessage());
		} finally {
			if (stmt != null)
				stmt.close();
		}

	}

	@BeforeClass
	public static void init() throws SQLException, ClassNotFoundException {
		Class.forName(SolrDriver.class.getName());
		Connection setUpConn = DriverManager.getConnection("jdbc:solr:http://diapp-nprd1-101:8094/solr/bre");

		try {
			// http://diapp-nprd1-101:8094/solr/bre/update?stream.body=%3Cdelete%3E%3Cquery%3E*:*%3C/query%3E%3C/delete%3E
			PreparedStatement dropStmt = setUpConn.prepareStatement("DROP TABLE player");
			try {
				dropStmt.executeUpdate();
			} catch (SQLException ignore) {
				ignore.printStackTrace();
			} finally {
				dropStmt.close();
			}

			// CREATE TABLE player (player_id number DISP KEY, team varchar(10) DISP KEY, player_name varchar(50), position varchar(10) ARRAY, was_homerun_king boolean, comment
			// TEXT,registered_at DATE)
			PreparedStatement stmt = setUpConn.prepareStatement("CREATE TABLE player (player_id number DISP KEY, team varchar(10) DISP KEY, "
					+ " player_name varchar(50), position varchar(10) ARRAY, " + " was_homerun_king boolean, comment TEXT," + " registered_at DATE) DATASTORE = TESTSTORE");
			try {
				stmt.executeUpdate();
			} finally {
				stmt.close();
			}

			PreparedStatement insStmt = setUpConn.prepareStatement("INSERT INTO player Values (?,?,?,?,?,?,?)");
			try {
				insStmt.setInt(1, 1);
				insStmt.setString(2, "red team");
				insStmt.setString(3, "bob");
				insStmt.setObject(4, new String[] { "pos1" });
				insStmt.setBoolean(5, false);
				insStmt.setString(6, "bob comment text");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();

				insStmt.setInt(1, 2);
				insStmt.setString(2, "red team");
				insStmt.setString(3, "tim");
				insStmt.setObject(4, new String[] { "pos1", "pos2" });
				insStmt.setBoolean(5, false);
				insStmt.setString(6, "comment text");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();

				insStmt.setInt(1, 3);
				insStmt.setString(2, "red team");
				insStmt.setString(3, "john");
				insStmt.setObject(4, new String[] { "pos3", "pos5" });
				insStmt.setBoolean(5, false);
				insStmt.setString(6, "comment text");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();

				insStmt.setInt(1, 4);
				insStmt.setString(2, "red team");
				insStmt.setString(3, "bala");
				insStmt.setObject(4, new String[] { "pos4" });
				insStmt.setBoolean(5, true);
				insStmt.setString(6, "comment text");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();

				insStmt.setInt(1, 5);
				insStmt.setString(2, "blue team");
				insStmt.setString(3, "jim");
				insStmt.setObject(4, new String[] { "pos3", "pos4" });
				insStmt.setBoolean(5, true);
				insStmt.setString(6, "comment text");
				insStmt.setDate(7, new Date(System.currentTimeMillis()));
				insStmt.executeUpdate();
			} finally {
				insStmt.close();
			}

			setUpConn.commit();
		} finally {
			setUpConn.close();
		}
	}
}
