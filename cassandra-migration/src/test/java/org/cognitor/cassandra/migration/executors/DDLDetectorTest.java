package org.cognitor.cassandra.migration.executors;

import org.cognitor.cassandra.migration.executors.DDLRecogniser.CQLDescription;
import org.cognitor.cassandra.migration.executors.DDLRecogniser.CQLDescription.DDLType;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Patrick Kranz
 */
public class DDLDetectorTest {

    private String[][] validKeyspaceDDLs = new String[][] {
            {"CREATE KEYSPACE IF NOT EXISTS testKeyspace WITH REPLICATION = {\n'class' : 'SimpleStrategy', \n 'replication_factor' : 1 \n  }", "testKeyspace", "CREATE"},
            {"CREATE KEYSPACE testKeyspace WITH REPLICATION = {\n'class' : 'SimpleStrategy', \n 'replication_factor' : 1 \n  }", "testKeyspace", "CREATE"},
            {"create keyspace \"test_Keyspace_1\" with replication = {\n'class' : 'SimpleStrategy', \n 'replication_factor' : 1 \n  }", "test_Keyspace_1", "CREATE"},
            {"create \t  \nkeyspace \t  \n \"test_Keyspace_1\"  \t  \nwith \t  \n replication = {\n'class' : 'SimpleStrategy', \n 'replication_factor' : 1 \n  }", "test_Keyspace_1", "CREATE"}
    };

    private String[][] validTableDDLs = new String[][]{
            { "CREATE TABLE my_table (id int, division text, PRIMARY KEY (id,division))",  "my_table", "CREATE" },
            { "CREATE TABLE IF NOT EXISTS my_table (id int, division text, PRIMARY KEY (id,division))",  "my_table", "CREATE" },
            { "create table \"my_table\" (id int, division text, PRIMARY KEY (id,division))",  "my_table", "CREATE" },
            { "ALTER TABLE employees_tbl ADD (first_name text);",  "employees_tbl", "ALTER" },
            { "ALTER TABLE mytable WITH custom_properties={'point_in_time_recovery': {'status': 'enabled'}}",  "mytable", "ALTER" },
            { "RESTORE TABLE mytable_restored from table mykeyspace.my_table WITH restore_timestamp = '2020-06-30T04:05:00+0000'",  "mytable_restored", "RESTORE" },
            { "DROP TABLE IF NOT EXISTS table_name",  "table_name", "DROP" }
    };

    private String[] nonDDLStatements = new String[] {
            "INSERT INTO AUTH_USER (email, userId, passwordHash, passwordSalt) VALUES ('tibi@localhost', 18024703-a8f0-40fa-b963-3f9a429d0909, '133137649eff792ed95bf406ab3e413a81fa6a42fb05f658eb38f5cc8299a85997561ddc8f', '3339fee7b51105f8');",
            "UPDATE cyclists SET firstname = 'Marianne', lastname = 'VOS' WHERE id = 88b8fd18-b1ed-4e96-bf79-4280797cba80"
    };

    @Test
    public void shouldDetectKeyspaceOperationAndReturnKeyspaceName() {
        for(String[] ddl : validKeyspaceDDLs){
            DDLRecogniser recogniser = new DDLRecogniser();
            CQLDescription result = recogniser.evaluate(ddl[0]);
            assertTrue(result.isAsyncDDL());
            assertTrue(result.isKeyspaceDDL());
            assertEquals(ddl[1], result.getResourceName());
            assertEquals(DDLType.valueOf(ddl[2]), result.getDdlType());
        }
    }

    @Test
    public void shouldDetectTableOperationAndReturnKeyspaceName() {
        for(String[] ddl : validTableDDLs){
            DDLRecogniser recogniser = new DDLRecogniser();
            CQLDescription result = recogniser.evaluate(ddl[0]);
            assertTrue(result.isAsyncDDL());
            assertTrue(result.isTableDDL());
            assertEquals(ddl[1], result.getResourceName());
            assertEquals(DDLType.valueOf(ddl[2]), result.getDdlType());
        }
    }

    @Test
    public void shouldDetectNonDDLStatements() {
        for(String statement : nonDDLStatements){
            DDLRecogniser recogniser = new DDLRecogniser();
            CQLDescription result = recogniser.evaluate(statement);
            assertTrue(result.isNotAsyncDDL());
        }
    }

}