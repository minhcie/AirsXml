package src.main.java;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.log4j.Logger;

/*
USE Analyst
GO

DROP TABLE dbo.Taxonomy
GO

CREATE TABLE dbo.Taxonomy
   (ID int PRIMARY KEY NOT NULL,
    Code varchar(64) NOT NULL,
	Name varchar(128) NOT NULL,
	CodeDefinition varchar(MAX),
    CreatedDate date,
	LastModifiedDate date,
	ImportedDate date NOT NULL default CURRENT_TIMESTAMP)
GO

CREATE INDEX Taxonomy_Code_Index ON dbo.Taxonomy (Code)
GO
*/
public class DbTaxonomy {
    private static final Logger log = Logger.getLogger(DbTaxonomy.class.getName());

    public long id = 0;
    public String code;
    public String name;
    public String definition;
    public Date created;
    public Date lastModified;

    public static DbTaxonomy findByCode(Connection conn, String code) {
        DbTaxonomy result = null;
        try {
            StringBuffer sb = new StringBuffer();
            sb.append("SELECT ID, Code, Name, CodeDefinition, CreatedDate, ");
            sb.append("LastModifiedDate, ImportedDate ");
            sb.append("FROM dbo.Taxonomy ");
            sb.append("WHERE Code = '" + code + "'");

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sb.toString());
            if (rs.next()) {
                result = new DbTaxonomy();
                result.id = rs.getLong("ID");
                result.code = rs.getString("Code");
                result.name = rs.getString("Name");
                result.definition = rs.getString("CodeDefinition");
                result.created = rs.getDate("CreatedDate");
                result.lastModified = rs.getDate("LastModifiedDate");
            }

            rs.close();
            stmt.close();
        }
        catch (SQLException sqle) {
             log.error("SQLException in DbTaxonomy.findByCode(): " + sqle);
        }
         catch (Exception e) {
             log.error("Exception in DbTaxonomy.findByCode(): " + e);
        }
        return result;
    }
}
