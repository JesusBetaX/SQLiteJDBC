package sqlitejdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class SQLiteOpenHelper {

  private File folder = new File("databases");
  private final String mName;
  private final int mNewVersion;

  private Connection mDatabase;
  private boolean mIsInitializing;

  public SQLiteOpenHelper(String databaseFilePath, int version) {
    mName = databaseFilePath;
    mNewVersion = version;
  }

  public File getFolder() {
    return folder;
  }
  public void setFolder(File folder) {
    this.folder = folder;
  }
  
  public File getDatabasePath(String name) {
    if (!folder.exists()) {
      folder.mkdirs();
    }
    return new File(folder, name);
  }

  public Connection getWritableDatabase() throws SQLException {
    synchronized (this) {
      return getDatabaseLocked(true);
    }
  }

  public Connection getReadableDatabase() throws SQLException {
    synchronized (this) {
      return getDatabaseLocked(false);
    }
  }

  /**
   * Connect to a sample database Si la base de datso no exesite la crea.
   *
   * Connection conn = DriverManager .getConnection("jdbc:sqlite:" +
   * databaseFilePath);
   *
   * @return
   * @throws java.sql.SQLException
   */
  private Connection getDatabaseLocked(boolean writable) throws SQLException {
    if (mDatabase != null) {
      if (mDatabase.isClosed()) {
        // Darn!  The user closed the database by calling mDatabase.close().
        mDatabase = null;
      } else if (!writable || !mDatabase.isReadOnly()) {
        // The database is already open for business.
        return mDatabase;
      }
    }

    if (mIsInitializing) {
      throw new IllegalStateException("getDatabase called recursively");
    }

    Connection db = mDatabase;
    try {
      mIsInitializing = true;

      if (db != null) {
        if (writable && db.isReadOnly()) {
          db.setReadOnly(false);
        }
      } else {
        final String path = getDatabasePath(mName).getPath();
        try {
          db = DriverManager.getConnection("jdbc:sqlite:" + path);
        } catch (SQLException ex) {
          if (writable) {
            throw ex;
          }
          db = DriverManager.getConnection("jdbc:sqlite:" + path);
        }
      }

      onConfigure(db);

      final int version = getVersion(db);
      db.setAutoCommit(false); //beginTransaction();
      try {
        if (version == 0) {
          onCreate(db);
        } else {
          if (version > mNewVersion) {
            onDowngrade(db, version, mNewVersion);
          } else {
            onUpgrade(db, version, mNewVersion);
          }
        }
        setVersion(db, mNewVersion); 
        db.commit(); //setTransactionSuccessful();
      } catch (SQLException e) {
        db.rollback();
      } finally {
        db.setAutoCommit(true);//endTransaction();
      }

      onOpen(db);

      if (db.isReadOnly()) {
        System.out.println("Opened " + mName + " in read-only mode");
      }

      mDatabase = db;
      return db;
    } finally {
      mIsInitializing = false;
      if (db != null && db != mDatabase) {
        db.close();
      }
    }
  }

  /**
   * Close any open database object.
   */
  public synchronized void close() {
    try {
      if (mIsInitializing) throw new IllegalStateException("Closed during initialization");

      if (mDatabase != null && !mDatabase.isClosed()) {
        mDatabase.close();
        mDatabase = null;
      }
    } catch (SQLException ignore) {
      // Empty
    }
  }
  
  // user_version
  private int getVersion(Connection db) throws SQLException {
    // SQL statement for creating a new table
    String sql = "CREATE TABLE IF NOT EXISTS PRAGMA (\n"
            + "	id integer PRIMARY KEY,\n"
            + "	user_version integer NOT NULL\n"
            + ");";
    db.createStatement().execute(sql);
    
    sql = "SELECT user_version FROM PRAGMA LIMIT 1";
    try (Statement stmt = db.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
      if (rs.next()) {
        return rs.getInt("user_version");
      }
    }
    
    return 0;
  }
  
  // user_version = " + version
  private void setVersion(Connection db, int mNewVersion) throws SQLException {
    String sql = "SELECT user_version FROM PRAGMA LIMIT 1";
    try (Statement stmt = db.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {

      if (rs.next()) {
        sql = "UPDATE PRAGMA SET user_version = ?";
        try (PreparedStatement pstmt = db.prepareStatement(sql)) {
          pstmt.setInt(1, mNewVersion);
          pstmt.executeUpdate();
        }
      } else {
        sql = "INSERT INTO PRAGMA(user_version) VALUES(?)";
        try (PreparedStatement pstmt = db.prepareStatement(sql)) {
          pstmt.setInt(1, mNewVersion);
          pstmt.executeUpdate();
        }
      }
    }
  }

  public void onConfigure(Connection db) throws SQLException {}

  public abstract void onCreate(Connection db) throws SQLException;

  public void onOpen(Connection db) throws SQLException {}

  public void onDowngrade(Connection db, int version, int mNewVersion) 
  throws SQLException{}

  public void onUpgrade(Connection db, int version, int mNewVersion) 
  throws SQLException {}
}
