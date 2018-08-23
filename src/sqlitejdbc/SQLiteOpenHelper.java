package sqlitejdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.sqlite.SQLiteConfig;

public abstract class SQLiteOpenHelper {

  private final File mFolder;
  private final String mName;
  private final int mNewVersion;

  private Connection mDatabase;
  private boolean mIsInitializing;
  
  public SQLiteOpenHelper(String name, int version) {
    this(new File("databases"), name, version);
  }
  public SQLiteOpenHelper(File databaseFilePath, String name, int version) {
    mFolder = databaseFilePath;
    mName = name;
    mNewVersion = version;
  }

  public File getDatabasePath() {
    return mFolder;
  }
  public File getDatabasePath(String name) {
    return new File(getDatabasePath(), name);
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
        // ¡Maldición! El usuario cerró la base de datos llamando a mDatabase.close ().
        mDatabase = null;
      } else if (!writable || !mDatabase.isReadOnly()) {
        // La base de datos ya está abierta para los negocios.
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
        final File path = getDatabasePath(mName);
        try {
          db = openDatabase(path, writable);
        } catch (SQLException ex) {
          db = openOrCreateDatabase(path);
        }
      }

      onConfigure(db);

      final int version = getVersion(db);
      if (version != mNewVersion) {
        if (db.isReadOnly()) {
          throw new SQLException("Can't upgrade read-only database from version "
                  + version + " to " + mNewVersion + ": " + mName);
        }
        
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
  
  private Connection openDatabase(File path, boolean writable) throws SQLException {
    SQLiteConfig config = new SQLiteConfig();
    config.setReadOnly(!writable);
    return config.createConnection("jdbc:sqlite:" + path.getPath());
  }
  
  private Connection openOrCreateDatabase(File path) throws SQLException {
    File parent = path.getParentFile();
    if (parent != null && !parent.exists()) {
      parent.mkdirs();
    }
    
    SQLiteConfig config = new SQLiteConfig();
    Connection conn = config.createConnection("jdbc:sqlite:" + path.getPath());
    
    String sql = "CREATE TABLE IF NOT EXISTS schema (\n"
            + "	user_version integer NOT NULL\n"
            + ");";
    conn.createStatement().execute(sql);
    
    return conn;
  }
  
  // user_version
  private int getVersion(Connection db) throws SQLException {
    String sql = "SELECT user_version FROM schema LIMIT 1";
    try (Statement stmt = db.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
      return rs.next() ? rs.getInt("user_version") : 0;
    }
  }
  
  // user_version = " + version
  private void setVersion(Connection db, int mNewVersion) throws SQLException {
    String sql = "SELECT user_version FROM schema LIMIT 1";
    try (Statement stmt = db.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {

      if (rs.next()) {
        sql = "UPDATE schema SET user_version = ?";
      } else {
        sql = "INSERT INTO schema(user_version) VALUES(?)";
      }
      try (PreparedStatement pstmt = db.prepareStatement(sql)) {
        pstmt.setInt(1, mNewVersion);
        pstmt.executeUpdate();
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
  
  public void onConfigure(Connection db) throws SQLException {}

  public abstract void onCreate(Connection db) throws SQLException;

  public void onOpen(Connection db) throws SQLException {}

  public void onDowngrade(Connection db, int version, int mNewVersion) 
  throws SQLException{}

  public void onUpgrade(Connection db, int version, int mNewVersion) 
  throws SQLException {}
}
