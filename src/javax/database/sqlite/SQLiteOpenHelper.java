package javax.database.sqlite;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.sqlite.SQLiteConfig;

public abstract class SQLiteOpenHelper {

  private final File mFolder;
  private final String mName;
  private final int mNewVersion;

  private SQLiteDatabase mDatabase;
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

  public SQLiteDatabase getWritableDatabase() throws SQLException {
    synchronized (this) {
      return getDatabaseLocked(true);
    }
  }

  public SQLiteDatabase getReadableDatabase() throws SQLException {
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
  private SQLiteDatabase getDatabaseLocked(boolean writable) throws SQLException {
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

    SQLiteDatabase db = mDatabase;
    try {
      mIsInitializing = true;

      final File path = getDatabasePath(mName);
      if (db != null) {
        if (writable && db.isReadOnly()) {
          db = reopenReadWrite(path, db);
        }
      } else {
        try {
          db = openOrCreateDatabase(path, writable);
        } catch (SQLException ex) {
          db = openOrCreateDatabase(path, true);
        }
      }

      onConfigure(db);

      final int version = getVersion(db);
      if (version != mNewVersion) {
        if (db.isReadOnly()) {
          throw new SQLException("Can't upgrade read-only database from version "
                  + version + " to " + mNewVersion + ": " + mName);
        }
        
        db.beginTransaction();
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
          db.setTransactionSuccessful();
        } catch (SQLException e) {
          db.rollback();
        } finally {
          db.endTransaction();
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
  
  private SQLiteDatabase reopenReadWrite(File path, SQLiteDatabase db) throws SQLException {
    synchronized(this) {
      if (!db.isReadOnly()) {
        return db; // nothing to do
      }
      db.close();
      return openOrCreateDatabase(path, true);
    }
  }
  

  private SQLiteDatabase openOrCreateDatabase(File path, boolean writable) throws SQLException {
    File parent = path.getParentFile();
    if (parent != null && !parent.exists()) {
      parent.mkdirs();
    }
    
    boolean existsDb = path.exists();
    
    SQLiteConfig config = new SQLiteConfig();
    config.setReadOnly(!writable);
    Connection conn = config.createConnection("jdbc:sqlite:" + path.getPath());
    SQLiteDatabase db = new SQLiteDatabase(conn);
    
    if (!existsDb) {
      createSchema(db);
    }
    
    return db;
  }
  
  protected void createSchema(SQLiteDatabase db) throws SQLException {
    String sql = "CREATE TABLE IF NOT EXISTS schema (\n"
            + "	user_version INTEGER NOT NULL,\n"
            + "	created_at INTEGER,\n"
            + "	updated_at INTEGER\n"
            + ");";
    db.execSQL(sql);
  }
 
  // user_version = " + version
  protected void setVersion(SQLiteDatabase db, int newVersion) throws SQLException {
    String sql = "SELECT user_version FROM schema LIMIT 1";
    try (/*Statement stmt = db.createStatement();*/
            ResultSet rs = db.query(sql)) {

      if (rs.next()) {
        sql = "UPDATE schema SET user_version = ?, updated_at = ?";
        try (PreparedStatement pstmt = db.compileStatement(sql)) {
          pstmt.setInt(1, newVersion);
          pstmt.setLong(2, System.currentTimeMillis());
          pstmt.executeUpdate();
        }
      } else {
        sql = "INSERT INTO schema(user_version, created_at) VALUES(?,?)";
        try (PreparedStatement pstmt = db.compileStatement(sql)) {
          pstmt.setInt(1, newVersion);
          pstmt.setLong(2, System.currentTimeMillis());
          pstmt.executeUpdate();
        }
      }
    }
  }
  
  // user_version
  protected int getVersion(SQLiteDatabase db) throws SQLException {
    String sql = "SELECT user_version FROM schema LIMIT 1";
    try (/*Statement stmt = db.createStatement();*/
            ResultSet rs = db.query(sql)) {
      return rs.next() ? rs.getInt("user_version") : 0;
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
  
  public void onConfigure(SQLiteDatabase db) throws SQLException {}

  public abstract void onCreate(SQLiteDatabase db) throws SQLException;

  public void onOpen(SQLiteDatabase db) throws SQLException {}

  public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) 
  throws SQLException{}

  public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) 
  throws SQLException {}
}
