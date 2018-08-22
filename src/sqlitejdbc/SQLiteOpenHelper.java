package sqlitejdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class SQLiteOpenHelper {

  private final String mName;

  private Connection mDatabase;
  private boolean mIsInitializing;

  public SQLiteOpenHelper(String databaseFilePath) {
    mName = databaseFilePath;
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
      } else  {
         try {
          db = DriverManager.getConnection("jdbc:sqlite:" + mName);
        } catch (SQLException ex) {
          if (writable) {
            throw ex;
          }
          db = DriverManager.getConnection("jdbc:sqlite:" + mName);
        }
      }

      onConfigure(db);

      db.setAutoCommit(false); //beginTransaction();
      try {
        onCreate(db);
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
      if (mIsInitializing)
        throw new IllegalStateException("Closed during initialization");

      if (mDatabase != null && !mDatabase.isClosed()) {
        mDatabase.close();
        mDatabase = null;
      }
    } catch (SQLException ignore) {
      // Empty
    }
  }

  public void onConfigure(Connection db) throws SQLException {
  }

  public abstract void onCreate(Connection db) throws SQLException;

  public void onOpen(Connection db) throws SQLException {
  }

}
