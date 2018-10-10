package javax.database.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;

public class SQLiteDatabase implements AutoCloseable {
  private static final String TAG = "SQLiteDatabase";

  private final Connection conn;
  
  public SQLiteDatabase(Connection conn) {
    this.conn = conn;
  }
  
  public Connection connection() {
    return conn;
  }

  @Override protected void finalize() throws Throwable {
    try {
      close();
    } finally {
      super.finalize();
    }
  }

  @Override public void close() {
    synchronized (this) {
      closeQuietly(conn);
    }
  }

  public static void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) return;
    try {
      closeable.close();
    } catch (Exception ignore) {
      // empty
    }
  }

  /**
   * Prepara sentencias sql.
   *
   * @param sql instruccion a preparar
   *
   * @return PreparedStatement setencia preparada
   *
   * @throws SQLException
   */
  public PreparedStatement compileStatement(String sql) throws SQLException {
    return conn.prepareStatement(sql);
  }
  public Statement createStatement() throws SQLException {
    return conn.createStatement();
  }

  private static void prepareBind(PreparedStatement ps, Object... bindArgs)
          throws SQLException {
    if (bindArgs == null) return;
    for (int i = 0; i < bindArgs.length; i++) {
      ps.setObject(i + 1, bindArgs[i]);
    }
  }
  
  public ResultSet query(String sql) throws SQLException {
    Statement statement = null;
    try {
      statement = conn.createStatement(/*ResultSet.TYPE_FORWARD_ONLY, 
              ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT*/);
      ResultSet resultSet = statement.executeQuery(sql);
      Log.i(TAG, sql);
      return new SQLiteResultSet(resultSet, statement);
    } catch (SQLException e) {
      closeQuietly(statement);
      throw e;
    }
  }
  public ResultSet query(String sql, Object... bindArgs) throws SQLException {
    PreparedStatement statement = null;
    try {
      statement = conn.prepareStatement(sql/*, ResultSet.TYPE_FORWARD_ONLY, 
              ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT*/);
      prepareBind(statement, bindArgs);
      ResultSet resultSet = statement.executeQuery();
      Log.i(TAG, sql + " " + Arrays.toString(bindArgs));
      return new SQLiteResultSet(resultSet, statement);
    } catch (SQLException e) {
      closeQuietly(statement);
      throw e;
    }
  }

  public void execSQL(String sql) throws SQLException {
    try (Statement statement = createStatement()) {
      statement.execute(sql);
    }
  }
  public void execSQL(String sql, Object... bindArgs) throws SQLException {
    try (PreparedStatement statement = compileStatement(sql)) {
      prepareBind(statement, bindArgs);
      statement.execute();
    }
  }

  /**
   * Ejecuta una sentencia que modifique las filas de la base de datos.
   * 
   * @param sql sentencia update o delete.
   * @param bindArgs valores de la sentencia: <code>nombre=?</code>.
   * @return el número de filas afectadas.
   * @throws SQLException 
   */
  public int executeUpdate(String sql, Object... bindArgs) throws SQLException {
    try (PreparedStatement statement = compileStatement(sql)) {
      prepareBind(statement, bindArgs);
      int rows = statement.executeUpdate();
      Log.i(TAG, sql + " " + Arrays.toString(bindArgs));
      return rows;
    }
  }

  /**
   * Inserta un registro en la base de datos y obtiene el id. 
   * 
   * @param sql sentencia insert: <code>INSERT INTO [table] VALUES(?)</code>
   * @param bindArgs valores de la sentencia: <code> VALUES(?)</code>.
   * @return id del registo insertado o: 
   *         -1 : si no se executo la sentencia.
   *          0 : si no se genero el id al isertar el registro.
   * @throws SQLException 
   */
  public long insertAndGetId(String sql, Object... bindArgs)
          throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
      prepareBind(ps, bindArgs);
      if (ps.executeUpdate() > 0) {
        Log.i(TAG, sql + " " + Arrays.toString(bindArgs));
        // obtengo las ultimas llaves generadas
        try (ResultSet rs = ps.getGeneratedKeys()) {
          // retorna la llave.
          return rs.next() ? rs.getLong(1) : 0;
        }
      } else {
        return -1;
      }
    }
  }
  
  /**
   * Inserta un registro en la base de datos.
   *
   * @param table nombre de la tabla donde se va a insertar la fila
   * @param initialValues contiene los valores de columna iniciales para la fila.
   * Las claves deben ser los nombres de las columnas y los valores valores de
   * la columna
   * @param conflictAlgorithm  OR ROLLBACK, OR ABORT, OR FAIL, OR IGNORE, OR REPLACE
   *
   * @return el ID de la fila recién insertada, o -1 si se produjo un error
   *
   * @throws SQLException
   */
  public long insertWithOnConflict(String table, Map<String, Object> initialValues, 
          String conflictAlgorithm) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT ");
    sql.append(conflictAlgorithm);
    sql.append(" INTO ");
    sql.append(table);
    sql.append('(');

    int size = initialValues.size();
    Object[] bindArgs = new Object[size];
    int i = 0;
    for (String colName : initialValues.keySet()) {
      sql.append((i > 0) ? "," : "");
      sql.append(colName);
      bindArgs[i++] = initialValues.get(colName);
    }
    sql.append(')');
    sql.append(" VALUES (");
    for (i = 0; i < size; i++) {
      sql.append((i > 0) ? ",?" : "?");
    }
    sql.append(')');

    return insertAndGetId(sql.toString(), bindArgs);
  }
  
  public long insert(String table, Map<String, Object> values) {
    try {
      return insertWithOnConflict(table, values, "");
    } catch(SQLException e) {
      Log.e(TAG, "Error inserting " + values, e);
      return -1;
    }
  }

  /**
   * Cuando se produce una violación de restricción UNIQUE o PRIMARY KEY, 
   * la REPLACE declaración:
   * 
   * ° Primero, elimina la fila existente que causa la violación de restricción.
   * ° Segundo, inserta una nueva fila.
   * 
   * @param table nombre de la tabla.
   * @param values valore del replece.
   * @return
   */
  public long repleace(String table, Map<String, Object> values) {
    try {
      return insertWithOnConflict(table, values, "OR REPLACE");
    } catch(SQLException e) {
      Log.e(TAG, "Error inserting " + values, e);
      return -1;
    }
  }

  /**
   * Actualiza una registro en la base de datos.
   *
   * @param table nombre de la tabla donde se va a actualizar la fila.
   * @param values contiene los valores de columna iniciales para la fila.
   * Las claves deben ser los nombres de las columnas y los valores valores de
   * la columna.
   * @param conflictAlgorithm
   * @param whereClause [opcional] cláusula WHERE para aplicar al actualizar.
   * Pasar null actualizará todas las filas.
   * @param whereArgs [opcional] Puede incluirse en la cláusula WHERE, que será
   * reemplazado por los valores de whereArgs. Los valores se enlazará como
   * cadenas.
   *
   * @return el número de filas afectadas.
   *
   * @throws SQLException
   */
  public int updateWithOnConflict(String table, Map<String, Object> values,
          String conflictAlgorithm, String whereClause, 
          Object... whereArgs) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("UPDATE ");
    sql.append(conflictAlgorithm);
    sql.append(" ");
    sql.append(table);
    sql.append(" SET ");

    int setValuesSize = values.size();
    int bindArgsSize = (whereArgs == null) ? setValuesSize
            : (setValuesSize + whereArgs.length);
    Object[] bindArgs = new Object[bindArgsSize];
    int i = 0;
    for (String colName : values.keySet()) {
      sql.append((i > 0) ? "," : "");
      sql.append(colName);
      bindArgs[i++] = values.get(colName);
      sql.append("=?");
    }

    if (whereArgs != null) {
      for (i = setValuesSize; i < bindArgsSize; i++) {
        bindArgs[i] = whereArgs[i - setValuesSize];
      }
    }
    if (whereClause != null && !whereClause.isEmpty()) {
      sql.append(" WHERE ");
      sql.append(whereClause);
    }

    return executeUpdate(sql.toString(), bindArgs);
  }
  
  public int update(String table,  Map<String, Object> values, 
          String whereClause, Object... whereArgs) {
    try {
      return updateWithOnConflict(table, values, "", whereClause, whereArgs);
    } catch (SQLException e) {
      Log.e(TAG, "Error updating " + values, e);
      return -1;
    }
  }

  public CreateOrUpdateStatus upsert(String table, Map<String, Object> values,
          String whereClause, Object... whereArgs) {
    try {
      int rows = update(table, values, whereClause, whereArgs);
      if (rows > 0) {
        return new CreateOrUpdateStatus(Boolean.FALSE, Boolean.TRUE, rows, -1);
      }
      long insertId = insertWithOnConflict(table, values, "OR IGNORE");
      if (insertId > -1) {
        return new CreateOrUpdateStatus(Boolean.TRUE, Boolean.FALSE, -1, insertId);
      }
      return new CreateOrUpdateStatus(Boolean.FALSE, Boolean.FALSE, -1, -1);
    } catch(SQLException e) {
      Log.e(TAG, "Error upsert " + values, e);
      return new CreateOrUpdateStatus(Boolean.FALSE, Boolean.FALSE, -1, -1);
    }
  }
  
  /**
   * Elimina un registro de la base de datos.
   *
   * @param table nombre de la tabla donde se eliminara
   * @param whereClause [opcional] cláusula WHERE para aplicar la eliminación.
   * Pasar null elimina todas las filas.
   * @param whereArgs [opcional] Puede incluirse en la cláusula WHERE, que será
   * reemplazado por los valores de whereArgs. Los valores se enlazarán como
   * cadenas.
   *
   * @return el número de filas afectadas.
   *
   * @throws SQLException
   */
  public int delete(String table, String whereClause, Object... whereArgs)
          throws SQLException {
    String sql = "DELETE FROM " + table;
    if (whereClause != null && !whereClause.isEmpty()) {
      sql += " WHERE " + whereClause;
    }
    return executeUpdate(sql, whereArgs);
  }
  
  /**
   * Obtiene la versión de la base de datos.
   *
   * @return la versión de la base de datos
   */
  public int getVersion(SQLiteDatabase db) throws SQLException {
    try (Statement stmt = db.createStatement();
            ResultSet rs = stmt.executeQuery("PRAGMA user_version")) {
      return rs.getInt(1);
    }
  }
  
  /**
   * Establece la versión de la base de datos.
   *
   * @param version la nueva versión de la base de datos
   */
  public void setVersion(SQLiteDatabase db, int version) throws SQLException {
    execSQL("PRAGMA user_version = " + version);
  }
  
  /** Obtiene un valor numerico segun el query que se forme con los parametros. */
  public long getLong(String campo, String tabla, String where, Object... vars)
  throws SQLException {
    StringBuilder sql = new StringBuilder()
            .append("SELECT ")
            .append(campo)
            .append(" FROM ")
            .append(tabla)
            .append(" ");
    if (where != null && !where.isEmpty()) {
      sql.append("\tWHERE ").append(where);
    }
    try (ResultSet rs = query(sql.toString(), vars)) {
      return rs.next() ? rs.getLong(1) : -1;
    }
  }
  
  /** Obtiene un constructor de quierys. */
  public QueryBuilder table(String table) {
    return new QueryBuilder(this).from(table);
  }
  
  public boolean isClosed() throws SQLException {
    return conn.isClosed();
  }

  public boolean isReadOnly() throws SQLException {
    return conn.isReadOnly();
  }

  public void beginTransaction() throws SQLException {
    conn.setAutoCommit(Boolean.FALSE); 
  }

  public void setTransactionSuccessful() throws SQLException {
    conn.commit();
  }

  public void endTransaction() throws SQLException {
    conn.setAutoCommit(Boolean.TRUE);
  }

  public void rollback() throws SQLException {
    conn.rollback();
  }
}
