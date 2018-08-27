package javax.database.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class SQLiteDatabase implements AutoCloseable {

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
      statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
              ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
      return new SQLiteResultSet(statement.executeQuery(sql), statement);
    } catch (SQLException e) {
      closeQuietly(statement);
      throw e;
    }
  }
  public ResultSet query(String sql, Object... bindArgs) throws SQLException {
    PreparedStatement statement = null;
    try {
      statement = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, 
              ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
      prepareBind(statement, bindArgs);
      return new SQLiteResultSet(statement.executeQuery(), statement);
    } catch (SQLException e) {
      closeQuietly(statement);
      throw e;
    }
  }

  public boolean execSQL(String sql) throws SQLException {
    try (Statement statement = createStatement()) {
      return statement.execute(sql);
    }
  }
  public boolean execSQL(String sql, Object... bindArgs) throws SQLException {
    try (PreparedStatement statement = compileStatement(sql)) {
      prepareBind(statement, bindArgs);
      return statement.execute();
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
      return statement.executeUpdate();
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
      if (ps.execute()) {
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
   *
   * @return el ID de la fila reci�n insertada, o -1 si se produjo un error
   *
   * @throws SQLException
   */
  public long insert(String table, Map<String, Object> initialValues)
          throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ");
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

  /**
   * Actualiza una registro en la base de datos.
   *
   * @param table nombre de la tabla donde se va a actualizar la fila.
   * @param values contiene los valores de columna iniciales para la fila.
   * Las claves deben ser los nombres de las columnas y los valores valores de
   * la columna.
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
  public int update(String table, Map<String, Object> values,
          String whereClause, String[] whereArgs) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("UPDATE ");
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

  /**
   * Elimina un registro de la base de datos.
   *
   * @param table nombre de la tabla donde se eliminara
   * @param whereClause [opcional] cláusula WHERE para aplicar la eliminación.
   * Pasar null elimina todas las filas.
   * @param whereArgs [opcional] Puede incluirse en la cl�usula WHERE, que será
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

  public boolean isClosed() throws SQLException {
    return conn.isClosed();
  }

  public boolean isReadOnly() throws SQLException {
    return conn.isReadOnly();
  }

  public void beginTransaction() throws SQLException {
    conn.setAutoCommit(false); 
  }

  public void setTransactionSuccessful() throws SQLException {
    conn.commit();
  }

  public void endTransaction() throws SQLException {
    conn.setAutoCommit(true);
  }

  public void rollback() throws SQLException {
    conn.rollback();
  }
}
