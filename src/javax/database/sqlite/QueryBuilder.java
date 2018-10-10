package javax.database.sqlite;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryBuilder {

  private final SQLiteDatabase db;
  private String table;
  private boolean distinct = false;
  private List<Object> columns = new ArrayList();
  private String whereClause;
  private Object[] whereArgs;
  private String groupBy;
  private String having;
  private String orderBy;
  private String limit;

  public QueryBuilder(SQLiteDatabase db) {
    this.db = db;
  }
  
  /** Permite forzar la consulta para devolver resultados distintos. */
  public QueryBuilder distinct() {
    this.distinct = true;
    return this;
  }
  
  /** Atributos de seleccion de la consulta. */
  public QueryBuilder select(String... fields) {
    this.columns.clear();
    this.columns.addAll(Arrays.asList(fields));
    return this;
  }
  
  /** Define el nombre de la tabla. */
  public QueryBuilder from(String table) {
    this.table = table;
    return this;
  }
  
  /**
   * Clausula where 
   * 
   * @param whereClause condicion
   * @param whereArgs [opcional] parametros del whereClause
   * @return this
   */
  public QueryBuilder where(String whereClause, Object... whereArgs) {
    this.whereClause = whereClause;
    this.whereArgs = whereArgs;
    return this;
  }
  
  public QueryBuilder groupBy(String groupBy) {
    this.groupBy = groupBy;
    return this;
  }
  
  public QueryBuilder having(String having) {
    this.having = having;
    return this;
  }
  
  public QueryBuilder orderBy(String orderBy) {
    this.orderBy = orderBy;
    return this;
  }
  
  public QueryBuilder limit(String limit) {
    this.limit = limit;
    return this;
  }

  /** Construye y ejecuta el query. */
  public ResultSet get() throws SQLException {
    if (this.whereArgs != null)
      return this.db.query(toString(), this.whereArgs);
    else
      return this.db.query(toString());
  }

  /** Compilamos el query. */
  @Override public String toString() {
    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    if (distinct) query.append("DISTINCT ");
    if (columns.isEmpty()) {
      query.append("*");
    } else {
      for (int i = 0; i < this.columns.size(); i++) {
        if (i > 0) query.append(',');
        query.append(this.columns.get(i));
      }
    }
    query.append(" FROM ");
    query.append(this.table);
    appendClause(query, " WHERE ", this.whereClause);
    appendClause(query, " GROUP BY ", this.groupBy);
    appendClause(query, " HAVING ", this.having);
    appendClause(query, " ORDER BY ", this.orderBy);
    appendClause(query, " LIMIT ", this.limit);
    return query.toString();
  }
  
  private static void appendClause(StringBuilder s, String name, String clause) {
    if (clause != null && !clause.isEmpty()) {
      s.append(name);
      s.append(clause);
    }
  }
}