package javax.database.sqlite;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

public class QueryBuilder {

  private final SQLiteDatabase db;
  private boolean distinct = false;
  private List<Object> columns;
  private String table;
  private LinkedHashSet<String> joins;
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
    this.columns = new ArrayList<Object>();
    this.columns.addAll(Arrays.asList(fields));
    return this;
  }
  
  /** Define el nombre de la tabla. */
  public QueryBuilder from(String table) {
    this.table = table;
    return this;
  }
  
  /**
   * Genera la parte JOIN de la consulta
   * 
   * @param table table t2
   * @param condition t1.field = t2.field
   * @return 
   */
  public QueryBuilder join(String table, String condition) {
    return join(table, condition, null);
  }

  /**
   * Genera la parte JOIN de la consulta
   * 
   * @param table table t2
   * @param condition t1.field = t2.field
   * @param type left, inner
   * @return 
   */
  public QueryBuilder join(String table, String condition, String type/*LEFT*/) {
    final StringBuilder join = new StringBuilder();
    if (type != null) join.append(type).append(" ");
    join.append("JOIN ")
      .append(table.trim())
      .append(" ON ")
      .append(condition.trim())
    ;
    if (this.joins == null) {
      this.joins = new LinkedHashSet<String>();
    }
    this.joins.add(join.toString());
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
    if (this.distinct) query.append("DISTINCT ");
    if (this.columns != null && !this.columns.isEmpty()) {
      for (int i = 0; i < this.columns.size(); i++) {
        if (i > 0) query.append(',');
        query.append(this.columns.get(i));
      }
    } else {
      query.append("*");
    }
    query.append(" FROM ");
    query.append(this.table);
    if (this.joins != null && !this.joins.isEmpty()) {
       for (String join : this.joins) {
        appendClause(query, " ", join);
      }
    }
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