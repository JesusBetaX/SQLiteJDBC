package javax.database.sqlite;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

public class QueryBuilder {

  private SQLiteDatabase db;
  private boolean distinct = false;
  private List<Object> columns;
  private String table;
  private QueryBuilder from;
  private LinkedHashSet<String> joins;
  private Where where;
  private String groupBy;
  private String having;
  private String orderBy;
  private String limit;

  public QueryBuilder() {
    this(null);
  }
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
    return from(table, null);
  }
  
  /**
   * Genera la parte FROM de la consulta.
   * 
   * @param as nombre de alias
   * @param from subQuery
   * @return 
   */
  public QueryBuilder from(String as, QueryBuilder from) {
    this.table = as;
    this.from = from;
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
  public QueryBuilder leftJoin(String table, String condition) {
    return join(table, condition, "LEFT");
  }
  public QueryBuilder innerJoin(String table, String condition) {
    return join(table, condition, "INNER");
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
  
  /** Clausula where. */
  public Where where(Where where) {
    return this.where = where;
  }
  public Where where() {
    if (this.where == null) 
      this.where(new Where(this));
    return this.where;
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
    if (this.db == null) throw new SQLException("SQLiteDatabase == null");
    return this.db.query(toString());
  }

  /** Compilamos el query. */
  @Override public String toString() {
    // SELECT:
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
    // FROM:
    query.append(" FROM ");
    if (this.from != null) {
      query.append("(").append(this.from).append(") AS ");
    }
    query.append(this.table);
    // JOIN:
    if (this.joins != null && !this.joins.isEmpty()) {
       for (String join : this.joins) {
        appendClause(query, " ", join);
      }
    }
    // EXTRAS:
    appendClause(query, " WHERE ", this.where);
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
  private static void appendClause(StringBuilder s, String name, Object clause) {
    if (clause != null) appendClause(s, name, clause.toString());
  }
  
  public static class Where {
    private final QueryBuilder qs;
    private final StringBuilder sql = new StringBuilder();
    private int countClauses = 0;
    
    public Where(QueryBuilder querySelect) {
      qs = querySelect;
    }
    
    public Where and() {
      if (countClauses > 0) {
        sql.append(" AND");
      }
      return this;
    }
    
    public Where or() {
      if (countClauses > 0) {
        sql.append(" OR");
      }
      return this;
    }
    
    public Where not() {
      sql.append(" NOT");
      return this;
    }

    public Where clause(String columnName, String op, Object value) {
      sql.append(" ")
         .append(columnName.trim())
         .append(" ").append(op.trim()).append(" ")
         .append(toValue(value))
      ;
      countClauses++;
      return this;
    }
    
    public Where like(String columnName, Object value) {
      return clause(columnName, "LIKE", value);
    }
    
    public Where between(String columnName, Object low, Object high) {
      sql.append(" ")
         .append(columnName)
         .append(" BETWEEN ")
         .append(toValue(low))
         .append(" AND ")
         .append(toValue(high))
      ;
      countClauses++;
      return this;
    }
    
    public Where in(String columnName, Object... values) {
      sql.append(" ");
      sql.append(columnName);
      sql.append(" IN (");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) sql.append(", ");
        sql.append(toValue(values[i]));
      }
      sql.append(")");
      countClauses++;
      return this;
    }
    
    public Where in(String columnName, QueryBuilder qs) {
      sql.append(" ")
         .append(columnName)
         .append(" IN (")
         .append(qs.toString())
         .append(")")
      ;
      countClauses++;
      return this;
    }
    
    public Where exists(QueryBuilder qs) {
      // EXISTS (SELECT * FROM `producto` WHERE `id` = 0 )
      sql.append(" EXISTS (")
         .append(qs.toString())
         .append(")")
      ;
      countClauses++;
      return this;
    }
    
    public Where str(String str) {
      sql.append(str);
      return this;
    }
    
    public QueryBuilder endWhere() {
      return qs;
    }
    
    @Override public String toString() {
      return sql.toString();
    }
    
    public static String toValue(Object value) {
      if (value == null) {
        return "NULL ";
      } else {
        String newValue = value.toString().replace("'", "\\'");
        return new StringBuilder(newValue.length() + 2)
                .append('\'')
                .append(newValue)
                .append('\'')
                .toString();
      }
    }
  }
}