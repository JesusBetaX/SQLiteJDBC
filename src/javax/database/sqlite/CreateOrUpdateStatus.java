package javax.database.sqlite;

public class CreateOrUpdateStatus {

  final boolean created;
  final boolean updated;
  final int numberLinesChanged;
  final long insertId;

  public CreateOrUpdateStatus(boolean created, boolean updated, int numberLinesChanged, long insertId) {
    this.created = created;
    this.updated = updated;
    this.numberLinesChanged = numberLinesChanged;
    this.insertId = insertId;
  }
  
  public boolean isCreated() {
    return created;
  }

  public boolean isUpdated() {
    return updated;
  }

  public int getNumberLinesChanged() {
    return numberLinesChanged;
  }

  public long getInsertId() {
    return insertId;
  }
}
