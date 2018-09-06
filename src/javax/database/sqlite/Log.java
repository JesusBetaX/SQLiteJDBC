package javax.database.sqlite;

public final class Log {

  private static boolean debuggable;
  
  public static boolean isDebuggable() {
    return debuggable;
  }

  public static void setDebuggable(boolean debuggable) {
    Log.debuggable = debuggable;
  }
  
  public static void i(String tag, String msg) {
    if (isDebuggable()) {
      System.out.printf("[%s]: %s\n", tag, msg);
    }
  }
  
  public static void i(String tag, String msg, Throwable tr) {
    if (isDebuggable()) {
      System.out.printf("[%s]: %s => %s\n", tag, 
              tr.getClass().getCanonicalName(), msg);
    }
  }

   public static void e(String tag, String msg) {
    if (isDebuggable()) {
      System.err.printf("[%s]: %s\n", tag, msg);
    }
  }
  
  public static void e(String tag, String msg, Throwable tr) {
    if (isDebuggable()) {
      System.err.printf("[%s]: %s => %s\n", tag, 
              tr.getClass().getCanonicalName(), msg);
    }
  }
  
  private Log() {
  }
  
}
