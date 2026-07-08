package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;

/**
 * Overrides scan to add range scan, that includes all documents whose IDs are
 * between two specified IDs.
 */
public class RangeScanWorkload extends CoreWorkload {

  /**
   * The name of the property for deciding whether to do range scan.
  */

  public static final String RANGE_SCAN_PROPERTY = "rangescan";
  /**
   * The default value for the rangescan property.
   */
  public static final String RANGE_SCAN_PROPERTY_DEFAULT = "true";

  /**
   * Limit the number of attempts to find a valid range key to a reasonable number.
   */
  private static final int MAX_RANGE_KEY_ATTEMPTS = 100;

  protected boolean rangescan;

  /**
  * Initialize the scenario.
  * Called once, in the main client thread, before any operations are started.
  */
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    rangescan = Boolean.parseBoolean(
        p.getProperty(RANGE_SCAN_PROPERTY, RANGE_SCAN_PROPERTY_DEFAULT));

  }

  public void doTransactionScan(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String startkeyname = buildKeyName(keynum);

    // choose a random scan length
    int len = scanlength.nextValue().intValue();

    HashSet<String> fields = null;

    if (rangescan) {
      long endKeynum = nextKeynum();
      String endName = buildKeyName(endKeynum);

      int attempts = 0;
      while (endName.equals(startkeyname) && attempts++ < MAX_RANGE_KEY_ATTEMPTS) {
        endKeynum = nextKeynum();
        endName = buildKeyName(endKeynum);
      }

      if (startkeyname.compareTo(endName) > 0) {
        String tmp = startkeyname;
        startkeyname = endName;
        endName = tmp;
      }

      startkeyname = "usertable:" + startkeyname;
      String endkeyname = "usertable:" + endName;

      db.rangescan(table, startkeyname, endkeyname, len, new Vector<HashMap<String, ByteIterator>>());
      return;
    }

    db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
  }


  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SCAN":
      doTransactionScan(db);
      break;
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }

}