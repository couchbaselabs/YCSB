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
public class RangeScanCollWorkload extends CustomCollectionWorkload {

  /**
   * The name of the property for deciding whether to do range scan.
  */

  public static final String RANGE_SCAN_PROPERTY = "rangescan";
  /**
   * The default value for the rangescan property.
   */
  public static final String RANGE_SCAN_PROPERTY_DEFAULT = "true";

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

    int collnum = (int) nextcollectionNum();
    String collname = collections[collnum];

    int scopenum = (int) nextscopeNum();
    String scopename = scopes[scopenum];

    // choose a random scan length
    int len = scanlength.nextValue().intValue();

    HashSet<String> fields = null;

    if (rangescan) {
      long endKeynum = nextKeynum();

      while (buildKeyName(endKeynum).compareTo(buildKeyName(keynum)) <= 0) {
        endKeynum = nextKeynum();
      }

      String endkeyname = buildKeyName(endKeynum);

      db.rangescan(table, startkeyname, endkeyname, len, new Vector<HashMap<String, ByteIterator>>(),
                   scopename, collname);
      return;
    }

    db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>(), scopename, collname);
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