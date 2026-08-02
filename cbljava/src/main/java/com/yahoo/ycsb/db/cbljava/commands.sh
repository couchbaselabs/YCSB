export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-25.jdk/Contents/Home
cd /Users/dragostaraban/perf/YCSB2/YCSB
SCRATCH=/private/tmp/claude-502/-Users-dragostaraban-perf-YCSB2-YCSB/6da7a671-5d6c-4a7f-a860-47699d5f9f73/scratchpad
CP="cbljava/target/cbljava-binding-0.15.0.jar:$(sed 's/^classpath=//' cbljava/cp.txt)"
J=$JAVA_HOME/bin/java

echo "== idrange scan =="
$J -cp "$CP" com.yahoo.ycsb.Client -db com.yahoo.ycsb.db.cbljava.CBLJavaClient \
  -P workloads/cbljava_blank -p recordcount=2000 -p operationcount=500 -p scanproportion=1 \
  -p cbl.dbPath=$SCRATCH/cbl_demo -p cbl.queryMode=idrange -threads 4 -t 2>$SCRATCH/scan1.log \
  | grep -E "\[SCAN\].*(Operations|Return)"
grep -iE "failed|exception" $SCRATCH/scan1.log | head -5

echo "== fieldmatch scan (needs index) =="
rm -rf $SCRATCH/cbl_idx && mkdir -p $SCRATCH/cbl_idx
$J -cp "$CP" com.yahoo.ycsb.Client -db com.yahoo.ycsb.db.cbljava.CBLJavaClient \
  -P workloads/workloada -p recordcount=2000 -p cbl.dbPath=$SCRATCH/cbl_idx \
  -p cbl.createIndexes=true -threads 4 -load 2>$SCRATCH/idxload.log | grep -E "\[INSERT\].*(Operations|Return)"
grep -iE "failed|exception" $SCRATCH/idxload.log | head -5

$J -cp "$CP" com.yahoo.ycsb.Client -db com.yahoo.ycsb.db.cbljava.CBLJavaClient \
  -P workloads/cbljava_blank -p recordcount=2000 -p operationcount=500 -p scanproportion=1 \
  -p cbl.dbPath=$SCRATCH/cbl_idx -p cbl.queryMode=fieldmatch -p cbl.queryField=field0 \
  -threads 4 -t 2>$SCRATCH/scan2.log | grep -E "\[SCAN\].*(Operations|Return)"
grep -iE "failed|exception" $SCRATCH/scan2.log | head -5

echo "== preserve-db reopen (upgrade-style) =="
$J -cp "$CP" com.yahoo.ycsb.Client -db com.yahoo.ycsb.db.cbljava.CBLJavaClient \
  -P workloads/workloadc -p recordcount=2000 -p operationcount=1 -p cbl.dbPath=$SCRATCH/cbl_demo \
  -p cbl.preserveDb=true -threads 1 -t 2>$SCRATCH/upgrade.log | grep -E "\[READ\].*(Operations|Return)"
grep -E "UPGRADE" $SCRATCH/upgrade.log