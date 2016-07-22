Configuration conf = HBaseConfiguration.create();
HTable table = new HTable(conf, "logtable");
Scan scan = new Scan();
scan.setMaxVersions(2);
ResultScanner result = table.getScanner(scan);
for (Result result: scanner) {
    System.out.println("Rows which were scaned : " + result.getRow());
}