import sys
import re

def main():

    for line in sys.stdin:
        cols = line.split()

        #filter and log invalid number of columns
        if len(cols) < 5:
            sys.stderr.write("reporter:counter:BadRecords,INVALID_NUMBER_OF_COLS,1")
            continue

        ip = cols[4]
        m = re.match(('^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\'
                      '.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\'
                      '.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\'
                      '.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$'), ip)

        #filter and log invalid ip addresses
        if not m:
            sys.stderr.write("reporter:counter:BadRecords,INVALID_IP,1")
            continue

        print '%s' % "\t".join(cols)


if __name__ == "__main__":
    main()