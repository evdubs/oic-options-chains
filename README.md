# oic-options-chains
These Racket programs will download the [OIC Options Chains](https://www.optionseducation.org/toolsoptionquotes/optionsquotes) HTML files 
and insert the data into a PostgreSQL database. The intended usage is:

```bash
$ racket extract.rkt
$ racket transform-load.rkt
```

You will need to provide a database password for both programs. The available parameters are:

```bash
$ racket extract.rkt -h
racket extract.rkt [ <option> ... ]
 where <option> is one of
  -f <first>, --first-symbol <first> : First symbol to query. Defaults to nothing
  -l <last>, --last-symbol <last> : Last symbol to query. Defaults to nothing
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-`. For
  example: `-h-` is the same as `-h --`

$ racket transform-load.rkt -h
racket transform-load.rkt [ <option> ... ]
 where <option> is one of
  -a, --all-options : Save all options instead of the default select strikes and expirations
  -b <folder>, --base-folder <folder> : OIC options chains base folder. Defaults to /var/local/oic/options-chains
  -d <date>, --folder-date <date> : OIC options chains folder date. Defaults to today
  -n <name>, --db-name <name> : Database name. Defaults to 'local'
  -p <password>, --db-pass <password> : Database password
  -u <user>, --db-user <user> : Database user name. Defaults to 'user'
  --help, -h : Show this help
  -- : Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after one `-`. For
  example: `-h-` is the same as `-h --`
```

The provided `schema.sql` file shows the expected schema within the target PostgreSQL instance. This process assumes you can write to a 
`/var/local/oic/options-chains` folder. This process also assumes you have loaded your database with the NASDAQ symbol file information,
SPDR ETF holding information, and Invesco ADR ETF holding information. This data is provided by the 
[nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project, [spdr-etf-holdings](https://github.com/evdubs/spdr-etf-holdings) project, 
and [invesco-etf-holdings](https://github.com/evdubs/invesco-etf-holdings) project. These programs currently (2019-11-11) will just extract 
option chains for S&P 500/400/600 component companies, Invesco ADR component companies, and some SPDR ETFs. The transform/load script will 
just insert options expiring 2 weeks, 4 weeks, and 8 weeks from the current date and they will also just grab the 
+/- 0%, 2%, 4%, 6%, 8%, 10%, 12.5%, 15%, 17.5%, 20%, 22.5%, 25%, 27.5%, and 30% strikes. This suits my purposes, but this filter can be removed 
if you want to insert everything.

### Dependencies

It is recommended that you start with the standard Racket distribution. With that, you will need to install the following packages:

```bash
$ raco pkg install --skip-installed gregor html-parsing sxml tasks threading
```
